import os
import pandas as pd

from dash import Dash, html, dcc, Input, Output, dash_table
import plotly.express as px
from influxdb_client import InfluxDBClient

INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG", "acme")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "telemetry")
APP_REFRESH_MS = int(os.getenv("APP_REFRESH_MS", "5000"))
DASH_TITLE = os.getenv("DASH_TITLE", "Truck Air Quality")

# Default view window
DEFAULT_LOOKBACK = "-24h"

# Create client once
client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
qapi = client.query_api()


def query_timeseries(lookback=DEFAULT_LOOKBACK, trucks=None):
    truck_filter = ""
    if trucks:
        truck_list = ",".join([f'"{t}"' for t in trucks])
        truck_filter = f'|> filter(fn: (r) => contains(value: r.truck_id, set: [{truck_list}]))'

    flux = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: {lookback})
  |> filter(fn: (r) => r._measurement == "truck_metrics")
  |> filter(fn: (r) => r._field =~ /(co2|hc|co)/)
  {truck_filter}
  |> keep(columns: ["_time","_value","truck_id","_field"])
  |> sort(columns: ["_time"])
    '''
    tables = qapi.query_data_frame(flux)
    if isinstance(tables, list) and len(tables) == 0:
        return pd.DataFrame()
    df = tables if isinstance(tables, pd.DataFrame) else pd.concat(tables)
    if df.empty:
        return df
    if "_measurement" in df.columns:
        df = df[df["_measurement"].notna()]
    df.rename(columns={"_time": "time", "_value": "value", "_field": "metric"}, inplace=True)
    return df


def query_latest_coords():
    flux = f'''
coords = from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "truck_metrics")
  |> filter(fn: (r) => r._field == "lat" or r._field == "lon")
  |> group(columns: ["truck_id"])
  |> last()
  |> pivot(rowKey:["_time","truck_id"], columnKey:["_field"], valueColumn:"_value")

pm = from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "truck_metrics")
  |> filter(fn: (r) => r._field =~ /(co2|hc|co)/)
  |> group(columns: ["truck_id","_field"])
  |> last()

join(tables: {{coords: coords, pm: pm}}, on: ["truck_id"])
  |> keep(columns: ["_time","truck_id","lat","lon","_field","_value"])
  |> rename(columns: {{_time: "time", _value: "metric_value", _field: "metric"}})
    '''
    tables = qapi.query_data_frame(flux)
    if isinstance(tables, list) and len(tables) == 0:
        return pd.DataFrame()
    df = tables if isinstance(tables, pd.DataFrame) else pd.concat(tables)
    if df.empty:
        return df
    if "truck_id" in df.columns:
        df = df[df["truck_id"].notna()]
    return df


def list_trucks():
    flux = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -7d)
  |> filter(fn: (r) => r._measurement == "truck_metrics")
  |> keep(columns: ["truck_id"])
  |> group()
  |> distinct(column: "truck_id")
    '''
    tables = qapi.query_data_frame(flux)
    if isinstance(tables, list) and len(tables) == 0:
        return []
    df = tables if isinstance(tables, pd.DataFrame) else pd.concat(tables)
    if df.empty or "truck_id" not in df.columns:
        return []
    vals = sorted([t for t in df["truck_id"].dropna().unique().tolist() if t])
    return vals


app = Dash(__name__, title=DASH_TITLE)
server = app.server

app.layout = html.Div([
    html.H2(DASH_TITLE, style={"margin": "10px 0"}),
    html.Div([
        html.Label("Trucks"),
        dcc.Dropdown(id="truck-filter", options=[], multi=True, placeholder="All trucks"),
        html.Label("Time window"),
        dcc.Dropdown(
            id="time-window",
            options=[
                {"label": "Last 1h", "value": "-1h"},
                {"label": "Last 6h", "value": "-6h"},
                {"label": "Last 24h", "value": "-24h"},
                {"label": "Last 7d", "value": "-7d"},
            ],
            value="-24h",
            clearable=False,
            style={"maxWidth": "220px"}
        ),
    ], style={"display": "flex", "gap": "16px", "alignItems": "center", "flexWrap": "wrap"}),

    dcc.Interval(id="tick", interval=APP_REFRESH_MS, n_intervals=0),

    html.Div([
        dcc.Graph(id="ts-graph"),
    ], style={"marginTop": "12px"}),

    html.Div([
        dcc.Graph(id="map-graph", style={"height": "520px"}),
    ], style={"marginTop": "12px"}),

    html.Div([
        html.H4("Latest readings"),
        dash_table.DataTable(
            id="latest-table",
            columns=[
                {"name": "time", "id": "time"},
                {"name": "truck_id", "id": "truck_id"},
                {"name": "metric", "id": "metric"},
                {"name": "value", "id": "value"},
            ],
            page_size=10,
            style_table={"overflowX": "auto"},
        )
    ], style={"marginTop": "12px"})
], style={"padding": "16px"})


@app.callback(
    Output("truck-filter", "options"),
    Input("tick", "n_intervals")
)
def refresh_truck_options(_):
    return [{"label": t, "value": t} for t in list_trucks()]


@app.callback(
    Output("ts-graph", "figure"),
    Output("map-graph", "figure"),
    Output("latest-table", "data"),
    Input("tick", "n_intervals"),
    Input("time-window", "value"),
    Input("truck-filter", "value"),
)
def refresh_dashboard(_, win, trucks):
    ts = query_timeseries(lookback=win, trucks=trucks)
    if ts.empty:
        fig_ts = px.line(title="No data yet")
        latest_rows = []
    else:
        fig_ts = px.line(
            ts, x="time", y="value", color="metric", line_group="truck_id",
            hover_data={"truck_id": True, "metric": True, "value": ":.2f"},
            title="Pollutants over time (co2 / hc / co)"
        )
        latest_rows = (ts.sort_values("time")
                         .groupby(["truck_id", "metric"])
                         .tail(1)[["time", "truck_id", "metric", "value"]]
                         .sort_values(["truck_id", "metric"], ascending=[True, True]))
        latest_rows["time"] = latest_rows["time"].astype(str)
        latest_rows = latest_rows.to_dict("records")

    coords = query_latest_coords()
    if coords.empty:
        fig_map = px.scatter_mapbox(title="No coordinates yet")
        fig_map.update_layout(mapbox_style="open-street-map", margin={"l": 0, "r": 0, "t": 50, "b": 0})
    else:
        if trucks:
            coords = coords[coords["truck_id"].isin(trucks)]
        pivot = (coords.pivot_table(index=["truck_id", "lat", "lon"], columns="metric",
                                    values="metric_value", aggfunc="last")
                       .reset_index())
        color_col = None
        for candidate in ["co", "co2", "hc"]:
            if candidate in pivot.columns:
                color_col = candidate
                break
        fig_map = px.scatter_mapbox(
            pivot,
            lat="lat", lon="lon",
            color=color_col if color_col else None,
            hover_data=pivot.columns,
            zoom=3,
            title="Latest truck positions (color = latest CO/CO₂/HC)"
        )
        fig_map.update_layout(mapbox_style="open-street-map", margin={"l": 0, "r": 0, "t": 50, "b": 0})

    return fig_ts, fig_map, latest_rows


if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8050, debug=False)

