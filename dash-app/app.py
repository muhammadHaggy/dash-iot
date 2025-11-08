import os
import pandas as pd

from dash import Dash, html, dcc, Input, Output, dash_table
import plotly.express as px
import plotly.graph_objects as go
from influxdb_client import InfluxDBClient

INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG", "acme")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "telemetry")
APP_REFRESH_MS = int(os.getenv("APP_REFRESH_MS", "5000"))
DASH_TITLE = os.getenv("DASH_TITLE", "Truck Air Quality")

# Color scheme
COLORS = {
    'primary': '#1f77b4',
    'secondary': '#ff7f0e',
    'success': '#2ca02c',
    'danger': '#d62728',
    'warning': '#ff9800',
    'info': '#17a2b8',
    'background': '#f8f9fa',
    'card': '#ffffff',
    'text': '#212529',
    'border': '#dee2e6',
    'co2': '#ff6b6b',
    'hc': '#4ecdc4',
    'co': '#ffe66d'
}

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
    # Build lat and lon separately, then join (robust when streams differ)
    coords_flux = f'''
lat = from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -24h)
  |> filter(fn: (r) => r["_measurement"] == "truck_metrics" and r["_field"] == "lat")
  |> group(columns: ["truck_id"])
  |> last()
  |> keep(columns: ["truck_id","_time","_value"])
  |> rename(columns: {{_value: "lat", _time: "time_lat"}})

lon = from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -24h)
  |> filter(fn: (r) => r["_measurement"] == "truck_metrics" and r["_field"] == "lon")
  |> group(columns: ["truck_id"])
  |> last()
  |> keep(columns: ["truck_id","_time","_value"])
  |> rename(columns: {{_value: "lon", _time: "time_lon"}})

join(tables: {{lat: lat, lon: lon}}, on: ["truck_id"])
  |> keep(columns: ["truck_id","lat","lon","time_lat","time_lon"])
    '''

    metrics_flux = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -24h)
  |> filter(fn: (r) => r["_measurement"] == "truck_metrics")
  |> filter(fn: (r) => r["_field"] =~ /(co2|hc|co)/)
  |> group(columns: ["truck_id", "_field"])
  |> last()
  |> keep(columns: ["_time", "truck_id", "_field", "_value"])
    '''

    coords_tables = qapi.query_data_frame(coords_flux)
    metrics_tables = qapi.query_data_frame(metrics_flux)

    if (isinstance(coords_tables, list) and len(coords_tables) == 0) or coords_tables is None:
        coords_df = pd.DataFrame()
    else:
        coords_df = coords_tables if isinstance(coords_tables, pd.DataFrame) else pd.concat(coords_tables)

    if (isinstance(metrics_tables, list) and len(metrics_tables) == 0) or metrics_tables is None:
        metrics_df = pd.DataFrame()
    else:
        metrics_df = metrics_tables if isinstance(metrics_tables, pd.DataFrame) else pd.concat(metrics_tables)

    if coords_df.empty and metrics_df.empty:
        return pd.DataFrame()

    if not coords_df.empty:
        # Ensure validity and last per truck_id
        if "truck_id" in coords_df.columns:
            coords_df = coords_df[coords_df["truck_id"].notna()]
            coords_df = coords_df.drop_duplicates(subset=["truck_id"], keep="last")

    if not metrics_df.empty:
        rename_map = {}
        if "_time" in metrics_df.columns:
            rename_map["_time"] = "time"
        if "_field" in metrics_df.columns:
            rename_map["_field"] = "metric"
        if "_value" in metrics_df.columns:
            rename_map["_value"] = "metric_value"
        if rename_map:
            metrics_df = metrics_df.rename(columns=rename_map)
        metrics_df = metrics_df[metrics_df["truck_id"].notna()]
        metrics_df = metrics_df.drop_duplicates(subset=["truck_id", "metric"], keep="last")
        metrics_df = metrics_df.pivot_table(
            index="truck_id",
            columns="metric",
            values="metric_value",
            aggfunc="last"
        ).reset_index()

    if coords_df.empty:
        return metrics_df if not metrics_df.empty else pd.DataFrame()

    if metrics_df.empty:
        return coords_df

    return coords_df.merge(metrics_df, on="truck_id", how="left")


def list_trucks():
    flux = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -7d)
  |> filter(fn: (r) => r._measurement == "truck_metrics")
  |> keep(columns: ["truck_id"])
  |> group()
  |> distinct(column: "truck_id")
  |> rename(columns: {{_value: "truck_id"}})
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

# Styles
card_style = {
    'backgroundColor': COLORS['card'],
    'padding': '20px',
    'borderRadius': '12px',
    'boxShadow': '0 2px 8px rgba(0,0,0,0.1)',
    'marginBottom': '20px'
}

stat_card_style = {
    'backgroundColor': COLORS['card'],
    'padding': '24px',
    'borderRadius': '12px',
    'boxShadow': '0 2px 8px rgba(0,0,0,0.1)',
    'textAlign': 'center',
    'flex': '1',
    'minWidth': '200px'
}

header_style = {
    'backgroundColor': COLORS['primary'],
    'color': 'white',
    'padding': '24px',
    'borderRadius': '12px',
    'marginBottom': '24px',
    'boxShadow': '0 4px 12px rgba(0,0,0,0.15)'
}

app.layout = html.Div([
    # Header
    html.Div([
        html.H1(DASH_TITLE, style={'margin': '0', 'fontSize': '32px', 'fontWeight': '600'}),
        html.P('Real-time emission monitoring dashboard', 
               style={'margin': '8px 0 0 0', 'opacity': '0.9', 'fontSize': '16px'})
    ], style=header_style),

    # Controls Card
    html.Div([
        html.Div([
            html.Div([
                html.Label("🚛 Truck Filter", style={'fontWeight': '600', 'marginBottom': '8px', 'display': 'block'}),
                dcc.Dropdown(
                    id="truck-filter",
                    options=[],
                    multi=True,
                    placeholder="Select trucks (All by default)",
                    style={'minWidth': '300px'}
                ),
            ], style={'flex': '1', 'minWidth': '300px'}),
            
            html.Div([
                html.Label("⏱️ Time Window", style={'fontWeight': '600', 'marginBottom': '8px', 'display': 'block'}),
                dcc.Dropdown(
                    id="time-window",
                    options=[
                        {"label": "Last 1 hour", "value": "-1h"},
                        {"label": "Last 6 hours", "value": "-6h"},
                        {"label": "Last 24 hours", "value": "-24h"},
                        {"label": "Last 7 days", "value": "-7d"},
                    ],
                    value="-24h",
                    clearable=False,
                    style={'minWidth': '200px'}
                ),
            ], style={'flex': '0 0 200px'}),
        ], style={'display': 'flex', 'gap': '20px', 'flexWrap': 'wrap', 'alignItems': 'flex-end'}),
    ], style=card_style),

    dcc.Interval(id="tick", interval=APP_REFRESH_MS, n_intervals=0),

    # Statistics Cards
    html.Div([
        html.Div([
            html.Div("📊", style={'fontSize': '32px', 'marginBottom': '8px'}),
            html.H3(id="stat-trucks", children="0", style={'margin': '0', 'fontSize': '28px', 'fontWeight': '700'}),
            html.P("Active Trucks", style={'margin': '8px 0 0 0', 'color': '#666', 'fontSize': '14px'})
        ], style=stat_card_style),
        
        html.Div([
            html.Div("💨", style={'fontSize': '32px', 'marginBottom': '8px'}),
            html.H3(id="stat-co2", children="--", style={'margin': '0', 'fontSize': '28px', 'fontWeight': '700'}),
            html.P("Avg CO₂ (ppm)", style={'margin': '8px 0 0 0', 'color': '#666', 'fontSize': '14px'})
        ], style=stat_card_style),
        
        html.Div([
            html.Div("🌫️", style={'fontSize': '32px', 'marginBottom': '8px'}),
            html.H3(id="stat-hc", children="--", style={'margin': '0', 'fontSize': '28px', 'fontWeight': '700'}),
            html.P("Avg HC (ppm)", style={'margin': '8px 0 0 0', 'color': '#666', 'fontSize': '14px'})
        ], style=stat_card_style),
        
        html.Div([
            html.Div("☁️", style={'fontSize': '32px', 'marginBottom': '8px'}),
            html.H3(id="stat-co", children="--", style={'margin': '0', 'fontSize': '28px', 'fontWeight': '700'}),
            html.P("Avg CO (ppm)", style={'margin': '8px 0 0 0', 'color': '#666', 'fontSize': '14px'})
        ], style=stat_card_style),
    ], style={'display': 'flex', 'gap': '20px', 'flexWrap': 'wrap', 'marginBottom': '20px'}),

    # Time Series Graph
    html.Div([
        html.H3("📈 Emission Trends", style={'margin': '0 0 16px 0', 'fontSize': '20px', 'fontWeight': '600'}),
        dcc.Graph(id="ts-graph", config={'displayModeBar': True, 'displaylogo': False}),
    ], style=card_style),

    # Map and Table Layout
    html.Div([
        # Map
        html.Div([
            html.H3("🗺️ Truck Locations", style={'margin': '0 0 16px 0', 'fontSize': '20px', 'fontWeight': '600'}),
            dcc.Graph(id="map-graph", style={"height": "500px"}, config={'displayModeBar': True, 'displaylogo': False}),
        ], style={**card_style, 'flex': '1', 'minWidth': '400px'}),
        
        # Latest Readings Table
        html.Div([
            html.H3("📋 Latest Readings", style={'margin': '0 0 16px 0', 'fontSize': '20px', 'fontWeight': '600'}),
            dash_table.DataTable(
                id="latest-table",
                columns=[
                    {"name": "Time", "id": "time"},
                    {"name": "Truck ID", "id": "truck_id"},
                    {"name": "Metric", "id": "metric"},
                    {"name": "Value", "id": "value", "type": "numeric", "format": {"specifier": ".2f"}},
                ],
                page_size=10,
                style_table={'overflowX': 'auto'},
                style_header={
                    'backgroundColor': COLORS['primary'],
                    'color': 'white',
                    'fontWeight': '600',
                    'textAlign': 'left',
                    'padding': '12px'
                },
                style_cell={
                    'textAlign': 'left',
                    'padding': '12px',
                    'fontSize': '14px',
                    'fontFamily': 'system-ui, -apple-system, sans-serif'
                },
                style_data_conditional=[
                    {
                        'if': {'row_index': 'odd'},
                        'backgroundColor': COLORS['background']
                    },
                    {
                        'if': {'column_id': 'metric', 'filter_query': '{metric} eq "co2"'},
                        'backgroundColor': '#ffe5e5',
                        'fontWeight': '600'
                    },
                    {
                        'if': {'column_id': 'metric', 'filter_query': '{metric} eq "hc"'},
                        'backgroundColor': '#e5f9f7',
                        'fontWeight': '600'
                    },
                    {
                        'if': {'column_id': 'metric', 'filter_query': '{metric} eq "co"'},
                        'backgroundColor': '#fff9e5',
                        'fontWeight': '600'
                    }
                ]
            )
        ], style={**card_style, 'flex': '1', 'minWidth': '400px'}),
    ], style={'display': 'flex', 'gap': '20px', 'flexWrap': 'wrap'}),

], style={
    'padding': '24px',
    'backgroundColor': COLORS['background'],
    'minHeight': '100vh',
    'fontFamily': 'system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif'
})


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
    Output("stat-trucks", "children"),
    Output("stat-co2", "children"),
    Output("stat-hc", "children"),
    Output("stat-co", "children"),
    Input("tick", "n_intervals"),
    Input("time-window", "value"),
    Input("truck-filter", "value"),
)
def refresh_dashboard(_, win, trucks):
    ts = query_timeseries(lookback=win, trucks=trucks)
    
    # Default statistics
    stat_trucks = "0"
    stat_co2 = "--"
    stat_hc = "--"
    stat_co = "--"
    
    if ts.empty:
        fig_ts = go.Figure()
        fig_ts.add_annotation(
            text="No data available for the selected period",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=16, color="#999")
        )
        fig_ts.update_layout(
            template="plotly_white",
            height=400,
            margin=dict(l=40, r=40, t=40, b=40)
        )
        latest_rows = []
    else:
        # Calculate statistics
        unique_trucks = ts['truck_id'].nunique()
        stat_trucks = str(unique_trucks)
        
        latest_values = ts.sort_values("time").groupby(["truck_id", "metric"]).tail(1)
        for metric in ['co2', 'hc', 'co']:
            metric_data = latest_values[latest_values['metric'] == metric]
            if not metric_data.empty:
                avg_val = metric_data['value'].mean()
                if metric == 'co2':
                    stat_co2 = f"{avg_val:.1f}"
                elif metric == 'hc':
                    stat_hc = f"{avg_val:.1f}"
                elif metric == 'co':
                    stat_co = f"{avg_val:.1f}"
        
        # Create enhanced time series plot
        fig_ts = go.Figure()
        
        # Define colors for metrics
        metric_colors = {'co2': COLORS['co2'], 'hc': COLORS['hc'], 'co': COLORS['co']}
        
        for metric in ts['metric'].unique():
            for truck in ts['truck_id'].unique():
                df_subset = ts[(ts['metric'] == metric) & (ts['truck_id'] == truck)]
                if not df_subset.empty:
                    fig_ts.add_trace(go.Scatter(
                        x=df_subset['time'],
                        y=df_subset['value'],
                        mode='lines+markers',
                        name=f"{truck} - {metric.upper()}",
                        line=dict(color=metric_colors.get(metric, COLORS['primary']), width=2),
                        marker=dict(size=4),
                        hovertemplate='<b>%{fullData.name}</b><br>' +
                                    'Time: %{x}<br>' +
                                    'Value: %{y:.2f} ppm<br>' +
                                    '<extra></extra>'
                    ))
        
        fig_ts.update_layout(
            template="plotly_white",
            height=400,
            hovermode='x unified',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            xaxis=dict(title="Time", showgrid=True, gridcolor='#f0f0f0'),
            yaxis=dict(title="Concentration (ppm)", showgrid=True, gridcolor='#f0f0f0'),
            margin=dict(l=60, r=40, t=80, b=60),
            plot_bgcolor='white'
        )
        
        # Latest readings for table
        latest_rows = (ts.sort_values("time")
                         .groupby(["truck_id", "metric"])
                         .tail(1)[["time", "truck_id", "metric", "value"]]
                         .sort_values(["truck_id", "metric"], ascending=[True, True]))
        latest_rows["time"] = latest_rows["time"].astype(str)
        latest_rows = latest_rows.to_dict("records")

    # Map visualization
    coords = query_latest_coords()
    if coords.empty:
        fig_map = go.Figure()
        fig_map.add_annotation(
            text="No location data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=16, color="#999")
        )
        fig_map.update_layout(
            template="plotly_white",
            margin=dict(l=0, r=0, t=0, b=0)
        )
    else:
        if trucks:
            coords = coords[coords["truck_id"].isin(trucks)]

        if coords.empty or "lat" not in coords.columns or "lon" not in coords.columns:
            fig_map = go.Figure()
            fig_map.add_annotation(
                text="No location data available",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=16, color="#999")
            )
            fig_map.update_layout(
                template="plotly_white",
                margin=dict(l=0, r=0, t=0, b=0)
            )
            return fig_ts, fig_map, latest_rows, stat_trucks, stat_co2, stat_hc, stat_co

        pivot = coords.copy()
        metric_columns = [col for col in ["co", "co2", "hc"] if col in pivot.columns]
        color_col = metric_columns[0] if metric_columns else None

        hover_data = {col: ':.2f' for col in metric_columns}
        extra_hover_cols = [col for col in pivot.columns if col not in ["truck_id", "lat", "lon"] + metric_columns]
        for col in extra_hover_cols:
            hover_data[col] = True

        fig_map = px.scatter_mapbox(
            pivot,
            lat="lat",
            lon="lon",
            color=color_col if color_col else None,
            hover_name="truck_id",
            hover_data=hover_data,
            zoom=3,
            size_max=15,
            color_continuous_scale="RdYlGn_r" if color_col else None
        )
        
        fig_map.update_layout(
            mapbox_style="open-street-map",
            margin=dict(l=0, r=0, t=0, b=0),
            coloraxis_colorbar=dict(
                title=f"{color_col.upper()} (ppm)" if color_col else "",
                thicknessmode="pixels",
                thickness=15,
                lenmode="pixels",
                len=200
            )
        )
        
        fig_map.update_traces(marker=dict(size=12))

    return fig_ts, fig_map, latest_rows, stat_trucks, stat_co2, stat_hc, stat_co


if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8050, debug=False)

