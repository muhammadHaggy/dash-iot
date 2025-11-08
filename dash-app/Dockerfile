FROM python:3.11-slim

# System deps for tz / certificates
RUN apt-get update && apt-get install -y --no-install-recommends tzdata ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
ENV PORT=8050
EXPOSE 8050

# Gunicorn, single worker is enough to start; scale later if needed
CMD ["gunicorn", "-w", "2", "-b", "0.0.0.0:8050", "app:server"]

