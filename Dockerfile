FROM python:3.9-slim
WORKDIR /app
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
RUN mkdir -p /app/downloaded_files && \
    mkdir -p /var/log/app && \
    chmod 777 /var/log/app && \
    chmod 777 /app/downloaded_files
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV DOWNLOAD_DIR=/app/downloaded_files
EXPOSE 80
CMD ["python", "main.py"]
