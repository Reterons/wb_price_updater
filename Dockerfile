FROM python:3.9-slim
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libssl-dev \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*
RUN update-ca-certificates
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN mkdir -p /app/downloaded_files && \
    chmod 777 /app/downloaded_files
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
EXPOSE 80
CMD ["python", "main.py"]
