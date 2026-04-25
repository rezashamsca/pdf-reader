# Build the image using the following command: docker build -t [username]/duckdb-alpine:[version] .
# To push it to Docker Hub use: docker push [username]/duckdb-alpine:[version]
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl unzip ca-certificates python3 python3-pip && \
    rm -rf /var/lib/apt/lists*

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir --break-system-packages -r /tmp/requirements.txt

RUN curl -L -o duckdb.zip "https://github.com/duckdb/duckdb/releases/download/v1.5.2/duckdb_cli-linux-arm64.zip" && \
    unzip duckdb.zip -d /usr/local/bin/ && \
    chmod +x /usr/local/bin/duckdb && \
    rm duckdb.zip

RUN mkdir -p /app

COPY ./app/pipeline.py /app/pipeline.py

WORKDIR /app

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
