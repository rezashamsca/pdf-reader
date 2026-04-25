# Build the image using the following command: docker build -t [username]/duckdb-alpine:[version] .
# To push it to Docker Hub use: docker push [username]/duckdb-alpine:[version]
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl unzip ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN curl -L -o duckdb.zip "https://github.com/duckdb/duckdb/releases/download/v1.5.2/duckdb_cli-linux-arm64.zip" && \
    unzip duckdb.zip -d /usr/local/bin/ && \
    chmod +x /usr/local/bin/duckdb && \
    rm duckdb.zip

ENTRYPOINT ["duckdb"]
