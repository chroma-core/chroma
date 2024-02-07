#!/usr/bin/env sh

./bin/generate_self_signed_cert.sh

mkdir certs
mv *.pem certs/


export CHROMA_PORT=8443
cat <<EOF > docker-compose.override.yml
version: '3.9'
services:
  server:
    volumes:
      - ${PWD}/certs:/chroma/certs
    command: uvicorn chromadb.app:app --workers 1 --host 0.0.0.0 --port 8443 --proxy-headers --log-config chromadb/log_config.yml --ssl-keyfile /chroma/certs/serverkey.pem --ssl-certfile /chroma/certs/servercert.pem
    environment:
      - ANONYMIZED_TELEMETRY=False
      - ALLOW_RESET=True
      - IS_PERSISTENT=TRUE
    ports:
      - "${CHROMA_PORT}:8443"
EOF



docker compose up --build -d