FROM python:3.11-slim-bookworm AS builder
ARG REBUILD_HNSWLIB
RUN apt-get update --fix-missing && apt-get install -y --fix-missing \
    build-essential \
    gcc \
    g++ \
    cmake \
    autoconf && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir /install

WORKDIR /install

COPY ./requirements.txt requirements.txt

RUN --mount=type=cache,target=/root/.cache/pip pip install --upgrade --prefix="/install" -r requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip if [ "$REBUILD_HNSWLIB" = "true" ]; then pip install --no-binary :all: --force-reinstall --prefix="/install" chroma-hnswlib; fi

FROM python:3.11-slim-bookworm AS final

RUN mkdir /chroma
WORKDIR /chroma

COPY ./bin/docker_entrypoint.sh /docker_entrypoint.sh

RUN apt-get update --fix-missing && apt-get install -y curl && \
    chmod +x /docker_entrypoint.sh && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local
COPY ./ /chroma

ENV CHROMA_HOST_ADDR="0.0.0.0"
ENV CHROMA_HOST_PORT=8000
ENV CHROMA_WORKERS=1
ENV CHROMA_LOG_CONFIG="chromadb/log_config.yml"
ENV CHROMA_TIMEOUT_KEEP_ALIVE=30

EXPOSE 8000

ENTRYPOINT ["/docker_entrypoint.sh"]
CMD [ "--workers ${CHROMA_WORKERS} --host ${CHROMA_HOST_ADDR} --port ${CHROMA_HOST_PORT} --proxy-headers --log-config ${CHROMA_LOG_CONFIG} --timeout-keep-alive ${CHROMA_TIMEOUT_KEEP_ALIVE}"]
