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

RUN pip install --no-cache-dir --upgrade --prefix="/install" -r requirements.txt
RUN if [ "$REBUILD_HNSWLIB" = "true" ]; then pip install --no-binary :all: --force-reinstall --no-cache-dir --prefix="/install" chroma-hnswlib; fi

FROM python:3.11-slim-bookworm AS final

RUN mkdir /chroma
WORKDIR /chroma

COPY --from=builder /install /usr/local
COPY ./bin/docker_entrypoint.sh /docker_entrypoint.sh
COPY ./ /chroma

RUN chmod +x /docker_entrypoint.sh

EXPOSE 8000

ENTRYPOINT ["/docker_entrypoint.sh"]
CMD [ "--workers 1 --host 0.0.0.0 --port 8000 --proxy-headers --log-config chromadb/log_config.yml --timeout-keep-alive 30"]