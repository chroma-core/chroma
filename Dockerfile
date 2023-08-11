FROM python:3.10-slim-bookworm as builder

RUN apt-get update --fix-missing && apt-get install -y --fix-missing \
    build-essential \
    gcc \
    g++ && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir /install
WORKDIR /install

COPY ./requirements.txt requirements.txt

RUN pip install --no-cache-dir --upgrade --prefix="/install" -r requirements.txt
RUN pip install chroma-hnswlib

FROM python:3.10-slim-bookworm as final

RUN apt-get update --fix-missing && apt-get install -y --fix-missing \
    build-essential \
    gcc \
    g++ && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir /chroma
WORKDIR /chroma

COPY --from=builder /install /usr/local
COPY ./ /chroma

EXPOSE 8000

ENV IS_PERSISTENT=1
CMD ["uvicorn", "chromadb.app:app", "--workers", "1", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers", "--log-config", "log_config.yml"]

