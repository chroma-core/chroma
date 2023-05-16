FROM python:3.10-slim-bullseye as builder

#RUN apt-get update -qq
#RUN apt-get install python3.10 python3-pip -y --no-install-recommends && rm -rf /var/lib/apt/lists_/*
RUN apt-get update && apt-get install build-essential -y

RUN mkdir /install
WORKDIR /install

COPY ./requirements.txt requirements.txt

RUN pip install --no-cache-dir --upgrade --prefix="/install" -r requirements.txt

FROM python:3.10-slim-bullseye as final

RUN mkdir /chroma
WORKDIR /chroma

COPY --from=builder /install /usr/local
COPY ./bin/docker_entrypoint.sh /docker_entrypoint.sh
COPY ./ /chroma

EXPOSE 8000

CMD ["/docker_entrypoint.sh"]
