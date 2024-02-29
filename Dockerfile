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

FROM python:3.10-slim-bookworm as final
# ARG STORAGEACCT
# ARG STORAGEKEY

RUN apt-get update --fix-missing && apt-get install -y --fix-missing \
    build-essential \
    gcc \
    g++ && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir /chroma 
# RUN chmod 777 -R /chroma
# RUN mount -t cifs //$STORAGEACCT.file.core.windows.net/testingdb /chroma -o vers=3.0,username=$STORAGEACCT,password=$STORAGEKEY,dir_mode=0777,file_mode=0777,serverino
WORKDIR /chroma

COPY --from=builder /install /usr/local
COPY ./bin/docker_entrypoint.sh /docker_entrypoint.sh
COPY ./ /chroma

EXPOSE 8000

CMD ["/docker_entrypoint.sh"]
