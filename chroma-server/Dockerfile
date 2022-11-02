FROM --platform=linux/amd64 python:3.10

#RUN apt-get update -qq
#RUN apt-get install python3.10 python3-pip -y --no-install-recommends && rm -rf /var/lib/apt/lists_/*

WORKDIR /chroma-server

RUN apt-get update \
  && apt-get -y install netcat gcc postgresql \
  && apt-get clean

COPY ./requirements.txt requirements.txt

RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY ./chroma_server /chroma-server/chroma_server

EXPOSE 8000

CMD ["uvicorn", "chroma_server:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]

