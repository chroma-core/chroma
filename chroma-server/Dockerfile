FROM --platform=linux/amd64 python:3.10 AS chroma_server

#RUN apt-get update -qq
#RUN apt-get install python3.10 python3-pip -y --no-install-recommends && rm -rf /var/lib/apt/lists_/*

WORKDIR /chroma-server

COPY ./requirements.txt requirements.txt

RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY ./chroma_server /chroma-server/chroma_server

EXPOSE 8000

CMD ["uvicorn", "chroma_server:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]

# Use a multi-stage build to layer in test dependencies without bloating server image
# https://docs.docker.com/build/building/multi-stage/
# Note: requires passing --target to docker-build.
FROM chroma_server AS chroma_server_test

COPY ./requirements_dev.txt requirements_dev.txt
RUN pip install --no-cache-dir --upgrade -r requirements_dev.txt

CMD ["python", "-m", "pytest"]

