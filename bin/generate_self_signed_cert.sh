#!/usr/bin/env sh

# check if the openssl.cnf file exists
if [ ! -f chromadb/test/openssl.cnf ]; then
    echo "openssl.cnf file not found. Please run the script from the root of the project."
    exit
fi

openssl req -new -newkey rsa:2048 -sha256 -days 365 -nodes -x509 \
  -keyout ./serverkey.pem \
  -out ./servercert.pem \
  -subj "/O=Chroma/C=US" \
  -config chromadb/test/openssl.cnf
