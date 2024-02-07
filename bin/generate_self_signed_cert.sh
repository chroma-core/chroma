#!/usr/bin/env sh

if ! command -v openssl &> /dev/null
then
    echo "openssl command not found. Please install it and try again."
    exit
fi

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
