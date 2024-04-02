#! /bin/bash

# Note: This is run as root

cd ~
export enable_auth="${enable_auth}"
export basic_auth_credentials="${basic_auth_credentials}"
export auth_type="${auth_type}"
export token_auth_credentials="${token_auth_credentials}"
apt-get update -y
apt-get install -y ca-certificates curl gnupg lsb-release
mkdir -m 0755 -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
apt-get update -y
chmod a+r /etc/apt/keyrings/docker.gpg
apt-get update -y
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin git
usermod -aG docker ubuntu
git clone https://github.com/chroma-core/chroma.git && cd chroma
git fetch --tags
git checkout tags/${chroma_release}

if [ "$${enable_auth}" = "true" ] && [ "$${auth_type}" = "basic" ] && [ ! -z "$${basic_auth_credentials}" ]; then
  username=$(echo $basic_auth_credentials | cut -d: -f1)
  password=$(echo $basic_auth_credentials | cut -d: -f2)
  docker run --rm --entrypoint htpasswd httpd:2 -Bbn $username $password > server.htpasswd
  cat <<EOF > .env
CHROMA_SERVER_AUTH_CREDENTIALS_FILE="/chroma/server.htpasswd"
CHROMA_SERVER_AUTH_CREDENTIALS_PROVIDER="chromadb.auth.providers.HtpasswdFileServerAuthCredentialsProvider"
CHROMA_SERVER_AUTH_PROVIDER="chromadb.auth.basic.BasicAuthServerProvider"
EOF
fi

if [ "$${enable_auth}" = "true" ] && [ "$${auth_type}" = "token" ] && [ ! -z "$${token_auth_credentials}" ]; then
  cat <<EOF > .env
CHROMA_SERVER_AUTH_CREDENTIALS="$${token_auth_credentials}"
CHROMA_SERVER_AUTH_CREDENTIALS_PROVIDER="chromadb.auth.token.TokenConfigServerAuthCredentialsProvider"
CHROMA_SERVER_AUTH_PROVIDER="chromadb.auth.token.TokenAuthServerProvider"
EOF
fi

cat <<EOF > docker-compose.override.yaml
version: '3.8'
services:
  server:
    volumes:
      - /chroma-data:/chroma/chroma
EOF

COMPOSE_PROJECT_NAME=chroma docker compose up -d --build
