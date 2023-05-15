#!/usr/bin/env bash

# install pip
apt install -y python3-pip

# install docker
sudo apt-get update
sudo apt-get -y install \
    ca-certificates \
    curl \
    gnupg \
    lsb-release

sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update

sudo apt-get -y install docker-ce docker-ce-cli containerd.io docker-compose-plugin

pip3 install docker-compose


# get the code
git clone https://oauth2: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@github.com/chroma-core/chroma.git

#checkout the right branch
cd chroma

# run docker
cd chroma-server
docker-compose up -d --build

# install chroma-client
cd ../chroma-client
pip3 install --upgrade pip # you have to do this or it will use UNKNOWN as the package name
pip3 install .
