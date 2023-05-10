# requirements
# - docker
# - pip

# get the code
git clone https://oauth2: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@github.com/chroma-core/chroma.git

#checkout the right branch
cd chroma

# run docker
cd chroma-server
docker-compose up -d --build

# install chroma-client
cd ../chroma-client
pip install --upgrade pip # you have to do this or it will use UNKNOWN as the package name
pip install .
