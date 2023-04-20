# requirements
# - docker
# - pip

# get the code 
git clone https://github.com/chroma-core/chroma.git

#checkout the right branch
cd chroma

# run docker
cd chroma-server
docker-compose up -d --build

# install chroma-client
cd ../chroma-client
pip install --upgrade pip # you have to do this or it will use UNKNOWN as the package name
pip install .