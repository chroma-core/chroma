# Chroma

This repository is a monorepo containing all the core components of
the Chroma product.

Contents:

- `/doc` - Project documentation
- `/chroma-client` - Python client for Chroma
- `/chroma-server` - FastAPI server used as the backend for Chroma client



### Get up and running on Linux
No requirements
```
/bin/bash -c "$(curl -fsSL https://gist.githubusercontent.com/jeffchuber/effcbac05021e863bbd634f4b7d0283d/raw/4d38b150809d6ccbc379f88433cadd86c81d32cd/chroma_setup.sh)" 
python3 chroma/bin/test.py
```

### Get up and running on Mac
Requirements
- git
- Docker & `docker-compose`
- pip

```
/bin/bash -c "$(curl -fsSL https://gist.githubusercontent.com/jeffchuber/27a3cbb28e6521c811da6398346cd35f/raw/55c2d82870436431120a9446b47f19b72d88fa31/chroma_setup_mac.sh)" 
python3 chroma/bin/test.py
```

* These urls will be swapped out for the link in the repo once it is live