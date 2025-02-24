# Building Hardware Optimized ChromaDB Image

The default Chroma DB image comes with binary distribution of hnsw lib which is not optimized to take advantage of
certain CPU architectures (Intel-based) with AVX support. This can be improved by building an image with hnsw rebuilt
from source. To do that run:

```bash
docker build -t chroma-test1 --build-arg REBUILD_HNSWLIB=true --no-cache .
```
