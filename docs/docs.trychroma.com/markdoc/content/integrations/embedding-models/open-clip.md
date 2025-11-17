---
id: open-clip
name: OpenCLIP
---

# OpenCLIP

Chroma provides a convenient wrapper around the OpenCLIP library. This embedding function runs locally and supports both text and image embeddings, making it useful for multimodal applications.

{% Tabs %}

{% Tab label="python" %}

This embedding function relies on several python packages:
- `open-clip-torch`: Install with `pip install open-clip-torch`
- `torch`: Install with `pip install torch`
- `pillow`: Install with `pip install pillow`

```python
from chromadb.utils.embedding_functions import OpenCLIPEmbeddingFunction
import numpy as np
from PIL import Image

open_clip_ef = OpenCLIPEmbeddingFunction(
    model_name="ViT-B-32",
    checkpoint="laion2b_s34b_b79k",
    device="cpu"
)

# For text embeddings
texts = ["Hello, world!", "How are you?"]
text_embeddings = open_clip_ef(texts)

# For image embeddings
images = [np.array(Image.open("image1.jpg")), np.array(Image.open("image2.jpg"))]
image_embeddings = open_clip_ef(images)

# Mixed embeddings
mixed = ["Hello, world!", np.array(Image.open("image1.jpg"))]
mixed_embeddings = open_clip_ef(mixed)
```

You can pass in optional arguments:
- `model_name`: The name of the OpenCLIP model to use (default: "ViT-B-32")
- `checkpoint`: The checkpoint to use for the model (default: "laion2b_s34b_b79k")
- `device`: Device used for computation, "cpu" or "cuda" (default: "cpu")

{% /Tab %}

{% /Tabs %}

{% Banner type="tip" %}
OpenCLIP is great for multimodal applications where you need to embed both text and images in the same embedding space. Visit [OpenCLIP documentation](https://github.com/mlfoundations/open_clip) for more information on available models and checkpoints.
{% /Banner %}
