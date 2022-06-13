# Basic Example of extracting embeddings from a simple MNIST classifier

<!-- ### Example

TODO: make sure this works... gives a pretty result ... and is better documented
```
pip install chroma-core
chroma application run
cd chroma-core/examples
pip install -r requirements.txt
python main_training.py --save-model
python3 main_embeddings.py --input-model mnist_cnn.pt
```

### Integrating into your code

TODO: lots obviously
```
import chroma
embedding_store = data_manager.ChromaDataManager()
embedding_store.store_batch_embeddings()
``` -->


# Install Requirements

```bash
pip install -r requirements.txt
```

# Train the MNIST classifier

```bash
python main_training.py --save-model
# CUDA_VISIBLE_DEVICES=2 python main_training.py --save-model  # to specify GPU id to ex. 2
```

This will create a state dict at `./minst_cnn.pt`

# Run the chroma data manager
```bash
FLASK_APP=../../data_manager/main.py FLASK_ENV=development flask run
```

# Extract the embeddings

```bash
python3 main_embeddings.py --input-model mnist_cnn.pt
```

This will run inference on the MNIST test set, and for each element extract the output of the
first fully connected layer. 
