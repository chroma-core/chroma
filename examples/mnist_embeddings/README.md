# Basic Example of extracting embeddings from a simple MNIST classifier

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

# Extract the embeddings

```bash
python3 main_embeddings.py --input-model mnist_cnn.pt
```

This will run inference on the MNIST test set, and for each element extract the output of the
first fully connected layer. Right now it just stores each element in an array as it gets it, then prints out the array at the end.
