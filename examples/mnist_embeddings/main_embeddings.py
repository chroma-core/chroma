import argparse
from functools import partial

import numpy
import torch
import random
import torch.nn.functional as F
from PIL import Image

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Use the model as defined in training
from main_training import Net
from torchvision import datasets, transforms

from chroma.sdk import chroma_manager
from chroma.sdk.utils import nn

import json

str_options = ["New York", "San Francisco", "Atlanta", "Miami", "Dallas", "Chicago", "DC"]

# We modify the MNIST dataset to expose some information about the source data
# to allow us to uniquely identify an input in a way that we can recover it later
class CustomDataset(datasets.MNIST):
    def __getitem__(self, index):
        img, target = super().__getitem__(index)
        resource_uri = f"{'train' if self.train else 't10k'}-images-idx3-ubyte-{index}"
        return img, target, resource_uri


def infer(model, device, data_loader, chroma_storage: chroma_manager.ChromaSDK, data_to_record):
    test_loss = 0
    correct = 0
    with torch.no_grad():
        for data, target, resource_uri in data_loader:

            label_json_list = []
            for label in target.data.detach().tolist():
                label_json_list.append(
                    {
                        "annotations": [
                            {
                                "category_id": int(label),
                            }
                        ]
                    }
                )

            chroma_storage.set_labels(labels=label_json_list)
            chroma_storage.set_resource_uris(uris=list(resource_uri))

            data, target = data.to(device), target.to(device)
            output = model(data)
            test_loss += F.nll_loss(output, target, reduction="sum").item()  # sum up batch loss
            pred = output.argmax(dim=1, keepdim=True)  # get the index of the max log-probability
            correct += pred.eq(target.view_as(pred)).sum().item()

            inference_json_list = []
            for label in pred.data.detach().flatten().tolist():
                inference_json_list.append(
                    {
                        "annotations": [
                            {
                                "category_id": int(label),
                            }
                        ]
                    }
                )

            chroma_storage.set_inferences(inference_json_list)

            metadata_list = []
            for label in pred.data.detach().flatten().tolist():
                metadata_list.append(
                    {
                        "quality": random.randint(0, 100),
                        "location": str_options[random.randint(0, 6)],
                    }
                )

            chroma_storage.set_metadata(metadata_list)
            

            df = pd.DataFrame({
                # 'embedding_data': data.tolist(),
                'embedding_data':[entry['data'] for entry in chroma_storage.get_embeddings_buffer()],
                'resource_uri': list(resource_uri),
                'metadata': metadata_list,
                'infer': inference_json_list,
                'label': label_json_list
            })
            # print()
            data_to_record = pd.concat([data_to_record, df], ignore_index=True)

            chroma_storage.store_batch_embeddings()

    test_loss /= len(data_loader.dataset)

    print(
        "\nAverage loss: {:.4f}, Accuracy: {}/{} ({:.0f}%)\n".format(
            test_loss, correct, len(data_loader.dataset), 100.0 * correct / len(data_loader.dataset)
        )
    )
    return data_to_record


def main():
    parser = argparse.ArgumentParser(description="PyTorch Embeddings Example")
    parser.add_argument("--input-model", required=True, help="Path to the trained model")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        metavar="N",
        help="input batch size for inference (default: 1000)",
    )
    parser.add_argument(
        "--no-cuda", action="store_true", default=False, help="disables CUDA inference"
    )
    parser.add_argument("--seed", type=int, default=1, metavar="S", help="random seed (default: 1)")

    args = parser.parse_args()

    use_cuda = not args.no_cuda and torch.cuda.is_available()
    device = torch.device("cuda" if use_cuda else "cpu")

    torch.manual_seed(args.seed)

    # Load the trained model
    model = Net()
    model.load_state_dict(torch.load(args.input_model))
    model.eval()
    model.to(device)

    inference_kwargs = {"batch_size": args.batch_size}
    if use_cuda:
        cuda_kwargs = {"num_workers": 1, "pin_memory": True, "shuffle": True}
        inference_kwargs.update(cuda_kwargs)

    transform = transforms.Compose(
        [transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))]
    )

    mnist_category_data = json.dumps(
        [
            {"supercategory": "none", "id": 1, "name": "1"},
            {"supercategory": "none", "id": 2, "name": "2"},
            {"supercategory": "none", "id": 3, "name": "3"},
            {"supercategory": "none", "id": 4, "name": "4"},
            {"supercategory": "none", "id": 5, "name": "5"},
            {"supercategory": "none", "id": 6, "name": "6"},
            {"supercategory": "none", "id": 7, "name": "7"},
            {"supercategory": "none", "id": 8, "name": "8"},
            {"supercategory": "none", "id": 9, "name": "9"},
            {"supercategory": "none", "id": 0, "name": "0"},
        ]
    )

    train_data_to_record = pd.DataFrame()
    test_data_to_record = pd.DataFrame()

    # Run in the Chroma context
    with chroma_manager.ChromaSDK(
        project_name="MNIST-All", dataset_name="Train", categories=mnist_category_data
    ) as chroma_storage:

        # Use the MNIST training set
        train_dataset = CustomDataset("../data", train=True, transform=transform, download=True)
        data_loader = torch.utils.data.DataLoader(train_dataset, **inference_kwargs)

        # Attach the hook
        chroma_storage.attach_forward_hook(model.fc2)

        train_data_to_record = infer(model, device, data_loader, chroma_storage, train_data_to_record)

    # Run in the Chroma context
    with chroma_manager.ChromaSDK(
        project_name="MNIST-All", dataset_name="Test", categories=mnist_category_data
    ) as chroma_storage:

        # Use the MNIST test set
        test_dataset = CustomDataset("../data", train=False, transform=transform, download=True)
        data_loader = torch.utils.data.DataLoader(test_dataset, **inference_kwargs)

        # Attach the hook
        chroma_storage.attach_forward_hook(model.fc2)

        test_data_to_record = infer(model, device, data_loader, chroma_storage, test_data_to_record)

    train_data_to_record_pq = pa.Table.from_pandas(train_data_to_record)
    pq.write_table(train_data_to_record_pq, 'train_data_to_record.parquet')
    # print("test_data_to_record", test_data_to_record, len(test_data_to_record))

    test_data_to_record_pq = pa.Table.from_pandas(test_data_to_record)
    pq.write_table(test_data_to_record_pq, 'test_data_to_record.parquet')

    train_data_to_record_pq_read = pq.read_table('train_data_to_record.parquet')
    train_data_to_record_pq_read.to_pandas()
    # print("train_data_to_record_pq_read", train_data_to_record_pq_read.head())

    test_data_to_record_pq_read = pq.read_table('test_data_to_record.parquet')
    test_data_to_record_pq_read.to_pandas()
    # print("test_data_to_record_pq_read", test_data_to_record_pq_read.head())


if __name__ == "__main__":
    main()
