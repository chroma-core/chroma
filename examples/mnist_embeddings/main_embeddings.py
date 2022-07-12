import argparse
from functools import partial

import torch
import torch.nn.functional as F
from PIL import Image

# Use the model as defined in training
from main_training import Net
from torchvision import datasets, transforms

from chroma.sdk import chroma_manager
from chroma.sdk.utils import nn

# We modify the MNIST dataset to expose some information about the source data
# to allow us to uniquely identify an input in a way that we can recover it later
class CustomDataset(datasets.MNIST):
    def __getitem__(self, index):
        img, target = super().__getitem__(index)
        resource_uri = f"{'train' if self.train else 't10k'}-images-idx3-ubyte-{index}"
        return img, target, resource_uri


def infer(model, device, data_loader, chroma_storage: chroma_manager.ChromaSDK):
    test_loss = 0
    correct = 0
    with torch.no_grad():
        for data, target, resource_uri in data_loader:

            chroma_storage.set_labels(labels=target.data.detach().tolist())
            chroma_storage.set_resource_uris(uris=list(resource_uri))

            data, target = data.to(device), target.to(device)
            output = model(data)
            test_loss += F.nll_loss(output, target, reduction="sum").item()  # sum up batch loss
            pred = output.argmax(dim=1, keepdim=True)  # get the index of the max log-probability
            correct += pred.eq(target.view_as(pred)).sum().item()

            chroma_storage.set_inferences(pred.data.detach().flatten().tolist())
            chroma_storage.store_batch_embeddings()

    test_loss /= len(data_loader.dataset)

    print(
        "\nAverage loss: {:.4f}, Accuracy: {}/{} ({:.0f}%)\n".format(
            test_loss, correct, len(data_loader.dataset), 100.0 * correct / len(data_loader.dataset)
        )
    )


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

    # Run in the Chroma context
    with chroma_manager.ChromaSDK(project_name="MNIST", dataset_name="Test") as chroma_storage:

        # Use the MNIST test set
        test_dataset = CustomDataset("../data", train=False, transform=transform, download=True)
        data_loader = torch.utils.data.DataLoader(test_dataset, **inference_kwargs)

        # Attach the hook
        chroma_storage.attach_forward_hook(model.fc2)

        infer(model, device, data_loader, chroma_storage)

    # Run in the Chroma context
    with chroma_manager.ChromaSDK(project_name="MNIST", dataset_name="Train") as chroma_storage:

        # Use the MNIST test set
        train_dataset = CustomDataset("../data", train=True, transform=transform, download=True)
        data_loader = torch.utils.data.DataLoader(train_dataset, **inference_kwargs)

        # Attach the hook
        chroma_storage.attach_forward_hook(model.fc2)

        infer(model, device, data_loader, chroma_storage)


if __name__ == "__main__":
    main()
