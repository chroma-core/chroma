import argparse
import torch
import torch.nn.functional as F
from torchvision import datasets, transforms

from functools import partial

# Use the model as defined in training
from main_training import Net

def get_and_store_layer_outputs(self, input, output, storage):
    storage.append(output.data.detach())

def infer(model, device, data_loader):
    test_loss = 0
    correct = 0
    with torch.no_grad():
        for data, target in data_loader:
            data, target = data.to(device), target.to(device)
            output = model(data)
            test_loss += F.nll_loss(output, target, reduction='sum').item()  # sum up batch loss
            pred = output.argmax(dim=1, keepdim=True)  # get the index of the max log-probability
            correct += pred.eq(target.view_as(pred)).sum().item()

    test_loss /= len(data_loader.dataset)

    print('\nTest set: Average loss: {:.4f}, Accuracy: {}/{} ({:.0f}%)\n'.format(
        test_loss, correct, len(data_loader.dataset),
        100. * correct / len(data_loader.dataset)))

def main():
    parser = argparse.ArgumentParser(description='PyTorch Embeddings Example')
    parser.add_argument('--input-model', required=True, help='Path to the trained model')
    parser.add_argument('--batch-size', type=int, default=1000, metavar='N',
                        help='input batch size for inference (default: 1000)')
    parser.add_argument('--no-cuda', action='store_true', default=False,
                        help='disables CUDA inference')
    parser.add_argument('--seed', type=int, default=1, metavar='S',
                        help='random seed (default: 1)')

    args = parser.parse_args()

    use_cuda = not args.no_cuda and torch.cuda.is_available()
    device = torch.device("cuda" if use_cuda else "cpu")

    torch.manual_seed(args.seed)

    # Load the trained model
    model = Net()
    model.load_state_dict(torch.load(args.input_model))
    model.eval()
    model.to(device)

    # Define somewhere to store the embeddings
    embedding_store = []

    # Attach the hook
    get_layer_outputs = partial(get_and_store_layer_outputs, storage=embedding_store)
    model.fc1.register_forward_hook(get_layer_outputs)

    # Use the MNIST test set
    inference_kwargs = {'batch_size': args.batch_size}
    if use_cuda:
        cuda_kwargs = {'num_workers': 1,
                       'pin_memory': True,
                       'shuffle': True}
        inference_kwargs.update(cuda_kwargs)

    transform=transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.1307,), (0.3081,))
        ])
    dataset = datasets.MNIST('../data', train=False,
                       transform=transform)
    data_loader = torch.utils.data.DataLoader(dataset, **inference_kwargs)

    # Run inference over the test set
    infer(model, device, data_loader)

    # Output stored embeddings
    print(embedding_store)

    
if __name__ == '__main__':
    main()