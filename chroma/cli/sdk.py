import typer
import pprint
import json
from chroma.sdk import chroma_manager
from pygments import highlight
from pygments.lexers import JsonLexer
from pygments.formatters import TerminalFormatter

typer_app = typer.Typer()
pp = pprint.PrettyPrinter(indent=4)

def _print(json_results):
    json_object = json.loads('{"foo":"bar"}')
    json_str = json.dumps(json_results, indent=4, sort_keys=True)
    print(highlight(json_str, JsonLexer(), TerminalFormatter()))

# project cli
@typer_app.command()
def get_project(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_project(id=id))

@typer_app.command()
def get_projects():
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_projects())

@typer_app.command()
def create_project(name: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.create_project(name=name))

@typer_app.command()
def update_project(id: str = typer.Argument(...), name: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.update_project(id=id, name=name))

@typer_app.command()
def delete_project(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.delete_project(id=id))

# model architecture cli
@typer_app.command()
def get_model_architecture(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_model_architecture(id=id))

@typer_app.command()
def get_model_architectures():
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_model_architectures())

@typer_app.command()
def create_model_architecture(name: str = typer.Argument(...), project_id: int = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.create_model_architecture(name=name, project_id=project_id))

@typer_app.command()
def update_model_architecture(id: str = typer.Argument(...), name: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.update_model_architecture(id=id, name=name))

@typer_app.command()
def delete_model_architecture(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.delete_model_architecture(id=id))

# dataset cli
@typer_app.command()
def get_dataset(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_dataset(id=id))

@typer_app.command()
def get_datasets():
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_datasets())

@typer_app.command()
def create_dataset(name: str = typer.Argument(...), project_id: int = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.create_dataset(name=name, project_id=project_id))

@typer_app.command()
def update_dataset(id: str = typer.Argument(...), name: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.update_dataset(id=id, name=name))

@typer_app.command()
def delete_dataset(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.delete_dataset(id=id))

# slice cli
@typer_app.command()
def get_slice(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_slice(id=id))

@typer_app.command()
def get_slices():
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_slices())

@typer_app.command()
def create_slice(name: str = typer.Argument(...), dataset_id: int = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.create_slice(name=name, dataset_id=dataset_id))

@typer_app.command()
def update_slice(id: str = typer.Argument(...), name: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.update_slice(id=id, name=name))

@typer_app.command()
def delete_slice(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.delete_slice(id=id))

# tag cli
@typer_app.command()
def get_tag(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_tag(id=id))

@typer_app.command()
def get_tags():
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_tags())

@typer_app.command()
def create_tag(name: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.create_tag(name=name))

@typer_app.command()
def update_tag(id: str = typer.Argument(...), name: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.update_tag(id=id, name=name))

@typer_app.command()
def delete_tag(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.delete_tag(id=id))

# resource cli
@typer_app.command()
def get_resource(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_resource(id=id))

@typer_app.command()
def get_resources():
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_resources())

@typer_app.command()
def create_resource():
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.create_resource())

@typer_app.command()
def update_resource(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.update_resource(id=id))

@typer_app.command()
def delete_resource(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.delete_resource(id=id))

# trained model cli
@typer_app.command()
def get_trained_model(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_trained_model(id=id))

@typer_app.command()
def get_trained_models():
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_trained_models())

@typer_app.command()
def create_trained_model(model_architecture_id: int = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.create_trained_model(model_architecture_id=model_architecture_id))

# @typer_app.command()
# def update_trained_model(id: str = typer.Argument(...)):
#     chroma_sdk = chroma_manager.ChromaSDK()
#     _print(chroma_sdk.update_trained_model(id=id))

@typer_app.command()
def delete_trained_model(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.delete_trained_model(id=id))

# layer set cli
@typer_app.command()
def get_layer_set(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_layer_set(id=id))

@typer_app.command()
def get_layer_sets():
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_layer_sets())

@typer_app.command()
def create_layer_set(trained_model_id: int = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.create_layer_set(trained_model_id=trained_model_id))

# @typer_app.command()
# def update_layer_set(id: str = typer.Argument(...)):
#     chroma_sdk = chroma_manager.ChromaSDK()
#     _print(chroma_sdk.update_layer_set(id=id))

@typer_app.command()
def delete_layer_set(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.delete_layer_set(id=id))

# layer cli
@typer_app.command()
def get_layer(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_layer(id=id))

@typer_app.command()
def get_layers():
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_layers())

@typer_app.command()
def create_layer(layer_set_id: int = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.create_layer(layer_set_id=layer_set_id))

# @typer_app.command()
# def update_layer(id: str = typer.Argument(...)):
#     chroma_sdk = chroma_manager.ChromaSDK()
#     _print(chroma_sdk.update_layer(id=id))

@typer_app.command()
def delete_layer(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.delete_layer(id=id))

# job cli
@typer_app.command()
def get_job(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_job(id=id))

@typer_app.command()
def get_jobs():
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_jobs())

@typer_app.command()
def create_job(name: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.create_job(name=name))

@typer_app.command()
def update_job(id: str = typer.Argument(...), name: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.update_job(id=id, name=name))

@typer_app.command()
def delete_job(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.delete_job(id=id))

# projector cli
@typer_app.command()
def get_projector(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_projector(id=id))

@typer_app.command()
def get_projectors():
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_projectors())

@typer_app.command()
def create_projector():
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.create_projector())

@typer_app.command()
def update_projector(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.update_projector(id=id))

@typer_app.command()
def delete_projector(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.delete_projector(id=id))

# resource cli
@typer_app.command()
def get_resource(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_resource(id=id))

@typer_app.command()
def get_resources():
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_resources())

@typer_app.command()
def create_resource(uri: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.create_resource(uri=uri))

@typer_app.command()
def update_resource(id: str = typer.Argument(...), uri: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.update_resource(id=id, uri=uri))

@typer_app.command()
def delete_resource(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.delete_resource(id=id))

# label cli
@typer_app.command()
def get_label(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_label(id=id))

@typer_app.command()
def get_labels():
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_labels())

@typer_app.command()
def create_label(data: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.create_label(data=data))

@typer_app.command()
def update_label(id: str = typer.Argument(...), data: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.update_label(id=id, data=data))

@typer_app.command()
def delete_label(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.delete_label(id=id))

# datapoint cli
@typer_app.command()
def get_datapoint(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_datapoint(id=id))

@typer_app.command()
def get_datapoints():
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.get_datapoints())

@typer_app.command()
def create_datapoint(dataset_id: int = typer.Argument(...), resource_id: int = typer.Argument(...), label_id: int = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.create_datapoint(dataset_id=dataset_id, resource_id=resource_id, label_id=label_id))

@typer_app.command()
def update_datapoint(id: str = typer.Argument(...), resource_id: int = typer.Argument(...), label_id: int = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.update_datapoint(id=id, resource_id=resource_id, label_id=label_id))

@typer_app.command()
def delete_datapoint(id: str = typer.Argument(...)):
    chroma_sdk = chroma_manager.ChromaSDK()
    _print(chroma_sdk.delete_datapoint(id=id))



if __name__ == "__main__":
    typer_app()
