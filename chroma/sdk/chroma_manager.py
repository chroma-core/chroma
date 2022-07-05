import json
import time
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from chroma.sdk.api.mutations import (
    create_project_mutation, 
    update_project_mutation, 
    delete_project_mutation,
    create_model_architecture_mutation, 
    update_model_architecture_mutation,
    delete_model_architecture_mutation,
    create_dataset_mutation,
    update_dataset_mutation,
    delete_dataset_mutation,
    create_slice_mutation,
    update_slice_mutation,
    delete_slice_mutation,
    create_tag_mutation,
    update_tag_mutation,
    delete_tag_mutation,
    create_trained_model_mutation,
    update_trained_model_mutation,
    delete_trained_model_mutation,
    create_layer_set_mutation,
    update_layer_set_mutation,
    delete_layer_set_mutation,
    create_layer_mutation,
    update_layer_mutation,
    delete_layer_mutation,
    create_job_mutation,
    update_job_mutation,
    delete_job_mutation,
    create_projector_mutation,
    update_projector_mutation,
    delete_projector_mutation,
    create_resource_mutation,
    update_resource_mutation,
    delete_label_mutation,
    create_label_mutation,
    update_label_mutation,
    delete_resource_mutation,
    create_datapoint_mutation,
    update_datapoint_mutation,
    delete_datapoint_mutation,
    create_datapoint_set_mutation,
    append_tag_to_datapoint_mutation,
    remove_tag_to_datapoint_mutation,
    create_or_get_project_mutation,
    create_or_get_dataset_mutation,
    create_embedding_set_mutation,
    create_datapoint_embedding_set_mutation,
    create_batch_datapoint_embedding_set_mutation,
    run_projector_on_embedding_set_mtuation
    )
from chroma.sdk.api.queries import (
    projects_query, 
    project_query,
    model_architecture_query, 
    model_architectures_query,
    trained_model_query, 
    trained_models_query, 
    layer_set_query, 
    layer_sets_query,
    layer_query,
    layers_query,
    dataset_query, 
    datasets_query, 
    label_query, 
    labels_query, 
    resource_query, 
    resources_query, 
    datapoint_query,
    datapoints_query, 
    inference_query, 
    inferences_query, 
    slice_query, 
    slices_query, 
    embedding_query, 
    embeddings_query,
    embeddingsByPage_query,
    projection_query,
    projections_query, 
    projector_query,
    projectors_query,
    job_query, 
    jobs_query, 
    tag_query, 
    tags_query,
    embedding_set_query,
    embedding_sets_query
)
from .utils import hoist_to_list

class ChromaSDK:

    # Internal
    def __init__(self) -> None:
        transport = AIOHTTPTransport(url="http://127.0.0.1:8000/graphql")
        self._client = Client(transport=transport, fetch_schema_from_transport=True, execute_timeout=30)
        self._metadata_buffer = {}
    
    # Storing embeddings requires the metadata to already be available
    def set_metadata(self, input_identifiers, inference_identifiers, labels, dataset_id, embedding_set_id):
        self._clear_metadata()

        input_identifiers = hoist_to_list(input_identifiers)
        inference_identifiers = hoist_to_list(inference_identifiers)
        labels = hoist_to_list(labels)

        input_identifiers = [str(i) for i in input_identifiers]
        inference_identifiers = [str(n) for n in inference_identifiers]
        labels = [str(l) for l in labels]

        # Sanity check that we have the right number of things
        assert len(input_identifiers) == len(labels)
        assert len(inference_identifiers) == len(labels)

        self._metadata_buffer["input_identifiers"] = input_identifiers
        self._metadata_buffer["inference_identifiers"] = inference_identifiers
        self._metadata_buffer["labels"] = labels
        self._metadata_buffer["dataset_id"] = dataset_id
        self._metadata_buffer["embedding_set_id"] = embedding_set_id

    def _clear_metadata(self):
        self._metadata_buffer = {}

    def store_batch_embeddings(self, dataset):
        # Sanity check
        assert len(dataset) == len(self._metadata_buffer["input_identifiers"])

        new_embeddings = []
        for index, data in enumerate(dataset):

            label_data = {
                "categories": [
                    {
                        "id": int(self._metadata_buffer["labels"][index]),
                        "name": str(self._metadata_buffer["labels"][index]),
                        "supercategory": "none"
                    },
                ]
            }

            new_embeddings.append({
                "embeddingData": json.dumps(dataset[index]),
                "resourceUri": self._metadata_buffer["input_identifiers"][index],
                #"inferenceIdentifier": self._metadata_buffer["inference_identifiers"][index],
                "labelData": json.dumps(label_data),
                # "embeddingSetId": 1, # think more about this
                "datasetId": int(self._metadata_buffer["dataset_id"]),
                "embeddingSetId": int(self._metadata_buffer["embedding_set_id"]),
            })

        start = time.process_time()
        result = self.create_batch_datapoint_embedding_set(new_embeddings)
        elapsedtime = time.process_time() - start
        self._clear_metadata()
        return result

    def get_embeddings_page(self, after):
        params = {"first": 100, "after": after}
        result = self._client.execute(embeddingsByPage_query, variable_values=params)
        return result 
    
    def get_embeddings_pages(self):
        after = None
        all_results = []
        while True:
            result = self.get_embeddings_page(after)
            page = result["embeddingsByPage"]
            all_results.extend(page["edges"])

            page_info = page["pageInfo"]
            has_next_page = page_info["hasNextPage"]
            end_cursor = page_info["endCursor"]
            if has_next_page:
                break
            after = end_cursor
        return all_results

    # embedding sets
    async def get_embeddings_async(self):
        result = await self._client.execute(embeddings_query)
        return result

    def get_embeddings(self):
        result = self._client.execute(embeddings_query)
        return result 

    def get_embedding(self, id: int):
        params = {"id": id}
        result = self._client.execute(embedding_query, variable_values=params)
        return result 

    # Abstract  
    def run_projector_on_embedding_set_mutation(self, embeddingSetId: int):
        params = {"embeddingSetId": embeddingSetId}
        result = self._client.execute(run_projector_on_embedding_set_mtuation, variable_values=params)
        return result

    def remove_tag_to_datapoint_mutation(self, tagId: int, datapointId: int):
        params = {"data": {"tagId": tagId, "datapointId":datapointId}}
        result = self._client.execute(remove_tag_to_datapoint_mutation, variable_values=params)
        return result

    def append_tag_to_datapoint_mutation(self, tagId: int, datapointId: int):
        params = {"data": {"tagId": tagId, "datapointId":datapointId}}
        result = self._client.execute(append_tag_to_datapoint_mutation, variable_values=params)
        return result

    def create_datapoint_set(self, datasetId:int, labelData: str, resourceUri: str):
        params = {"data": {"datasetId": datasetId, "labelData":labelData, "resourceUri": resourceUri}}
        result = self._client.execute(create_datapoint_set_mutation, variable_values=params)
        return result 

    def create_datapoint_embedding_set(self, datasetId:int, labelData: str, resourceUri: str, embeddingData):
        params = {"data": {"datasetId": datasetId, "labelData":labelData, "resourceUri": resourceUri, "embeddingData": embeddingData }}
        result = self._client.execute(create_datapoint_embedding_set_mutation, variable_values=params)
        return result 

    def create_batch_datapoint_embedding_set(self, new_datapoint_embedding_sets):
        params = {"batchData": {"batchData": new_datapoint_embedding_sets}}
        result = self._client.execute(create_batch_datapoint_embedding_set_mutation, variable_values=params)
        return result 

    # Project    
    def get_projects(self):
        result = self._client.execute(projects_query)
        return result 

    def get_project(self, id: int):
        params = {"id": id}
        result = self._client.execute(project_query, variable_values=params)
        return result 

    def create_project(self, name: str):
        params = {"project": {"name": name}}
        result = self._client.execute(create_project_mutation, variable_values=params)
        return result 

    def create_or_get_project(self, name: str):
        params = {"project": {"name": name}}
        result = self._client.execute(create_or_get_project_mutation, variable_values=params)
        return result 

    def update_project(self, id: int, name: str):
        params = {"project": {"id": id, "name": name}}
        result = self._client.execute(update_project_mutation, variable_values=params)
        return result 
    
    def delete_project(self, id: int):
        params = {"project": {"id": id}}
        result = self._client.execute(delete_project_mutation, variable_values=params)
        return result 

    # model architecture
    def get_model_architectures(self):
        result = self._client.execute(model_architectures_query)
        return result 

    def get_model_architecture(self, id: int):
        params = {"id": id}
        result = self._client.execute(model_architecture_query, variable_values=params)
        return result 

    def create_model_architecture(self, name: str, project_id: int):
        params = {"modelArchitecture": {"name": name, "projectId": project_id}}
        result = self._client.execute(create_model_architecture_mutation, variable_values=params)
        return result 

    def update_model_architecture(self, id: int, name: str):
        params = {"modelArchitecture": {"id": id, "name": name}}
        result = self._client.execute(update_model_architecture_mutation, variable_values=params)
        return result 
    
    def delete_model_architecture(self, id: int):
        params = {"modelArchitecture": {"id": id}}
        result = self._client.execute(delete_model_architecture_mutation, variable_values=params)
        return result 

    # dataset
    def get_datasets(self):
        result = self._client.execute(datasets_query)
        return result 

    def get_dataset(self, id: int):
        params = {"id": id}
        result = self._client.execute(dataset_query, variable_values=params)
        return result 

    def create_dataset(self, name: str, project_id: int):
        params = {"dataset": {"name": name, "projectId": project_id}}
        result = self._client.execute(create_dataset_mutation, variable_values=params)
        return result 

    def create_or_get_dataset(self, name: str, project_id: int):
        params = {"dataset": {"name": name, "projectId": project_id}}
        result = self._client.execute(create_or_get_dataset_mutation, variable_values=params)
        return result 

    def update_dataset(self, id: int, name: str):
        params = {"dataset": {"id": id, "name": name}}
        result = self._client.execute(update_dataset_mutation, variable_values=params)
        return result 
    
    def delete_dataset(self, id: int):
        params = {"dataset": {"id": id}}
        result = self._client.execute(delete_dataset_mutation, variable_values=params)
        return result 

    # slice
    def get_slices(self):
        result = self._client.execute(slices_query)
        return result 

    def get_slice(self, id: int):
        params = {"id": id}
        result = self._client.execute(slice_query, variable_values=params)
        return result 

    def create_slice(self, name: str, dataset_id: int):
        params = {"slice": {"name": name, "datasetId": dataset_id}}
        result = self._client.execute(create_slice_mutation, variable_values=params)
        return result 

    def update_slice(self, id: int, name: str):
        params = {"slice": {"id": id, "name": name}}
        result = self._client.execute(update_slice_mutation, variable_values=params)
        return result 
    
    def delete_slice(self, id: int):
        params = {"slice": {"id": id}}
        result = self._client.execute(delete_slice_mutation, variable_values=params)
        return result 

    # tag
    def get_tags(self):
        result = self._client.execute(tags_query)
        return result 

    def get_tag(self, id: int):
        params = {"id": id}
        result = self._client.execute(tag_query, variable_values=params)
        return result 

    def create_tag(self, name: str):
        params = {"tag": {"name": name}}
        result = self._client.execute(create_tag_mutation, variable_values=params)
        return result 

    def update_tag(self, id: int, name: str):
        params = {"tag": {"id": id, "name": name}}
        result = self._client.execute(update_tag_mutation, variable_values=params)
        return result 
    
    def delete_tag(self, id: int):
        params = {"tag": {"id": id}}
        result = self._client.execute(delete_tag_mutation, variable_values=params)
        return result 

    # while these work, they are handled in batch right now and not created directly
    # resource
    # def get_resources(self):
    #     result = self._client.execute(resources_query)
    #     return result 

    # def get_resource(self, id: int):
    #     params = {"id": id}
    #     result = self._client.execute(resource_query, variable_values=params)
    #     return result 

    # def create_resource(self):
    #     params = {}
    #     result = self._client.execute(create_resource_mutation, variable_values=params)
    #     return result 

    # def update_resource(self, id: int, name: str):
    #     params = {}
    #     result = self._client.execute(update_resource_mutation, variable_values=params)
    #     return result 
    
    # def delete_resource(self, id: int):
    #     params = {"resource": {"id": id}}
    #     result = self._client.execute(delete_resource_mutation, variable_values=params)
    #     return result 

    # trained model
    def get_trained_models(self):
        result = self._client.execute(trained_models_query)
        return result 

    def get_trained_model(self, id: int):
        params = {"id": id}
        result = self._client.execute(trained_model_query, variable_values=params)
        return result 

    def create_trained_model(self, model_architecture_id: int):
        params = {"trainedModel": {"modelArchitectureId": model_architecture_id}}
        result = self._client.execute(create_trained_model_mutation, variable_values=params)
        return result 

    # we dont have any fields on this object to update yet
    # def update_trained_model(self, id: int, name: str):
    #     params = {"trained_model": {"id": id, "name": name}}
    #     result = self._client.execute(update_trained_model_mutation, variable_values=params)
    #     return result 
    
    def delete_trained_model(self, id: int):
        params = {"trainedModel": {"id": id}}
        result = self._client.execute(delete_trained_model_mutation, variable_values=params)
        return result 

    # layer set
    def get_layer_sets(self):
        result = self._client.execute(layer_sets_query)
        return result 

    def get_layer_set(self, id: int):
        params = {"id": id}
        result = self._client.execute(layer_set_query, variable_values=params)
        return result 

    def create_layer_set(self, trained_model_id: int):
        params = {"layerSet": {"trainedModelId": trained_model_id}}
        result = self._client.execute(create_layer_set_mutation, variable_values=params)
        return result 

    # we dont have any fields on this object to update yet
    # def update_layer_set(self, id: int, name: str):
    #     params = {"layer_set": {"id": id, "name": name}}
    #     result = self._client.execute(update_layer_set_mutation, variable_values=params)
    #     return result 
    
    def delete_layer_set(self, id: int):
        params = {"layerSet": {"id": id}}
        result = self._client.execute(delete_layer_set_mutation, variable_values=params)
        return result 

  # layer
    def get_layers(self):
        result = self._client.execute(layers_query)
        return result 

    def get_layer(self, id: int):
        params = {"id": id}
        result = self._client.execute(layer_query, variable_values=params)
        return result 

    def create_layer(self, layer_set_id: int):
        params = {"layer": {"layerSetId": layer_set_id}}
        result = self._client.execute(create_layer_mutation, variable_values=params)
        return result 

    # we dont have any fields on this object to update yet
    # def update_layer(self, id: int, name: str):
    #     params = {"layer": {"id": id, "name": name}}
    #     result = self._client.execute(update_layer_mutation, variable_values=params)
    #     return result 
    
    def delete_layer(self, id: int):
        params = {"layer": {"id": id}}
        result = self._client.execute(delete_layer_mutation, variable_values=params)
        return result 

    # job    
    def get_jobs(self):
        result = self._client.execute(jobs_query)
        return result 

    def get_job(self, id: int):
        params = {"id": id}
        result = self._client.execute(job_query, variable_values=params)
        return result 

    def create_job(self, name: str):
        params = {"job": {"name": name}}
        result = self._client.execute(create_job_mutation, variable_values=params)
        return result 

    def update_job(self, id: int, name: str):
        params = {"job": {"id": id, "name": name}}
        result = self._client.execute(update_job_mutation, variable_values=params)
        return result 
    
    def delete_job(self, id: int):
        params = {"job": {"id": id}}
        result = self._client.execute(delete_job_mutation, variable_values=params)
        return result 

    # projector    
    def get_projectors(self):
        result = self._client.execute(projectors_query)
        return result 

    def get_projector(self, id: int):
        params = {"id": id}
        result = self._client.execute(projector_query, variable_values=params)
        return result 

    def create_projector(self):
        params = {"projector": {}}
        result = self._client.execute(create_projector_mutation, variable_values=params)
        return result 

    # we dont have any fields on this object to update yet
    # def update_projector(self, id: int, name: str):
    #     params = {"projector": {"id": id, "name": name}}
    #     result = self._client.execute(update_projector_mutation, variable_values=params)
    #     return result 
    
    def delete_projector(self, id: int):
        params = {"projector": {"id": id}}
        result = self._client.execute(delete_projector_mutation, variable_values=params)
        return result 

    # Resource    
    def get_resources(self):
        result = self._client.execute(resources_query)
        return result 

    def get_resource(self, id: int):
        params = {"id": id}
        result = self._client.execute(resource_query, variable_values=params)
        return result 

    def create_resource(self, uri: str):
        params = {"resource": {"uri": uri}}
        result = self._client.execute(create_resource_mutation, variable_values=params)
        return result 

    def update_resource(self, id: int, uri: str):
        params = {"resource": {"id": id, "uri": uri}}
        result = self._client.execute(update_resource_mutation, variable_values=params)
        return result 
    
    def delete_resource(self, id: int):
        params = {"resource": {"id": id}}
        result = self._client.execute(delete_resource_mutation, variable_values=params)
        return result 

    # Label    
    def get_labels(self):
        result = self._client.execute(labels_query)
        return result 

    def get_label(self, id: int):
        params = {"id": id}
        result = self._client.execute(label_query, variable_values=params)
        return result 

    def create_label(self, data: str):
        params = {"label": {"data": data}}
        result = self._client.execute(create_label_mutation, variable_values=params)
        return result 

    def update_label(self, id: int, data: str):
        params = {"label": {"id": id, "data": data}}
        result = self._client.execute(update_label_mutation, variable_values=params)
        return result 
    
    def delete_label(self, id: int):
        params = {"label": {"id": id}}
        result = self._client.execute(delete_label_mutation, variable_values=params)
        return result 

    # Datapoint    
    def get_datapoints(self):
        result = self._client.execute(datapoints_query)
        return result 

    def get_datapoint(self, id: int):
        params = {"id": id}
        result = self._client.execute(datapoint_query, variable_values=params)
        return result 

    def create_datapoint(self, dataset_id: int, resource_id: int, label_id: int):
        params = {"datapoint": {"datasetId": dataset_id, "resourceId": resource_id, "labelId": label_id}}
        result = self._client.execute(create_datapoint_mutation, variable_values=params)
        return result 

    def update_datapoint(self, id: int, resource_id: int, label_id: int):
        params = {"datapoint": {"id": id, "resourceId": resource_id, "labelId": label_id}}
        result = self._client.execute(update_datapoint_mutation, variable_values=params)
        return result 
    
    def delete_datapoint(self, id: int):
        params = {"datapoint": {"id": id}}
        result = self._client.execute(delete_datapoint_mutation, variable_values=params)
        return result 

    # embedding sets
    def get_embedding_sets(self):
        result = self._client.execute(embedding_sets_query)
        return result 

    def get_embedding_set(self, id: int):
        params = {"id": id}
        result = self._client.execute(embedding_set_query, variable_values=params)
        return result 

    def create_embedding_set(self, dataset_id: int):
        params = {"embeddingSet": {"datasetId": dataset_id}}
        result = self._client.execute(create_embedding_set_mutation, variable_values=params)
        return result 