# mutate all the things

from gql import gql

# Abstract mutations
append_tag_by_name_to_datapoints_mutation = gql(
    """
    mutation appendTagByNameToDatapoints($tagName: String!, $datapointIds: [Int!], $target: [String!]) {
        appendTagByNameToDatapoints(data: {
            tagName: $tagName, datapointIds: $datapointIds,  target: $target
        } ) {
            id
            tags {
                id
                name
            }
        }
    }
  """
)

remove_tag_by_name_from_datapoints_mutation = gql(
    """
    mutation removeTagFromDatapoints($tagName: String!, $datapointIds: [Int!], $target: [String!]) {
        removeTagFromDatapoints(data: {
            tagName: $tagName, datapointIds: $datapointIds,  target: $target
        } ) {
            ... on ObjectDeleted {
        message
        }
        }
    }
  """
)


run_projector_on_embedding_sets_mutuation = gql(
    """
    mutation runProjectorOnEmbeddingSets($embeddingSetIds: [Int!]!){
        runProjectorOnEmbeddingSets(embeddingSetIds: $embeddingSetIds) 
    }
    """
)

run_compute_class_distances_mutation = gql(
    """
    mutation computeClassDistances($trainingDatasetId: Int!, $targetDatasetId: Int!){
        computeClassDistances(trainingDatasetId: $trainingDatasetId, targetDatasetId: $targetDatasetId)
    }
    """
)

gql_batch_create_embeddings = gql(
    """
    mutation batchCreateEmbeddings($embeddingsInput: EmbeddingsInput!) {
        addEmbeddings(embeddingsInput: $embeddingsInput) {
            id
            data
            embeddingSet {
                id
            }
        }
    }
    """
)

create_or_get_dataset_mutation = gql(
    """
    mutation createOrGetDataset($dataset: CreateDatasetInput!) {
        createOrGetDataset(dataset: $dataset) {
            id
            name
        }
    }
    """
)

create_or_get_project_mutation = gql(
    """
    mutation createOrGetProject($project: CreateProjectInput!) {
        createOrGetProject(project: $project) {
            id
            name
            datasets {
                id
            }
        }
    }
    """
)

remove_tag_from_datapoint_mutation = gql(
    """
    mutation removeTagFromDatapoint($data: TagToDataPointInput!) {
        removeTagFromDatapoint(data: $data) {
            ... on ObjectDeleted {
                __typename
                message
            }
        }
    }
    """
)

append_tag_to_datapoint_mutation = gql(
    """
    mutation appendTagToDatapoint($data: TagToDataPointInput!) {
        appendTagToDatapoint(data: $data) {
            id
        }
    }
    """
)

create_datapoint_set_mutation = gql(
    """
    mutation createDatapointSet($data: CreateDatapointSetInput!) {
        createDatapointSet(data: $data) {
            id
            label {
                id
                data
            }
            resource {
                id
                uri
            }
            dataset {
                id
                name
            }
        }
    }
    """
)

# returns true or false for now
create_batch_datapoint_embedding_set_mutation = gql(
    """
    mutation createBatchDatapointEmbeddingSet($batchData: CreateBatchDatapointEmbeddingSetInput!) {
        createBatchDatapointEmbeddingSet(batchData: $batchData) 
    }
    """
)

create_datapoint_embedding_set_mutation = gql(
    """
    mutation createDatapointEmbeddingSet($data: CreateDatapointEmbeddingSetInput!) {
        createDatapointEmbeddingSet(data: $data) {
            id
            label {
                id
                data
            }
            inference {
                id
                data
            }
            resource {
                id
                uri
            }
            dataset {
                id
                name
            }
            embeddings {
                id
            }
        }
    }
    """
)

# Project mutations
create_project_mutation = gql(
    """
    mutation createProject($project: CreateProjectInput!) {
        createProject(project: $project) {
            id
            name
            createdAt
            updatedAt
        }
    }
    """
)

update_project_mutation = gql(
    """
    mutation updateProject($project: UpdateProjectInput!) {
        updateProject(project: $project) {
            id
            name
        }
    }
    """
)

delete_project_mutation = gql(
    """
    mutation deleteProject($project: UpdateProjectInput!) {
        deleteProject(project: $project) {
            ... on ObjectDeleted {
                __typename
                message
            }
        }
    }
    """
)

# dataset mutations
create_dataset_mutation = gql(
    """
    mutation createDataset($dataset: CreateDatasetInput!) {
        createDataset(dataset: $dataset) {
            ... on Dataset {
                id
                name
                project {
                    id
                }
            }
            ... on ProjectDoesNotExist {
                message
            }
        }
    }
    """
)

update_dataset_mutation = gql(
    """
    mutation updateDataset($dataset: UpdateDatasetInput!) {
        updateDataset(dataset: $dataset) {
            id
            name
            project {
                id
            }
        }
    }
    """
)

delete_dataset_mutation = gql(
    """
    mutation deleteDataset($dataset: UpdateDatasetInput!) {
        deleteDataset(dataset: $dataset) {
            ... on ObjectDeleted {
                __typename
                message
            }
        }
    }
    """
)

# tag mutations
create_tag_mutation = gql(
    """
    mutation createTag($tag: CreateTagInput!) {
        createTag(tag: $tag) {
            ... on Tag {
                id
                name
            }
        }
    }
    """
)

update_tag_mutation = gql(
    """
    mutation updateTag($tag: UpdateTagInput!) {
        updateTag(tag: $tag) {
            id
            name
        }
    }
    """
)

delete_tag_mutation = gql(
    """
    mutation deleteTag($tag: UpdateTagInput!) {
        deleteTag(tag: $tag) {
            ... on ObjectDeleted {
                __typename
                message
            }
        }
    }
    """
)

# these are not added individually typically and instead are added a set in batch
# resources
# datapoints
# labels
# inferences
# create_resource_mutation = gql(
#     """
#     mutation createResource($resource: CreateResourceInput!) {
#         createResource(resource: $resource) {
#             ... on Resource {
#                 id
#             }
#         }
#     }
#     """
# )

# update_resource_mutation = gql(
#     """
#     mutation updateResource($resource: UpdateResourceInput!) {
#         updateResource(resource: $resource) {
#             id
#         }
#     }
#     """
# )

# delete_resource_mutation = gql(
#     """
#     mutation deleteResource($resource: UpdateResourceInput!) {
#         deleteResource(resource: $resource) {
#             ... on ObjectDeleted {
#                 __typename
#                 message
#             }
#         }
#     }
#     """
# )


# job mutations
create_job_mutation = gql(
    """
    mutation createJob($job: CreateJobInput!) {
        createJob(job: $job) {
            ... on Job {
                id
                name
            }
        }
    }
    """
)

update_job_mutation = gql(
    """
    mutation updateJob($job: UpdateJobInput!) {
        updateJob(job: $job) {
            id
            name
        }
    }
    """
)

delete_job_mutation = gql(
    """
    mutation deleteJob($job: UpdateJobInput!) {
        deleteJob(job: $job) {
            ... on ObjectDeleted {
                __typename
                message
            }
        }
    }
    """
)

# Resource mutations
create_resource_mutation = gql(
    """
    mutation createResource($resource: CreateResourceInput!) {
        createResource(resource: $resource) {
            id
            uri
            createdAt
            updatedAt
        }
    }
    """
)

update_resource_mutation = gql(
    """
    mutation updateResource($resource: UpdateResourceInput!) {
        updateResource(resource: $resource) {
            id
            uri
        }
    }
    """
)

delete_resource_mutation = gql(
    """
    mutation deleteResource($Resource: UpdateResourceInput!) {
        deleteResource(Resource: $Resource) {
            ... on ObjectDeleted {
                __typename
                message
            }
        }
    }
    """
)

# Label mutations
create_label_mutation = gql(
    """
    mutation createLabel($label: CreateLabelInput!) {
        createLabel(label: $label) {
            id
            data
            createdAt
            updatedAt
        }
    }
    """
)

update_label_mutation = gql(
    """
    mutation updateLabel($label: UpdateLabelInput!) {
        updateLabel(label: $label) {
            id
            data
        }
    }
    """
)

delete_label_mutation = gql(
    """
    mutation deleteLabel($label: UpdateLabelInput!) {
        deleteLabel(label: $label) {
            ... on ObjectDeleted {
                __typename
                message
            }
        }
    }
    """
)

# datapoint mutations
create_datapoint_mutation = gql(
    """
    mutation createDatapoint($datapoint: CreateDatapointInput!) {
        createDatapoint(datapoint: $datapoint) {
            ... on Datapoint {
                id
                resource {
                    id
                }
                label {
                    id
                }
                inference {
                    id
                }
                dataset {
                    id
                }
                createdAt
                updatedAt
            }
            ... on LabelDoesNotExist {
                message
            }
             ... on ResourceDoesNotExist {
                message
            } 
        }
    }
    """
)

update_datapoint_mutation = gql(
    """
    mutation updateDatapoint($datapoint: UpdateDatapointInput!) {
        updateDatapoint(datapoint: $datapoint) {
            id
            resource {
                id
            }
            label {
                id
            }
            inference {
                id
            }
            dataset {
                id
            }
        }
    }
    """
)

delete_datapoint_mutation = gql(
    """
    mutation deleteDatapoint($datapoint: UpdateDatapointInput!) {
        deleteDatapoint(datapoint: $datapoint) {
            ... on ObjectDeleted {
                __typename
                message
            }
        }
    }
    """
)

# embedding set mutations
create_embedding_set_mutation = gql(
    """
    mutation createEmbeddingSet($embeddingSet: EmbeddingSetInput!) {
        createEmbeddingSet(embeddingSet: $embeddingSet) {
            id
        }
    }
    """
)
