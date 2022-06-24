# query all the things

# create project
# find project

# create dataset
# create datapoints
# create labels
# create resources
# create inferences
# create tags
# create embeddings 

# create model architectures
# create trained models
# create layer sets
# create layers

# kick off projection tasks

from gql import gql

# project
projects_query = gql(
    """
    query projectsQuery {
        projects {
            id
            name
            createdAt
            updatedAt
        }
    }
    """
)

project_query = gql(
    """
    query projectQuery($id: ID!) {
        project(id: $id) {
            id
            name
        }
    }
    """
)

# model architecture
model_architectures_query = gql(
    """
    query modelArchitecturesQuery {
        modelArchitectures {
            id
            name
            project {
                id
            }
        }
    }
    """
)

model_architecture_query = gql(
    """
    query modelArchitectureQuery($id: ID!) {
        modelArchitecture(id: $id) {
            id
            name
        }
    }
    """
)

# trained model
trained_model_query = gql(
    """
    query trainedModelQuery($id: ID!) {
        trainedModel(id: $id) {
            id
            modelArchitecture {
                id
            }
        }
    }
    """
)
trained_models_query = gql(
    """
    query trainedModelsQuery {
        trainedModels {
            id
            modelArchitecture {
                id
            }
        }
    }
    """
)

# layer set
layer_set_query = gql(
    """
    query layerSetQuery($id: ID!) {
        layerSet(id: $id) {
            id
            trainedModel {
                id
            }
        }
    }
    """
)
layer_sets_query = gql(
    """
    query layerSetsQuery {
        layerSets {
            id
            trainedModel {
                id
            }
        }
    }
    """
)

# layer
layer_query = gql(
    """
    query layerQuery($id: ID!) {
        layer(id: $id) {
            id
            layerSet {
                id
            }
        }
    }
    """
)
layers_query = gql(
    """
    query layersQuery {
        layers {
            id
            layerSet {
                id
            }
        }
    }
    """
)
# dataset
dataset_query = gql(
    """
    query datasetQuery($id: ID!) {
        dataset(id: $id) {
            id
            name
            project {
                id
            }
        }
    }
    """
)
datasets_query = gql(
    """
    query datasetsQuery {
        datasets {
            id
            name
            createdAt
            updatedAt
            project {
                id
            }
        }
    }
    """
)

# label
label_query = gql(
    """
    query labelQuery($id: ID!) {
        label(id: $id) {
            id
        }
    }
    """
)
labels_query = gql(
    """
    query labelsQuery {
        labels {
            id
        }
    }
    """
)

# resource
resource_query = gql(
    """
    query resourceQuery($id: ID!) {
        resource(id: $id) {
            id
        }
    }
    """
)
resources_query = gql(
    """
    query resourcesQuery {
        resources {
            id
        }
    }
    """
)


# datapoints
datapoint_query = gql(
    """
    query datapointQuery($id: ID!) {
        datapoint(id: $id) {
            id
        }
    }
    """
)
datapoints_query = gql(
    """
    query datapointsQuery {
        datapoints {
            id
        }
    }
    """
)


# inference
inference_query = gql(
    """
    query inferenceQuery($id: ID!) {
        inference(id: $id) {
            id
        }
    }
    """
)
inferences_query = gql(
    """
    query inferencesQuery {
        inferences {
            id
        }
    }
    """
)


# slice
slice_query = gql(
    """
    query sliceQuery($id: ID!) {
        slice(id: $id) {
            id
            name
        }
    }
    """
)
slices_query = gql(
    """
    query slicesQuery {
        slices {
            id
            name
        }
    }
    """
)


# embedding
embedding_query = gql(
    """
    query embeddingQuery($id: ID!) {
        embedding(id: $id) {
            id
        }
    }
    """
)
embeddingsByPage_query = gql(
    """
     query embeddingsByPage ($first: Int, $after: String) {
        embeddingsByPage(first: $first, after: $after) {
            pageInfo {
                hasNextPage
                hasPreviousPage
                startCursor
                endCursor
            }
            edges {
                node {
                    id
                    data
                }
                cursor
            }
        }
    }
    """
)


# projection
projection_query = gql(
    """
    query projectionQuery($id: ID!) {
        projection(id: $id) {
            id
        }
    }
    """
)
projections_query = gql(
    """
    query projectionsQuery {
        projections {
            id
        }
    }
    """
)


# projector
projector_query = gql(
    """
    query projectorQuery($id: ID!) {
        projector(id: $id) {
            id
        }
    }
    """
)
projectors_query = gql(
    """
    query projectorsQuery {
        projectors {
            id
        }
    }
    """
)


# job
job_query = gql(
    """
    query jobQuery($id: ID!) {
        job(id: $id) {
            id
            name
        }
    }
    """
)
jobs_query = gql(
    """
    query jobsQuery {
        jobs {
            id
            name
        }
    }
    """
)

# tag
tag_query = gql(
    """
    query tagQuery($id: ID!) {
        tag(id: $id) {
            id
            name
        }
    }
    """
)
tags_query = gql(
    """
    query tagsQuery {
        tags {
            id
            name
        }
    }
    """
)