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
            createdAt
            updatedAt
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
            createdAt
            updatedAt
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
            createdAt
            updatedAt
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
            createdAt
            updatedAt
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
            createdAt
            updatedAt
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
            createdAt
            updatedAt
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
            createdAt
            updatedAt
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
            createdAt
            updatedAt
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
            createdAt
            updatedAt
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
            createdAt
            updatedAt
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
            data
            createdAt
            updatedAt
        }
    }
    """
)
labels_query = gql(
    """
    query labelsQuery {
        labels {
            id
            data
            createdAt
            updatedAt
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
            uri
            createdAt
            updatedAt
        }
    }
    """
)
resources_query = gql(
    """
    query resourcesQuery {
        resources {
            id
            uri
            createdAt
            updatedAt
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
            label {
                id
                data
            }
            tags {
                id
                name
            }
            resource {
                id
                uri
            }
            dataset {
                id
                name
            }
            createdAt
            updatedAt
        }
    }
    """
)
datapoints_query = gql(
    """
    query datapointsQuery {
        datapoints {
            id
            label {
                id
                data
            }
            resource {
                id
                uri
            }
            createdAt
            updatedAt
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
            createdAt
            updatedAt
        }
    }
    """
)
inferences_query = gql(
    """
    query inferencesQuery {
        inferences {
            id
            createdAt
            updatedAt
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
            createdAt
            updatedAt
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
            createdAt
            updatedAt
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
            createdAt
            updatedAt
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
                    createdAt
                    updatedAt
                }
                cursor
            }
        }
    }
    """
)

# embedding set
embedding_set_query = gql(
    """
    query projectionQuery($id: ID!) {
        embeddingSet(id: $id) {
            id
            createdAt
            updatedAt
        }
    }
    """
)
embedding_sets_query = gql(
    """
    query projectionsQuery {
        embeddingSets {
            id
            createdAt
            updatedAt
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
            createdAt
            updatedAt
        }
    }
    """
)
projections_query = gql(
    """
    query projectionsQuery {
        projections {
            id
            createdAt
            updatedAt
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
            createdAt
            updatedAt
        }
    }
    """
)
projectors_query = gql(
    """
    query projectorsQuery {
        projectors {
            id
            createdAt
            updatedAt
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
            createdAt
            updatedAt
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
            createdAt
            updatedAt
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
            createdAt
            updatedAt
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
            createdAt
            updatedAt
            datapoints {
                id
            }
        }
    }
    """
)