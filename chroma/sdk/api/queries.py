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
            datasets {
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
            embeddingSets {
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
            createdAt
            updatedAt
        }
    }
    """
)
datapoints_query = gql(
    """
    query datapointsQuery($filter: FilterDatapoints!) {
        datapoints(filter: $filter) {
            id
            label {
                id
                data
            }
            resource {
                id
                uri
            }
            inference {
                id
                data
            }
            tagdatapoints {
                id
                target
                tag {
                    id
                    name
                }
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
            data
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
            data
        }
    }
    """
)

# embedding
embeddings_query = gql(
    """
    query embeddingsQuery {
        embeddings {
            id
            createdAt
            updatedAt
            data
        }
    }
    """
)
embedding_query = gql(
    """
    query embeddingQuery($id: ID!) {
        embedding(id: $id) {
            id
            createdAt
            updatedAt
            data
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
    query ebeddingSetQuery($id: ID!) {
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
    query ebeddingSetsQuery {
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
