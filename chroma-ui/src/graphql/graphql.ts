import gql from 'graphql-tag';
import * as Urql from 'urql';
export type Maybe<T> = T | null;
export type InputMaybe<T> = Maybe<T>;
export type Exact<T extends { [key: string]: unknown }> = { [K in keyof T]: T[K] };
export type MakeOptional<T, K extends keyof T> = Omit<T, K> & { [SubKey in K]?: Maybe<T[SubKey]> };
export type MakeMaybe<T, K extends keyof T> = Omit<T, K> & { [SubKey in K]: Maybe<T[SubKey]> };
export type Omit<T, K extends keyof T> = Pick<T, Exclude<keyof T, K>>;
/** All built-in and custom scalars, mapped to their actual values */
export type Scalars = {
  ID: string;
  String: string;
  Boolean: boolean;
  Int: number;
  Float: number;
  /** Date with time (isoformat) */
  DateTime: any;
  /** The `JSON` scalar type represents JSON values as specified by [ECMA-404](http://www.ecma-international.org/publications/files/ECMA-ST/ECMA-404.pdf). */
  JSON: any;
};

export type AddDatapointResponse = Datapoint | LabelDoesNotExist | ResourceDoesNotExist;

export type AddDatasetResponse = Dataset | ProjectDoesNotExist;

export type CreateBatchDatapointEmbeddingSetInput = {
  batchData: Array<CreateDatapointEmbeddingSetInput>;
};

export type CreateDatapointEmbeddingSetInput = {
  ctxEmbeddingData?: InputMaybe<Array<Scalars['String']>>;
  ctxEmbeddingSetId?: InputMaybe<Scalars['Int']>;
  datasetId: Scalars['Int'];
  embeddingData: Array<Scalars['String']>;
  embeddingSetId: Scalars['Int'];
  inferenceData: Scalars['String'];
  labelData: Scalars['String'];
  metadata?: InputMaybe<Scalars['String']>;
  resourceUri: Scalars['String'];
};

export type CreateDatapointInput = {
  datasetId: Scalars['Int'];
  labelId?: InputMaybe<Scalars['Int']>;
  resourceId: Scalars['Int'];
};

export type CreateDatapointSetInput = {
  datasetId: Scalars['Int'];
  labelData: Scalars['String'];
  resourceUri: Scalars['String'];
};

export type CreateDatasetInput = {
  categories?: InputMaybe<Scalars['String']>;
  name: Scalars['String'];
  projectId: Scalars['Int'];
};

export type CreateJobInput = {
  name: Scalars['String'];
};

export type CreateLabelInput = {
  data: Scalars['String'];
};

export type CreateProjectInput = {
  name: Scalars['String'];
};

export type CreateResourceInput = {
  uri: Scalars['String'];
};

export type CreateTagInput = {
  name: Scalars['String'];
};

export type Datapoint = {
  __typename?: 'Datapoint';
  createdAt: Scalars['DateTime'];
  dataset: Dataset;
  embeddings: Array<Embedding>;
  id: Scalars['ID'];
  label: Label;
  metadata_?: Maybe<Scalars['String']>;
  resource: Resource;
  resourceId?: Maybe<Scalars['Int']>;
  tags: Array<Tag>;
  updatedAt: Scalars['DateTime'];
};

export type Dataset = {
  __typename?: 'Dataset';
  categories?: Maybe<Scalars['JSON']>;
  createdAt: Scalars['DateTime'];
  datapoints: Array<Datapoint>;
  embeddingSets: Array<EmbeddingSet>;
  id: Scalars['ID'];
  name?: Maybe<Scalars['String']>;
  project: Project;
  projectId?: Maybe<Scalars['Int']>;
  updatedAt: Scalars['DateTime'];
};

export type Embedding = {
  __typename?: 'Embedding';
  createdAt: Scalars['DateTime'];
  data?: Maybe<Scalars['String']>;
  datapoint: Datapoint;
  datapointId?: Maybe<Scalars['Int']>;
  embeddingSet: EmbeddingSet;
  embeddingSetId?: Maybe<Scalars['Int']>;
  id: Scalars['ID'];
  projections: Array<Projection>;
  updatedAt: Scalars['DateTime'];
};

export type EmbeddingConnection = {
  __typename?: 'EmbeddingConnection';
  edges: Array<EmbeddingEdge>;
  pageInfo: PageInfo;
};

export type EmbeddingEdge = {
  __typename?: 'EmbeddingEdge';
  cursor: Scalars['String'];
  node: Embedding;
};

export type EmbeddingSet = {
  __typename?: 'EmbeddingSet';
  createdAt: Scalars['DateTime'];
  dataset: Dataset;
  embeddings: Array<Embedding>;
  id: Scalars['ID'];
  projectionSets: Array<ProjectionSet>;
  updatedAt: Scalars['DateTime'];
};

export type EmbeddingSetInput = {
  datasetId: Scalars['Int'];
};

export type FilterDatapoints = {
  datasetId?: InputMaybe<Scalars['Int']>;
  tagName?: InputMaybe<Scalars['String']>;
};

export type FilterProjectionSets = {
  projectId?: InputMaybe<Scalars['Int']>;
};

export type ImageAndSize = {
  __typename?: 'ImageAndSize';
  imageData: Scalars['String'];
  originalHeight: Scalars['Int'];
  originalWidth: Scalars['Int'];
};

export type Inference = {
  __typename?: 'Inference';
  createdAt: Scalars['DateTime'];
  datapoint?: Maybe<Datapoint>;
  id: Scalars['ID'];
  updatedAt: Scalars['DateTime'];
};

export type Job = {
  __typename?: 'Job';
  createdAt: Scalars['DateTime'];
  id: Scalars['ID'];
  name?: Maybe<Scalars['String']>;
  updatedAt: Scalars['DateTime'];
};

export type Label = {
  __typename?: 'Label';
  createdAt: Scalars['DateTime'];
  data: Scalars['JSON'];
  id: Scalars['ID'];
  updatedAt: Scalars['DateTime'];
};

export type LabelDoesNotExist = {
  __typename?: 'LabelDoesNotExist';
  message: Scalars['String'];
};

export type Mutation = {
  __typename?: 'Mutation';
  addProjection: Projection;
  addProjectionSet: ProjectionSet;
  appendTagByNameToDatapoints: Array<Datapoint>;
  computeClassDistances: Scalars['Boolean'];
  createBatchDatapointEmbeddingSet: Scalars['Boolean'];
  createDatapoint: AddDatapointResponse;
  createDatapointSet: Datapoint;
  createDataset: AddDatasetResponse;
  createEmbeddingSet: EmbeddingSet;
  createJob: Job;
  createLabel: Label;
  createOrGetDataset: Dataset;
  createOrGetProject: Project;
  createProject: Project;
  createResource: Resource;
  createTag: Tag;
  deleteDatapoint: ObjectDeleted;
  deleteDataset: ObjectDeleted;
  deleteJob: ObjectDeleted;
  deleteLabel: ObjectDeleted;
  deleteProject: ObjectDeleted;
  deleteResource: ObjectDeleted;
  deleteTag: ObjectDeleted;
  removeTagFromDatapoints: ObjectDeleted;
  runProjectorOnEmbeddingSets: Scalars['Boolean'];
  updateDatapoint: Datapoint;
  updateDataset: Dataset;
  updateJob: Job;
  updateLabel: Label;
  updateProject: Project;
  updateResource: Resource;
  updateTag: Tag;
};


export type MutationAddProjectionArgs = {
  projectionInput: ProjectionInput;
};


export type MutationAddProjectionSetArgs = {
  projectionSetInput: ProjectionSetInput;
};


export type MutationAppendTagByNameToDatapointsArgs = {
  data: TagByNameToDataPointsInput;
};


export type MutationComputeClassDistancesArgs = {
  targetDatasetId: Scalars['Int'];
  trainingDatasetId: Scalars['Int'];
};


export type MutationCreateBatchDatapointEmbeddingSetArgs = {
  batchData: CreateBatchDatapointEmbeddingSetInput;
};


export type MutationCreateDatapointArgs = {
  datapoint: CreateDatapointInput;
};


export type MutationCreateDatapointSetArgs = {
  data: CreateDatapointSetInput;
};


export type MutationCreateDatasetArgs = {
  dataset: CreateDatasetInput;
};


export type MutationCreateEmbeddingSetArgs = {
  embeddingSet: EmbeddingSetInput;
};


export type MutationCreateJobArgs = {
  job: CreateJobInput;
};


export type MutationCreateLabelArgs = {
  label: CreateLabelInput;
};


export type MutationCreateOrGetDatasetArgs = {
  dataset: CreateDatasetInput;
};


export type MutationCreateOrGetProjectArgs = {
  project: CreateProjectInput;
};


export type MutationCreateProjectArgs = {
  project: CreateProjectInput;
};


export type MutationCreateResourceArgs = {
  resource: CreateResourceInput;
};


export type MutationCreateTagArgs = {
  tag: CreateTagInput;
};


export type MutationDeleteDatapointArgs = {
  datapoint: UpdateDatapointInput;
};


export type MutationDeleteDatasetArgs = {
  dataset: UpdateDatasetInput;
};


export type MutationDeleteJobArgs = {
  job: UpdateJobInput;
};


export type MutationDeleteLabelArgs = {
  label: UpdateLabelInput;
};


export type MutationDeleteProjectArgs = {
  project: UpdateProjectInput;
};


export type MutationDeleteResourceArgs = {
  resource: UpdateResourceInput;
};


export type MutationDeleteTagArgs = {
  tag: UpdateTagInput;
};


export type MutationRemoveTagFromDatapointsArgs = {
  data: TagByNameToDataPointsInput;
};


export type MutationRunProjectorOnEmbeddingSetsArgs = {
  embeddingSetIds: Array<Scalars['Int']>;
};


export type MutationUpdateDatapointArgs = {
  datapoint: UpdateDatapointInput;
};


export type MutationUpdateDatasetArgs = {
  dataset: UpdateDatasetInput;
};


export type MutationUpdateJobArgs = {
  job: UpdateJobInput;
};


export type MutationUpdateLabelArgs = {
  label: UpdateLabelInput;
};


export type MutationUpdateProjectArgs = {
  project: UpdateProjectInput;
};


export type MutationUpdateResourceArgs = {
  resource: UpdateResourceInput;
};


export type MutationUpdateTagArgs = {
  tag: UpdateTagInput;
};

export type ObjectDeleted = {
  __typename?: 'ObjectDeleted';
  message: Scalars['String'];
};

export type PageInfo = {
  __typename?: 'PageInfo';
  endCursor?: Maybe<Scalars['String']>;
  hasNextPage: Scalars['Boolean'];
  hasPreviousPage: Scalars['Boolean'];
  startCursor?: Maybe<Scalars['String']>;
};

export type PageInput = {
  after?: InputMaybe<Scalars['String']>;
  first: Scalars['Int'];
};

export type Project = {
  __typename?: 'Project';
  createdAt: Scalars['DateTime'];
  datasets: Array<Dataset>;
  id: Scalars['ID'];
  name?: Maybe<Scalars['String']>;
  updatedAt: Scalars['DateTime'];
};

export type ProjectDoesNotExist = {
  __typename?: 'ProjectDoesNotExist';
  message: Scalars['String'];
};

export type Projection = {
  __typename?: 'Projection';
  createdAt: Scalars['DateTime'];
  embedding: Embedding;
  embeddingId?: Maybe<Scalars['Int']>;
  id: Scalars['ID'];
  projectionSet: ProjectionSet;
  projectionSetId?: Maybe<Scalars['Int']>;
  updatedAt: Scalars['DateTime'];
  x: Scalars['Float'];
  y: Scalars['Float'];
};

export type ProjectionInput = {
  embeddingId: Scalars['Int'];
  projectionSetId: Scalars['Int'];
  x: Scalars['Float'];
  y: Scalars['Float'];
};

export type ProjectionSet = {
  __typename?: 'ProjectionSet';
  createdAt: Scalars['DateTime'];
  embeddingSet: EmbeddingSet;
  id: Scalars['ID'];
  project: Project;
  projectId?: Maybe<Scalars['Int']>;
  projections: Array<Projection>;
  setType: Scalars['String'];
  updatedAt: Scalars['DateTime'];
};

export type ProjectionSetInput = {
  projectionSetId: Scalars['Int'];
};

export type Query = {
  __typename?: 'Query';
  datapoint: Datapoint;
  datapoints: Array<Datapoint>;
  dataset: Dataset;
  datasets: Array<Dataset>;
  embedding: Embedding;
  embeddingSet: EmbeddingSet;
  embeddingSets: Array<EmbeddingSet>;
  embeddings: Array<Embedding>;
  embeddingsByPage: EmbeddingConnection;
  imageResolver: ImageAndSize;
  inference: Inference;
  inferences: Array<Inference>;
  job: Job;
  jobs: Array<Job>;
  label: Label;
  labels: Array<Label>;
  mnistImage: Scalars['String'];
  project: Project;
  projection: Projection;
  projectionSets: Array<ProjectionSet>;
  projections: Array<Projection>;
  projects: Array<Project>;
  resource: Resource;
  resources: Array<Resource>;
  tag: Tag;
  tags: Array<Tag>;
};


export type QueryDatapointArgs = {
  id: Scalars['ID'];
};


export type QueryDatapointsArgs = {
  filter: FilterDatapoints;
};


export type QueryDatasetArgs = {
  id: Scalars['ID'];
};


export type QueryEmbeddingArgs = {
  id: Scalars['ID'];
};


export type QueryEmbeddingSetArgs = {
  id: Scalars['ID'];
};


export type QueryEmbeddingsByPageArgs = {
  pageInput: PageInput;
};


export type QueryImageResolverArgs = {
  cropHeight: Scalars['Float'];
  cropWidth: Scalars['Float'];
  identifier: Scalars['String'];
  leftOffset: Scalars['Float'];
  resolverName: Scalars['String'];
  thumbnail: Scalars['Boolean'];
  topOffset: Scalars['Float'];
};


export type QueryInferenceArgs = {
  id: Scalars['ID'];
};


export type QueryJobArgs = {
  id: Scalars['ID'];
};


export type QueryLabelArgs = {
  id: Scalars['ID'];
};


export type QueryMnistImageArgs = {
  identifier: Scalars['String'];
};


export type QueryProjectArgs = {
  id: Scalars['ID'];
};


export type QueryProjectionArgs = {
  id: Scalars['ID'];
};


export type QueryProjectionSetsArgs = {
  filter: FilterProjectionSets;
};


export type QueryResourceArgs = {
  id: Scalars['ID'];
};


export type QueryTagArgs = {
  id: Scalars['ID'];
};

export type Resource = {
  __typename?: 'Resource';
  createdAt: Scalars['DateTime'];
  datapoints: Array<Datapoint>;
  id: Scalars['ID'];
  updatedAt: Scalars['DateTime'];
  uri: Scalars['String'];
};

export type ResourceDoesNotExist = {
  __typename?: 'ResourceDoesNotExist';
  message: Scalars['String'];
};

export type Tag = {
  __typename?: 'Tag';
  createdAt: Scalars['DateTime'];
  datapoints: Array<Datapoint>;
  id: Scalars['ID'];
  name?: Maybe<Scalars['String']>;
  updatedAt: Scalars['DateTime'];
};

export type TagByNameToDataPointsInput = {
  datapointIds?: InputMaybe<Array<Scalars['Int']>>;
  tagName: Scalars['String'];
  target?: InputMaybe<Array<Scalars['String']>>;
};

export type UpdateDatapointInput = {
  id: Scalars['ID'];
  inferenceId?: InputMaybe<Scalars['Int']>;
  labelId?: InputMaybe<Scalars['Int']>;
  resourceId: Scalars['Int'];
};

export type UpdateDatasetInput = {
  categories?: InputMaybe<Scalars['String']>;
  id: Scalars['ID'];
  name?: InputMaybe<Scalars['String']>;
};

export type UpdateJobInput = {
  id: Scalars['ID'];
  name?: InputMaybe<Scalars['String']>;
};

export type UpdateLabelInput = {
  data: Scalars['String'];
  id: Scalars['ID'];
};

export type UpdateProjectInput = {
  id: Scalars['ID'];
  name?: InputMaybe<Scalars['String']>;
};

export type UpdateResourceInput = {
  id: Scalars['ID'];
  uri: Scalars['String'];
};

export type UpdateTagInput = {
  id: Scalars['ID'];
  name?: InputMaybe<Scalars['String']>;
};

export type ProjectFieldsFragment = { __typename?: 'Project', id: string, name?: string | null };

export type GetProjectsQueryVariables = Exact<{ [key: string]: never; }>;


export type GetProjectsQuery = { __typename?: 'Query', projects: Array<{ __typename?: 'Project', id: string, name?: string | null }> };

export type GetProjectQueryVariables = Exact<{
  id: Scalars['ID'];
}>;


export type GetProjectQuery = { __typename?: 'Query', project: { __typename?: 'Project', id: string, name?: string | null } };

export type JobFieldsFragment = { __typename?: 'Job', id: string };

export type GetJobsQueryVariables = Exact<{ [key: string]: never; }>;


export type GetJobsQuery = { __typename?: 'Query', jobs: Array<{ __typename?: 'Job', id: string }> };

export type GetJobQueryVariables = Exact<{
  id: Scalars['ID'];
}>;


export type GetJobQuery = { __typename?: 'Query', job: { __typename?: 'Job', id: string } };

export type ProjectionFieldsFragment = { __typename?: 'Projection', id: string, x: number, y: number };

export type GetProjectionsQueryVariables = Exact<{ [key: string]: never; }>;


export type GetProjectionsQuery = { __typename?: 'Query', projections: Array<{ __typename?: 'Projection', id: string, x: number, y: number }> };

export type GetProjectionQueryVariables = Exact<{
  id: Scalars['ID'];
}>;


export type GetProjectionQuery = { __typename?: 'Query', projection: { __typename?: 'Projection', id: string, x: number, y: number } };

export type AddProjectionMutationVariables = Exact<{
  projectionInput: ProjectionInput;
}>;


export type AddProjectionMutation = { __typename?: 'Mutation', addProjection: { __typename: 'Projection', id: string, x: number, y: number } };

export type ProjectionSetFieldsFragment = { __typename?: 'ProjectionSet', id: string, projections: Array<{ __typename?: 'Projection', id: string, x: number, y: number }> };

export type AddProjectionSetMutationVariables = Exact<{
  projectionSetInput: ProjectionSetInput;
}>;


export type AddProjectionSetMutation = { __typename?: 'Mutation', addProjectionSet: { __typename: 'ProjectionSet', id: string, projections: Array<{ __typename?: 'Projection', id: string, x: number, y: number }> } };

export type EmbeddingFieldsFragment = { __typename?: 'Embedding', id: string, data?: string | null };

export type GetEmbeddingsQueryVariables = Exact<{ [key: string]: never; }>;


export type GetEmbeddingsQuery = { __typename?: 'Query', embeddings: Array<{ __typename?: 'Embedding', id: string, data?: string | null }> };

export type GetEmbeddingQueryVariables = Exact<{
  id: Scalars['ID'];
}>;


export type GetEmbeddingQuery = { __typename?: 'Query', embedding: { __typename?: 'Embedding', id: string, data?: string | null } };

export type PageInfoFieldsFragment = { __typename?: 'PageInfo', hasNextPage: boolean, hasPreviousPage: boolean, startCursor?: string | null, endCursor?: string | null };

export type EmbeddingsByPageQueryVariables = Exact<{
  pageInput: PageInput;
}>;


export type EmbeddingsByPageQuery = { __typename?: 'Query', embeddingsByPage: { __typename?: 'EmbeddingConnection', pageInfo: { __typename?: 'PageInfo', hasNextPage: boolean, hasPreviousPage: boolean, startCursor?: string | null, endCursor?: string | null }, edges: Array<{ __typename?: 'EmbeddingEdge', cursor: string, node: { __typename?: 'Embedding', id: string, data?: string | null } }> } };

export type AppendTagByNameToDatapointsMutationVariables = Exact<{
  tagName: Scalars['String'];
  datapointIds?: InputMaybe<Array<Scalars['Int']> | Scalars['Int']>;
  target?: InputMaybe<Array<Scalars['String']> | Scalars['String']>;
}>;


export type AppendTagByNameToDatapointsMutation = { __typename?: 'Mutation', appendTagByNameToDatapoints: Array<{ __typename?: 'Datapoint', id: string, tags: Array<{ __typename?: 'Tag', id: string, name?: string | null }> }> };

export type RemoveTagFromDatapointsMutationVariables = Exact<{
  tagName: Scalars['String'];
  datapointIds?: InputMaybe<Array<Scalars['Int']> | Scalars['Int']>;
  target?: InputMaybe<Array<Scalars['String']> | Scalars['String']>;
}>;


export type RemoveTagFromDatapointsMutation = { __typename?: 'Mutation', removeTagFromDatapoints: { __typename?: 'ObjectDeleted', message: string } };

export const ProjectFieldsFragmentDoc = gql`
    fragment ProjectFields on Project {
  id
  name
}
    `;
export const JobFieldsFragmentDoc = gql`
    fragment JobFields on Job {
  id
}
    `;
export const ProjectionFieldsFragmentDoc = gql`
    fragment ProjectionFields on Projection {
  id
  x
  y
}
    `;
export const ProjectionSetFieldsFragmentDoc = gql`
    fragment ProjectionSetFields on ProjectionSet {
  id
  projections {
    id
    x
    y
  }
}
    `;
export const EmbeddingFieldsFragmentDoc = gql`
    fragment EmbeddingFields on Embedding {
  id
  data
}
    `;
export const PageInfoFieldsFragmentDoc = gql`
    fragment PageInfoFields on PageInfo {
  hasNextPage
  hasPreviousPage
  startCursor
  endCursor
}
    `;
export const GetProjectsDocument = gql`
    query getProjects {
  projects {
    ...ProjectFields
  }
}
    ${ProjectFieldsFragmentDoc}`;

export function useGetProjectsQuery(options?: Omit<Urql.UseQueryArgs<GetProjectsQueryVariables>, 'query'>) {
  return Urql.useQuery<GetProjectsQuery>({ query: GetProjectsDocument, ...options });
};
export const GetProjectDocument = gql`
    query getProject($id: ID!) {
  project(id: $id) {
    ...ProjectFields
  }
}
    ${ProjectFieldsFragmentDoc}`;

export function useGetProjectQuery(options: Omit<Urql.UseQueryArgs<GetProjectQueryVariables>, 'query'>) {
  return Urql.useQuery<GetProjectQuery>({ query: GetProjectDocument, ...options });
};
export const GetJobsDocument = gql`
    query getJobs {
  jobs {
    ...JobFields
  }
}
    ${JobFieldsFragmentDoc}`;

export function useGetJobsQuery(options?: Omit<Urql.UseQueryArgs<GetJobsQueryVariables>, 'query'>) {
  return Urql.useQuery<GetJobsQuery>({ query: GetJobsDocument, ...options });
};
export const GetJobDocument = gql`
    query getJob($id: ID!) {
  job(id: $id) {
    ...JobFields
  }
}
    ${JobFieldsFragmentDoc}`;

export function useGetJobQuery(options: Omit<Urql.UseQueryArgs<GetJobQueryVariables>, 'query'>) {
  return Urql.useQuery<GetJobQuery>({ query: GetJobDocument, ...options });
};
export const GetProjectionsDocument = gql`
    query getProjections {
  projections {
    ...ProjectionFields
  }
}
    ${ProjectionFieldsFragmentDoc}`;

export function useGetProjectionsQuery(options?: Omit<Urql.UseQueryArgs<GetProjectionsQueryVariables>, 'query'>) {
  return Urql.useQuery<GetProjectionsQuery>({ query: GetProjectionsDocument, ...options });
};
export const GetProjectionDocument = gql`
    query getProjection($id: ID!) {
  projection(id: $id) {
    ...ProjectionFields
  }
}
    ${ProjectionFieldsFragmentDoc}`;

export function useGetProjectionQuery(options: Omit<Urql.UseQueryArgs<GetProjectionQueryVariables>, 'query'>) {
  return Urql.useQuery<GetProjectionQuery>({ query: GetProjectionDocument, ...options });
};
export const AddProjectionDocument = gql`
    mutation AddProjection($projectionInput: ProjectionInput!) {
  addProjection(projectionInput: $projectionInput) {
    __typename
    ... on Projection {
      __typename
      ...ProjectionFields
    }
  }
}
    ${ProjectionFieldsFragmentDoc}`;

export function useAddProjectionMutation() {
  return Urql.useMutation<AddProjectionMutation, AddProjectionMutationVariables>(AddProjectionDocument);
};
export const AddProjectionSetDocument = gql`
    mutation AddProjectionSet($projectionSetInput: ProjectionSetInput!) {
  addProjectionSet(projectionSetInput: $projectionSetInput) {
    __typename
    ... on ProjectionSet {
      __typename
      ...ProjectionSetFields
    }
  }
}
    ${ProjectionSetFieldsFragmentDoc}`;

export function useAddProjectionSetMutation() {
  return Urql.useMutation<AddProjectionSetMutation, AddProjectionSetMutationVariables>(AddProjectionSetDocument);
};
export const GetEmbeddingsDocument = gql`
    query getEmbeddings {
  embeddings {
    ...EmbeddingFields
  }
}
    ${EmbeddingFieldsFragmentDoc}`;

export function useGetEmbeddingsQuery(options?: Omit<Urql.UseQueryArgs<GetEmbeddingsQueryVariables>, 'query'>) {
  return Urql.useQuery<GetEmbeddingsQuery>({ query: GetEmbeddingsDocument, ...options });
};
export const GetEmbeddingDocument = gql`
    query getEmbedding($id: ID!) {
  embedding(id: $id) {
    ...EmbeddingFields
  }
}
    ${EmbeddingFieldsFragmentDoc}`;

export function useGetEmbeddingQuery(options: Omit<Urql.UseQueryArgs<GetEmbeddingQueryVariables>, 'query'>) {
  return Urql.useQuery<GetEmbeddingQuery>({ query: GetEmbeddingDocument, ...options });
};
export const EmbeddingsByPageDocument = gql`
    query embeddingsByPage($pageInput: PageInput!) {
  embeddingsByPage(pageInput: $pageInput) {
    pageInfo {
      ...PageInfoFields
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
    ${PageInfoFieldsFragmentDoc}`;

export function useEmbeddingsByPageQuery(options: Omit<Urql.UseQueryArgs<EmbeddingsByPageQueryVariables>, 'query'>) {
  return Urql.useQuery<EmbeddingsByPageQuery>({ query: EmbeddingsByPageDocument, ...options });
};
export const AppendTagByNameToDatapointsDocument = gql`
    mutation appendTagByNameToDatapoints($tagName: String!, $datapointIds: [Int!], $target: [String!]) {
  appendTagByNameToDatapoints(
    data: {tagName: $tagName, datapointIds: $datapointIds, target: $target}
  ) {
    id
    tags {
      id
      name
    }
  }
}
    `;

export function useAppendTagByNameToDatapointsMutation() {
  return Urql.useMutation<AppendTagByNameToDatapointsMutation, AppendTagByNameToDatapointsMutationVariables>(AppendTagByNameToDatapointsDocument);
};
export const RemoveTagFromDatapointsDocument = gql`
    mutation removeTagFromDatapoints($tagName: String!, $datapointIds: [Int!], $target: [String!]) {
  removeTagFromDatapoints(
    data: {tagName: $tagName, datapointIds: $datapointIds, target: $target}
  ) {
    ... on ObjectDeleted {
      message
    }
  }
}
    `;

export function useRemoveTagFromDatapointsMutation() {
  return Urql.useMutation<RemoveTagFromDatapointsMutation, RemoveTagFromDatapointsMutationVariables>(RemoveTagFromDatapointsDocument);
};