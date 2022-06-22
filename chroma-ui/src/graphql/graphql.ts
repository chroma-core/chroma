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
};

export type AddEmbeddingResponse = Embedding | EmbeddingExists;

export type Embedding = {
  __typename?: 'Embedding';
  data?: Maybe<Scalars['String']>;
  embeddingSet?: Maybe<EmbeddingSet>;
  id: Scalars['ID'];
  inferenceIdentifier: Scalars['String'];
  inputIdentifier: Scalars['String'];
  label?: Maybe<Scalars['String']>;
  projections: Array<Projection>;
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

export type EmbeddingExists = {
  __typename?: 'EmbeddingExists';
  message: Scalars['String'];
};

export type EmbeddingInput = {
  data: Scalars['String'];
  embeddingSetId: Scalars['Int'];
  inferenceIdentifier: Scalars['String'];
  inputIdentifier: Scalars['String'];
  label: Scalars['String'];
};

export type EmbeddingSet = {
  __typename?: 'EmbeddingSet';
  embeddings: Array<Embedding>;
  id: Scalars['ID'];
  projectionSets: Array<ProjectionSet>;
};

export type EmbeddingsInput = {
  embeddings: Array<EmbeddingInput>;
};

export type Mutation = {
  __typename?: 'Mutation';
  addEmbedding: AddEmbeddingResponse;
  addEmbeddingSet: EmbeddingSet;
  addEmbeddings: Array<Embedding>;
  addProjection: Projection;
  addProjectionSet: ProjectionSet;
};


export type MutationAddEmbeddingArgs = {
  embeddingInput: EmbeddingInput;
};


export type MutationAddEmbeddingsArgs = {
  embeddingsInput: EmbeddingsInput;
};


export type MutationAddProjectionArgs = {
  projectionInput: ProjectionInput;
};


export type MutationAddProjectionSetArgs = {
  projectionSetInput: ProjectionSetInput;
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

export type Projection = {
  __typename?: 'Projection';
  embedding?: Maybe<Embedding>;
  id: Scalars['ID'];
  projectionSet?: Maybe<ProjectionSet>;
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
  embeddingSet?: Maybe<EmbeddingSet>;
  id: Scalars['ID'];
  projections: Array<Projection>;
};

export type ProjectionSetInput = {
  projectionSetId: Scalars['Int'];
};

export type Query = {
  __typename?: 'Query';
  embedding: Embedding;
  embeddingSet: EmbeddingSet;
  embeddingSets: Array<EmbeddingSet>;
  embeddings: Array<Embedding>;
  embeddingsByPage: EmbeddingConnection;
  projection: Projection;
  projectionSet: ProjectionSet;
  projectionSets: Array<ProjectionSet>;
  projections: Array<Projection>;
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


export type QueryProjectionArgs = {
  id: Scalars['ID'];
};


export type QueryProjectionSetArgs = {
  id: Scalars['ID'];
};

export type Subscription = {
  __typename?: 'Subscription';
  count: Scalars['Int'];
};


export type SubscriptionCountArgs = {
  target?: Scalars['Int'];
};

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

export type GetProjectionSetsQueryVariables = Exact<{ [key: string]: never; }>;


export type GetProjectionSetsQuery = { __typename?: 'Query', projectionSets: Array<{ __typename?: 'ProjectionSet', id: string, projections: Array<{ __typename?: 'Projection', id: string, x: number, y: number }> }> };

export type GetProjectionSetQueryVariables = Exact<{
  id: Scalars['ID'];
}>;


export type GetProjectionSetQuery = { __typename?: 'Query', projectionSet: { __typename?: 'ProjectionSet', id: string, projections: Array<{ __typename?: 'Projection', id: string, x: number, y: number }> } };

export type AddProjectionSetMutationVariables = Exact<{
  projectionSetInput: ProjectionSetInput;
}>;


export type AddProjectionSetMutation = { __typename?: 'Mutation', addProjectionSet: { __typename: 'ProjectionSet', id: string, projections: Array<{ __typename?: 'Projection', id: string, x: number, y: number }> } };

export type EmbeddingFieldsFragment = { __typename?: 'Embedding', id: string, label?: string | null, inputIdentifier: string, inferenceIdentifier: string };

export type GetEmbeddingsQueryVariables = Exact<{ [key: string]: never; }>;


export type GetEmbeddingsQuery = { __typename?: 'Query', embeddings: Array<{ __typename?: 'Embedding', id: string, label?: string | null, inputIdentifier: string, inferenceIdentifier: string }> };

export type GetEmbeddingQueryVariables = Exact<{
  id: Scalars['ID'];
}>;


export type GetEmbeddingQuery = { __typename?: 'Query', embedding: { __typename?: 'Embedding', id: string, label?: string | null, inputIdentifier: string, inferenceIdentifier: string } };

export type PageInfoFieldsFragment = { __typename?: 'PageInfo', hasNextPage: boolean, hasPreviousPage: boolean, startCursor?: string | null, endCursor?: string | null };

export type EmbeddingsByPageQueryVariables = Exact<{
  pageInput: PageInput;
}>;


export type EmbeddingsByPageQuery = { __typename?: 'Query', embeddingsByPage: { __typename?: 'EmbeddingConnection', pageInfo: { __typename?: 'PageInfo', hasNextPage: boolean, hasPreviousPage: boolean, startCursor?: string | null, endCursor?: string | null }, edges: Array<{ __typename?: 'EmbeddingEdge', cursor: string, node: { __typename?: 'Embedding', id: string, data?: string | null } }> } };

export type AddEmbeddingMutationVariables = Exact<{
  embeddingInput: EmbeddingInput;
}>;


export type AddEmbeddingMutation = { __typename?: 'Mutation', addEmbedding: { __typename: 'Embedding', id: string, label?: string | null, inputIdentifier: string, inferenceIdentifier: string } | { __typename: 'EmbeddingExists', message: string } };

export type AddEmbeddingsMutationVariables = Exact<{
  embeddingsInput: EmbeddingsInput;
}>;


export type AddEmbeddingsMutation = { __typename?: 'Mutation', addEmbeddings: Array<{ __typename?: 'Embedding', id: string, data?: string | null, embeddingSet?: { __typename?: 'EmbeddingSet', id: string } | null }> };

export type EmbeddingSetFieldsFragment = { __typename?: 'EmbeddingSet', id: string };

export type GetEmbeddingSetsQueryVariables = Exact<{ [key: string]: never; }>;


export type GetEmbeddingSetsQuery = { __typename?: 'Query', embeddingSets: Array<{ __typename?: 'EmbeddingSet', id: string }> };

export type GetEmbeddingSetQueryVariables = Exact<{
  id: Scalars['ID'];
}>;


export type GetEmbeddingSetQuery = { __typename?: 'Query', embeddingSet: { __typename?: 'EmbeddingSet', id: string } };

export type AddEmbeddingSetMutationVariables = Exact<{ [key: string]: never; }>;


export type AddEmbeddingSetMutation = { __typename?: 'Mutation', addEmbeddingSet: { __typename: 'EmbeddingSet', id: string } };

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
  label
  inputIdentifier
  inferenceIdentifier
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
export const EmbeddingSetFieldsFragmentDoc = gql`
    fragment EmbeddingSetFields on EmbeddingSet {
  id
}
    `;
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
export const GetProjectionSetsDocument = gql`
    query getProjectionSets {
  projectionSets {
    ...ProjectionSetFields
  }
}
    ${ProjectionSetFieldsFragmentDoc}`;

export function useGetProjectionSetsQuery(options?: Omit<Urql.UseQueryArgs<GetProjectionSetsQueryVariables>, 'query'>) {
  return Urql.useQuery<GetProjectionSetsQuery>({ query: GetProjectionSetsDocument, ...options });
};
export const GetProjectionSetDocument = gql`
    query getProjectionSet($id: ID!) {
  projectionSet(id: $id) {
    ...ProjectionSetFields
  }
}
    ${ProjectionSetFieldsFragmentDoc}`;

export function useGetProjectionSetQuery(options: Omit<Urql.UseQueryArgs<GetProjectionSetQueryVariables>, 'query'>) {
  return Urql.useQuery<GetProjectionSetQuery>({ query: GetProjectionSetDocument, ...options });
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
export const AddEmbeddingDocument = gql`
    mutation AddEmbedding($embeddingInput: EmbeddingInput!) {
  addEmbedding(embeddingInput: $embeddingInput) {
    __typename
    ... on EmbeddingExists {
      __typename
      message
    }
    ... on Embedding {
      __typename
      ...EmbeddingFields
    }
  }
}
    ${EmbeddingFieldsFragmentDoc}`;

export function useAddEmbeddingMutation() {
  return Urql.useMutation<AddEmbeddingMutation, AddEmbeddingMutationVariables>(AddEmbeddingDocument);
};
export const AddEmbeddingsDocument = gql`
    mutation AddEmbeddings($embeddingsInput: EmbeddingsInput!) {
  addEmbeddings(embeddingsInput: $embeddingsInput) {
    id
    data
    embeddingSet {
      id
    }
  }
}
    `;

export function useAddEmbeddingsMutation() {
  return Urql.useMutation<AddEmbeddingsMutation, AddEmbeddingsMutationVariables>(AddEmbeddingsDocument);
};
export const GetEmbeddingSetsDocument = gql`
    query getEmbeddingSets {
  embeddingSets {
    ...EmbeddingSetFields
  }
}
    ${EmbeddingSetFieldsFragmentDoc}`;

export function useGetEmbeddingSetsQuery(options?: Omit<Urql.UseQueryArgs<GetEmbeddingSetsQueryVariables>, 'query'>) {
  return Urql.useQuery<GetEmbeddingSetsQuery>({ query: GetEmbeddingSetsDocument, ...options });
};
export const GetEmbeddingSetDocument = gql`
    query getEmbeddingSet($id: ID!) {
  embeddingSet(id: $id) {
    ...EmbeddingSetFields
  }
}
    ${EmbeddingSetFieldsFragmentDoc}`;

export function useGetEmbeddingSetQuery(options: Omit<Urql.UseQueryArgs<GetEmbeddingSetQueryVariables>, 'query'>) {
  return Urql.useQuery<GetEmbeddingSetQuery>({ query: GetEmbeddingSetDocument, ...options });
};
export const AddEmbeddingSetDocument = gql`
    mutation AddEmbeddingSet {
  addEmbeddingSet {
    __typename
    ... on EmbeddingSet {
      __typename
      ...EmbeddingSetFields
    }
  }
}
    ${EmbeddingSetFieldsFragmentDoc}`;

export function useAddEmbeddingSetMutation() {
  return Urql.useMutation<AddEmbeddingSetMutation, AddEmbeddingSetMutationVariables>(AddEmbeddingSetDocument);
};