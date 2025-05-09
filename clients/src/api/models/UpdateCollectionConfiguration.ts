/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { EmbeddingFunctionConfiguration } from './EmbeddingFunctionConfiguration';
import type { SpannConfiguration } from './SpannConfiguration';
import type { UpdateHnswConfiguration } from './UpdateHnswConfiguration';
export type UpdateCollectionConfiguration = {
    embedding_function?: (null | EmbeddingFunctionConfiguration);
    hnsw?: (null | UpdateHnswConfiguration);
    spann?: (null | SpannConfiguration);
};

