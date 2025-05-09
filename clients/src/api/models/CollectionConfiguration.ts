/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { EmbeddingFunctionConfiguration } from './EmbeddingFunctionConfiguration';
import type { HnswConfiguration } from './HnswConfiguration';
import type { SpannConfiguration } from './SpannConfiguration';
export type CollectionConfiguration = {
    embedding_function?: (null | EmbeddingFunctionConfiguration);
    hnsw?: (null | HnswConfiguration);
    spann?: (null | SpannConfiguration);
};

