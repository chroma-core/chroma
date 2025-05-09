/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
export const $CollectionConfiguration = {
    properties: {
        embedding_function: {
            type: 'one-of',
            contains: [{
                type: 'null',
            }, {
                type: 'EmbeddingFunctionConfiguration',
            }],
        },
        hnsw: {
            type: 'one-of',
            contains: [{
                type: 'null',
            }, {
                type: 'HnswConfiguration',
            }],
        },
        spann: {
            type: 'one-of',
            contains: [{
                type: 'null',
            }, {
                type: 'SpannConfiguration',
            }],
        },
    },
} as const;
