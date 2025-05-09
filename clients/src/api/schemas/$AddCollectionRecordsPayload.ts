/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
export const $AddCollectionRecordsPayload = {
    properties: {
        documents: {
            type: 'any[]',
            isNullable: true,
        },
        embeddings: {
            type: 'any[]',
            isNullable: true,
        },
        ids: {
            type: 'array',
            contains: {
                type: 'string',
            },
            isRequired: true,
        },
        metadatas: {
            type: 'any[]',
            isNullable: true,
        },
        uris: {
            type: 'any[]',
            isNullable: true,
        },
    },
} as const;
