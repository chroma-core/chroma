/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
export const $QueryRequestPayload = {
    type: 'all-of',
    contains: [{
        type: 'RawWhereFields',
    }, {
        properties: {
            ids: {
                type: 'any[]',
                isNullable: true,
            },
            include: {
                type: 'IncludeList',
            },
            n_results: {
                type: 'number',
                isNullable: true,
                format: 'int32',
            },
            query_embeddings: {
                type: 'array',
                contains: {
                    type: 'array',
                    contains: {
                        type: 'number',
                        format: 'float',
                    },
                },
                isRequired: true,
            },
        },
    }],
} as const;
