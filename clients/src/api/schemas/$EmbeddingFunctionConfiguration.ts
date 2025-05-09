/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
export const $EmbeddingFunctionConfiguration = {
    type: 'one-of',
    contains: [{
        properties: {
            type: {
                type: 'Enum',
                isRequired: true,
            },
        },
    }, {
        type: 'all-of',
        contains: [{
            type: 'EmbeddingFunctionNewConfiguration',
        }, {
            properties: {
                type: {
                    type: 'Enum',
                    isRequired: true,
                },
            },
        }],
    }],
} as const;
