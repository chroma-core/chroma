/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
export const $HnswConfiguration = {
    properties: {
        ef_construction: {
            type: 'number',
            isNullable: true,
        },
        ef_search: {
            type: 'number',
            isNullable: true,
        },
        max_neighbors: {
            type: 'number',
            isNullable: true,
        },
        resize_factor: {
            type: 'number',
            isNullable: true,
            format: 'double',
        },
        space: {
            type: 'HnswSpace',
        },
        sync_threshold: {
            type: 'number',
            isNullable: true,
        },
    },
} as const;
