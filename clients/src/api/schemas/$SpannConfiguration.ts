/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
export const $SpannConfiguration = {
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
        merge_threshold: {
            type: 'number',
            isNullable: true,
            format: 'int32',
        },
        reassign_neighbor_count: {
            type: 'number',
            isNullable: true,
            format: 'int32',
        },
        search_nprobe: {
            type: 'number',
            isNullable: true,
            format: 'int32',
        },
        space: {
            type: 'HnswSpace',
        },
        split_threshold: {
            type: 'number',
            isNullable: true,
            format: 'int32',
        },
        write_nprobe: {
            type: 'number',
            isNullable: true,
            format: 'int32',
        },
    },
} as const;
