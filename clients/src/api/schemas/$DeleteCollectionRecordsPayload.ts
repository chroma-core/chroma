/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
export const $DeleteCollectionRecordsPayload = {
    type: 'all-of',
    contains: [{
        type: 'RawWhereFields',
    }, {
        properties: {
            ids: {
                type: 'any[]',
                isNullable: true,
            },
        },
    }],
} as const;
