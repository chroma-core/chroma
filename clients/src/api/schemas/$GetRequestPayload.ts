/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
export const $GetRequestPayload = {
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
            limit: {
                type: 'number',
                isNullable: true,
                format: 'int32',
            },
            offset: {
                type: 'number',
                isNullable: true,
                format: 'int32',
            },
        },
    }],
} as const;
