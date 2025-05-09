/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
export const $HashMap = {
    type: 'dictionary',
    contains: {
        type: 'one-of',
        contains: [{
            type: 'boolean',
        }, {
            type: 'number',
            format: 'int64',
        }, {
            type: 'number',
            format: 'double',
        }, {
            type: 'string',
        }],
    },
} as const;
