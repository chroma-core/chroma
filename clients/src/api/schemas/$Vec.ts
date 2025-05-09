/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
export const $Vec = {
    type: 'array',
    contains: {
        properties: {
            configuration_json: {
                type: 'CollectionConfiguration',
                isRequired: true,
            },
            database: {
                type: 'string',
                isRequired: true,
            },
            dimension: {
                type: 'number',
                isNullable: true,
                format: 'int32',
            },
            id: {
                type: 'CollectionUuid',
                isRequired: true,
            },
            log_position: {
                type: 'number',
                isRequired: true,
                format: 'int64',
            },
            metadata: {
                type: 'one-of',
                contains: [{
                    type: 'null',
                }, {
                    type: 'HashMap',
                }],
            },
            name: {
                type: 'string',
                isRequired: true,
            },
            tenant: {
                type: 'string',
                isRequired: true,
            },
            version: {
                type: 'number',
                isRequired: true,
                format: 'int32',
            },
        },
    },
} as const;
