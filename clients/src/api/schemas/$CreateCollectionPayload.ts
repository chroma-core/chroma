/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
export const $CreateCollectionPayload = {
    properties: {
        configuration: {
            type: 'one-of',
            contains: [{
                type: 'null',
            }, {
                type: 'CollectionConfiguration',
            }],
        },
        get_or_create: {
            type: 'boolean',
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
    },
} as const;
