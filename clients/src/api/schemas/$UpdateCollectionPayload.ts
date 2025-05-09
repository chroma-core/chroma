/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
export const $UpdateCollectionPayload = {
    properties: {
        new_configuration: {
            type: 'one-of',
            contains: [{
                type: 'null',
            }, {
                type: 'UpdateCollectionConfiguration',
            }],
        },
        new_metadata: {
            type: 'one-of',
            contains: [{
                type: 'null',
            }, {
                type: 'HashMap',
            }],
        },
        new_name: {
            type: 'string',
            isNullable: true,
        },
    },
} as const;
