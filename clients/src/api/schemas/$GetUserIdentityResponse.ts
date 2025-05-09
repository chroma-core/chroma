/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
export const $GetUserIdentityResponse = {
    properties: {
        databases: {
            type: 'array',
            contains: {
                type: 'string',
            },
            isRequired: true,
        },
        tenant: {
            type: 'string',
            isRequired: true,
        },
        user_id: {
            type: 'string',
            isRequired: true,
        },
    },
} as const;
