/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { CollectionConfiguration } from './CollectionConfiguration';
import type { HashMap } from './HashMap';
export type CreateCollectionPayload = {
    configuration?: (null | CollectionConfiguration);
    get_or_create?: boolean;
    metadata?: (null | HashMap);
    name: string;
};

