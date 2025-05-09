/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { CollectionConfiguration } from './CollectionConfiguration';
import type { CollectionUuid } from './CollectionUuid';
import type { HashMap } from './HashMap';
export type Collection = {
    configuration_json: CollectionConfiguration;
    database: string;
    dimension?: number | null;
    id: CollectionUuid;
    log_position: number;
    metadata?: (null | HashMap);
    name: string;
    tenant: string;
    version: number;
};

