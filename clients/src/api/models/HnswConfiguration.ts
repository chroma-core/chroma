/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { HnswSpace } from './HnswSpace';
export type HnswConfiguration = {
    ef_construction?: number | null;
    ef_search?: number | null;
    max_neighbors?: number | null;
    resize_factor?: number | null;
    space?: HnswSpace;
    sync_threshold?: number | null;
};

