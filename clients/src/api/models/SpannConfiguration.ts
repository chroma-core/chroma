/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { HnswSpace } from './HnswSpace';
export type SpannConfiguration = {
    ef_construction?: number | null;
    ef_search?: number | null;
    max_neighbors?: number | null;
    merge_threshold?: number | null;
    reassign_neighbor_count?: number | null;
    search_nprobe?: number | null;
    space?: HnswSpace;
    split_threshold?: number | null;
    write_nprobe?: number | null;
};

