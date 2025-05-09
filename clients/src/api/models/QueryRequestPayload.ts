/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { IncludeList } from './IncludeList';
import type { RawWhereFields } from './RawWhereFields';
export type QueryRequestPayload = (RawWhereFields & {
    ids?: any[] | null;
    include?: IncludeList;
    n_results?: number | null;
    query_embeddings: Array<Array<number>>;
});

