import { APIResource } from "../resource.js";
import * as Core from "../core.js";
import { type Response } from "../_shims/index.js";
export declare class Files extends APIResource {
    /**
     * List the metadata for a single uploaded data file.
     */
    retrieve(id: string, options?: Core.RequestOptions): Core.APIPromise<FileRetrieveResponse>;
    /**
     * List the metadata for all uploaded data files.
     */
    list(options?: Core.RequestOptions): Core.APIPromise<FileListResponse>;
    /**
     * Delete a previously uploaded data file.
     */
    delete(id: string, options?: Core.RequestOptions): Core.APIPromise<FileDeleteResponse>;
    /**
     * Get the contents of a single uploaded data file.
     */
    content(id: string, options?: Core.RequestOptions): Core.APIPromise<Response>;
    /**
     * Upload a file.
     */
    upload(_: string): Promise<void>;
}
export interface FileObject {
    id?: string;
    filename?: string;
    object?: string;
    size?: number;
}
export interface FileRetrieveResponse {
    id: string;
    bytes: number;
    created_at: number;
    filename: string;
    FileType: 'jsonl' | 'parquet';
    LineCount: number;
    object: string;
    Processed: boolean;
    purpose: 'fine-tune';
}
export interface FileListResponse {
    data: Array<FileListResponse.Data>;
}
export declare namespace FileListResponse {
    interface Data {
        id: string;
        bytes: number;
        created_at: number;
        filename: string;
        FileType: 'jsonl' | 'parquet';
        LineCount: number;
        object: string;
        Processed: boolean;
        purpose: 'fine-tune';
    }
}
export interface FileDeleteResponse {
    id?: string;
    deleted?: boolean;
}
export declare namespace Files {
    export { type FileObject as FileObject, type FileRetrieveResponse as FileRetrieveResponse, type FileListResponse as FileListResponse, type FileDeleteResponse as FileDeleteResponse, };
}
//# sourceMappingURL=files.d.ts.map