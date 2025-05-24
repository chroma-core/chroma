export interface FileResponse {
    id: string;
    object: string;
    type: 'jsonl' | 'parquet';
    purpose: 'fine-tune';
    filename: string;
    bytes: number;
    line_count: number;
    processed: boolean;
}
export interface ErrorResponse {
    message: string;
}
export interface CheckFileResponse {
    success: boolean;
    message?: string;
}
export declare function check_file(fileName: string): Promise<CheckFileResponse>;
export declare function check_parquet(fileName: string): Promise<string | undefined>;
export declare function check_jsonl(fileName: string): Promise<string | undefined>;
export declare function upload(fileName: string, check?: boolean): Promise<FileResponse | ErrorResponse>;
//# sourceMappingURL=upload.d.ts.map