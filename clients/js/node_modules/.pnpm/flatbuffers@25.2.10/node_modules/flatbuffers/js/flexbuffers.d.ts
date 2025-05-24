import { Builder } from './flexbuffers/builder.js';
export { toReference } from './flexbuffers/reference.js';
export declare function builder(): Builder;
export declare function toObject(buffer: ArrayBuffer): unknown;
export declare function encode(object: unknown, size?: number, deduplicateStrings?: boolean, deduplicateKeys?: boolean, deduplicateKeyVectors?: boolean): Uint8Array;
