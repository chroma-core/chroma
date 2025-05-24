import { BitWidth } from './bit-width.js';
export declare function toByteWidth(bitWidth: BitWidth): number;
export declare function iwidth(value: number | bigint): BitWidth;
export declare function fwidth(value: number): BitWidth;
export declare function uwidth(value: number): BitWidth;
export declare function fromByteWidth(value: number): BitWidth;
export declare function paddingSize(bufSize: number, scalarSize: number): number;
