export declare function validateOffset(dataView: DataView, offset: number, width: number): void;
export declare function readInt(dataView: DataView, offset: number, width: number): number | bigint;
export declare function readUInt(dataView: DataView, offset: number, width: number): number | bigint;
export declare function readFloat(dataView: DataView, offset: number, width: number): number;
export declare function indirect(dataView: DataView, offset: number, width: number): number;
export declare function keyIndex(key: string, dataView: DataView, offset: number, parentWidth: number, byteWidth: number, length: number): number | null;
export declare function diffKeys(input: Uint8Array, index: number, dataView: DataView, offset: number, width: number): number;
export declare function keyForIndex(index: number, dataView: DataView, offset: number, parentWidth: number, byteWidth: number): string;
