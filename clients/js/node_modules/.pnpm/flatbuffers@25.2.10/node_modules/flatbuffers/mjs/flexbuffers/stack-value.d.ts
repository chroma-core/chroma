import { Builder } from './builder.js';
import { BitWidth } from './bit-width.js';
import { ValueType } from './value-type.js';
export declare class StackValue {
    private builder;
    type: ValueType;
    width: number;
    value: number | boolean | null;
    offset: number;
    constructor(builder: Builder, type: ValueType, width: number, value?: number | boolean | null, offset?: number);
    elementWidth(size: number, index: number): BitWidth;
    writeToBuffer(byteWidth: number): void;
    storedWidth(width?: BitWidth): BitWidth;
    storedPackedType(width?: BitWidth): ValueType;
    isOffset(): boolean;
}
