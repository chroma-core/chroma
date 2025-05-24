import { BitWidth } from './bit-width.js';
import { paddingSize, uwidth, fromByteWidth } from './bit-width-util.js';
import { ValueType } from './value-type.js';
import { isInline, packedType } from './value-type-util.js';
export class StackValue {
    constructor(builder, type, width, value = null, offset = 0) {
        this.builder = builder;
        this.type = type;
        this.width = width;
        this.value = value;
        this.offset = offset;
    }
    elementWidth(size, index) {
        if (isInline(this.type))
            return this.width;
        for (let i = 0; i < 4; i++) {
            const width = 1 << i;
            const offsetLoc = size + paddingSize(size, width) + index * width;
            const offset = offsetLoc - this.offset;
            const bitWidth = uwidth(offset);
            if (1 << bitWidth === width) {
                return bitWidth;
            }
        }
        throw `Element is unknown. Size: ${size} at index: ${index}. This might be a bug. Please create an issue https://github.com/google/flatbuffers/issues/new`;
    }
    writeToBuffer(byteWidth) {
        const newOffset = this.builder.computeOffset(byteWidth);
        if (this.type === ValueType.FLOAT) {
            if (this.width === BitWidth.WIDTH32) {
                this.builder.view.setFloat32(this.builder.offset, this.value, true);
            }
            else {
                this.builder.view.setFloat64(this.builder.offset, this.value, true);
            }
        }
        else if (this.type === ValueType.INT) {
            const bitWidth = fromByteWidth(byteWidth);
            this.builder.pushInt(this.value, bitWidth);
        }
        else if (this.type === ValueType.UINT) {
            const bitWidth = fromByteWidth(byteWidth);
            this.builder.pushUInt(this.value, bitWidth);
        }
        else if (this.type === ValueType.NULL) {
            this.builder.pushInt(0, this.width);
        }
        else if (this.type === ValueType.BOOL) {
            this.builder.pushInt(this.value ? 1 : 0, this.width);
        }
        else {
            throw `Unexpected type: ${this.type}. This might be a bug. Please create an issue https://github.com/google/flatbuffers/issues/new`;
        }
        this.offset = newOffset;
    }
    storedWidth(width = BitWidth.WIDTH8) {
        return isInline(this.type) ? Math.max(width, this.width) : this.width;
    }
    storedPackedType(width = BitWidth.WIDTH8) {
        return packedType(this.type, this.storedWidth(width));
    }
    isOffset() {
        return !isInline(this.type);
    }
}
