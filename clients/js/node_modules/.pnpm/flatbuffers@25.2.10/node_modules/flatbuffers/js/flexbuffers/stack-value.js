"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.StackValue = void 0;
const bit_width_js_1 = require("./bit-width.js");
const bit_width_util_js_1 = require("./bit-width-util.js");
const value_type_js_1 = require("./value-type.js");
const value_type_util_js_1 = require("./value-type-util.js");
class StackValue {
    constructor(builder, type, width, value = null, offset = 0) {
        this.builder = builder;
        this.type = type;
        this.width = width;
        this.value = value;
        this.offset = offset;
    }
    elementWidth(size, index) {
        if ((0, value_type_util_js_1.isInline)(this.type))
            return this.width;
        for (let i = 0; i < 4; i++) {
            const width = 1 << i;
            const offsetLoc = size + (0, bit_width_util_js_1.paddingSize)(size, width) + index * width;
            const offset = offsetLoc - this.offset;
            const bitWidth = (0, bit_width_util_js_1.uwidth)(offset);
            if (1 << bitWidth === width) {
                return bitWidth;
            }
        }
        throw `Element is unknown. Size: ${size} at index: ${index}. This might be a bug. Please create an issue https://github.com/google/flatbuffers/issues/new`;
    }
    writeToBuffer(byteWidth) {
        const newOffset = this.builder.computeOffset(byteWidth);
        if (this.type === value_type_js_1.ValueType.FLOAT) {
            if (this.width === bit_width_js_1.BitWidth.WIDTH32) {
                this.builder.view.setFloat32(this.builder.offset, this.value, true);
            }
            else {
                this.builder.view.setFloat64(this.builder.offset, this.value, true);
            }
        }
        else if (this.type === value_type_js_1.ValueType.INT) {
            const bitWidth = (0, bit_width_util_js_1.fromByteWidth)(byteWidth);
            this.builder.pushInt(this.value, bitWidth);
        }
        else if (this.type === value_type_js_1.ValueType.UINT) {
            const bitWidth = (0, bit_width_util_js_1.fromByteWidth)(byteWidth);
            this.builder.pushUInt(this.value, bitWidth);
        }
        else if (this.type === value_type_js_1.ValueType.NULL) {
            this.builder.pushInt(0, this.width);
        }
        else if (this.type === value_type_js_1.ValueType.BOOL) {
            this.builder.pushInt(this.value ? 1 : 0, this.width);
        }
        else {
            throw `Unexpected type: ${this.type}. This might be a bug. Please create an issue https://github.com/google/flatbuffers/issues/new`;
        }
        this.offset = newOffset;
    }
    storedWidth(width = bit_width_js_1.BitWidth.WIDTH8) {
        return (0, value_type_util_js_1.isInline)(this.type) ? Math.max(width, this.width) : this.width;
    }
    storedPackedType(width = bit_width_js_1.BitWidth.WIDTH8) {
        return (0, value_type_util_js_1.packedType)(this.type, this.storedWidth(width));
    }
    isOffset() {
        return !(0, value_type_util_js_1.isInline)(this.type);
    }
}
exports.StackValue = StackValue;
