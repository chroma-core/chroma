import { BitWidth } from './bit-width.js';
import { paddingSize, iwidth, uwidth, fwidth, toByteWidth, fromByteWidth } from './bit-width-util.js';
import { toUTF8Array } from './flexbuffers-util.js';
import { ValueType } from './value-type.js';
import { isNumber, isTypedVectorElement, toTypedVector } from './value-type-util.js';
import { StackValue } from './stack-value.js';
export class Builder {
    constructor(size = 2048, dedupStrings = true, dedupKeys = true, dedupKeyVectors = true) {
        this.dedupStrings = dedupStrings;
        this.dedupKeys = dedupKeys;
        this.dedupKeyVectors = dedupKeyVectors;
        this.stack = [];
        this.stackPointers = [];
        this.offset = 0;
        this.finished = false;
        this.stringLookup = {};
        this.keyLookup = {};
        this.keyVectorLookup = {};
        this.indirectIntLookup = {};
        this.indirectUIntLookup = {};
        this.indirectFloatLookup = {};
        this.buffer = new ArrayBuffer(size > 0 ? size : 2048);
        this.view = new DataView(this.buffer);
    }
    align(width) {
        const byteWidth = toByteWidth(width);
        this.offset += paddingSize(this.offset, byteWidth);
        return byteWidth;
    }
    computeOffset(newValueSize) {
        const targetOffset = this.offset + newValueSize;
        let size = this.buffer.byteLength;
        const prevSize = size;
        while (size < targetOffset) {
            size <<= 1;
        }
        if (prevSize < size) {
            const prevBuffer = this.buffer;
            this.buffer = new ArrayBuffer(size);
            this.view = new DataView(this.buffer);
            new Uint8Array(this.buffer).set(new Uint8Array(prevBuffer), 0);
        }
        return targetOffset;
    }
    pushInt(value, width) {
        if (width === BitWidth.WIDTH8) {
            this.view.setInt8(this.offset, value);
        }
        else if (width === BitWidth.WIDTH16) {
            this.view.setInt16(this.offset, value, true);
        }
        else if (width === BitWidth.WIDTH32) {
            this.view.setInt32(this.offset, value, true);
        }
        else if (width === BitWidth.WIDTH64) {
            this.view.setBigInt64(this.offset, BigInt(value), true);
        }
        else {
            throw `Unexpected width: ${width} for value: ${value}`;
        }
    }
    pushUInt(value, width) {
        if (width === BitWidth.WIDTH8) {
            this.view.setUint8(this.offset, value);
        }
        else if (width === BitWidth.WIDTH16) {
            this.view.setUint16(this.offset, value, true);
        }
        else if (width === BitWidth.WIDTH32) {
            this.view.setUint32(this.offset, value, true);
        }
        else if (width === BitWidth.WIDTH64) {
            this.view.setBigUint64(this.offset, BigInt(value), true);
        }
        else {
            throw `Unexpected width: ${width} for value: ${value}`;
        }
    }
    writeInt(value, byteWidth) {
        const newOffset = this.computeOffset(byteWidth);
        this.pushInt(value, fromByteWidth(byteWidth));
        this.offset = newOffset;
    }
    writeUInt(value, byteWidth) {
        const newOffset = this.computeOffset(byteWidth);
        this.pushUInt(value, fromByteWidth(byteWidth));
        this.offset = newOffset;
    }
    writeBlob(arrayBuffer) {
        const length = arrayBuffer.byteLength;
        const bitWidth = uwidth(length);
        const byteWidth = this.align(bitWidth);
        this.writeUInt(length, byteWidth);
        const blobOffset = this.offset;
        const newOffset = this.computeOffset(length);
        new Uint8Array(this.buffer).set(new Uint8Array(arrayBuffer), blobOffset);
        this.stack.push(this.offsetStackValue(blobOffset, ValueType.BLOB, bitWidth));
        this.offset = newOffset;
    }
    writeString(str) {
        if (this.dedupStrings && Object.prototype.hasOwnProperty.call(this.stringLookup, str)) {
            this.stack.push(this.stringLookup[str]);
            return;
        }
        const utf8 = toUTF8Array(str);
        const length = utf8.length;
        const bitWidth = uwidth(length);
        const byteWidth = this.align(bitWidth);
        this.writeUInt(length, byteWidth);
        const stringOffset = this.offset;
        const newOffset = this.computeOffset(length + 1);
        new Uint8Array(this.buffer).set(utf8, stringOffset);
        const stackValue = this.offsetStackValue(stringOffset, ValueType.STRING, bitWidth);
        this.stack.push(stackValue);
        if (this.dedupStrings) {
            this.stringLookup[str] = stackValue;
        }
        this.offset = newOffset;
    }
    writeKey(str) {
        if (this.dedupKeys && Object.prototype.hasOwnProperty.call(this.keyLookup, str)) {
            this.stack.push(this.keyLookup[str]);
            return;
        }
        const utf8 = toUTF8Array(str);
        const length = utf8.length;
        const newOffset = this.computeOffset(length + 1);
        new Uint8Array(this.buffer).set(utf8, this.offset);
        const stackValue = this.offsetStackValue(this.offset, ValueType.KEY, BitWidth.WIDTH8);
        this.stack.push(stackValue);
        if (this.dedupKeys) {
            this.keyLookup[str] = stackValue;
        }
        this.offset = newOffset;
    }
    writeStackValue(value, byteWidth) {
        const newOffset = this.computeOffset(byteWidth);
        if (value.isOffset()) {
            const relativeOffset = this.offset - value.offset;
            if (byteWidth === 8 || BigInt(relativeOffset) < (BigInt(1) << BigInt(byteWidth * 8))) {
                this.writeUInt(relativeOffset, byteWidth);
            }
            else {
                throw `Unexpected size ${byteWidth}. This might be a bug. Please create an issue https://github.com/google/flatbuffers/issues/new`;
            }
        }
        else {
            value.writeToBuffer(byteWidth);
        }
        this.offset = newOffset;
    }
    integrityCheckOnValueAddition() {
        if (this.finished) {
            throw "Adding values after finish is prohibited";
        }
        if (this.stackPointers.length !== 0 && this.stackPointers[this.stackPointers.length - 1].isVector === false) {
            if (this.stack[this.stack.length - 1].type !== ValueType.KEY) {
                throw "Adding value to a map before adding a key is prohibited";
            }
        }
    }
    integrityCheckOnKeyAddition() {
        if (this.finished) {
            throw "Adding values after finish is prohibited";
        }
        if (this.stackPointers.length === 0 || this.stackPointers[this.stackPointers.length - 1].isVector) {
            throw "Adding key before starting a map is prohibited";
        }
    }
    startVector() {
        this.stackPointers.push({ stackPosition: this.stack.length, isVector: true });
    }
    startMap(presorted = false) {
        this.stackPointers.push({ stackPosition: this.stack.length, isVector: false, presorted: presorted });
    }
    endVector(stackPointer) {
        const vecLength = this.stack.length - stackPointer.stackPosition;
        const vec = this.createVector(stackPointer.stackPosition, vecLength, 1);
        this.stack.splice(stackPointer.stackPosition, vecLength);
        this.stack.push(vec);
    }
    endMap(stackPointer) {
        if (!stackPointer.presorted) {
            this.sort(stackPointer);
        }
        let keyVectorHash = "";
        for (let i = stackPointer.stackPosition; i < this.stack.length; i += 2) {
            keyVectorHash += `,${this.stack[i].offset}`;
        }
        const vecLength = (this.stack.length - stackPointer.stackPosition) >> 1;
        if (this.dedupKeyVectors && !Object.prototype.hasOwnProperty.call(this.keyVectorLookup, keyVectorHash)) {
            this.keyVectorLookup[keyVectorHash] = this.createVector(stackPointer.stackPosition, vecLength, 2);
        }
        const keysStackValue = this.dedupKeyVectors ? this.keyVectorLookup[keyVectorHash] : this.createVector(stackPointer.stackPosition, vecLength, 2);
        const valuesStackValue = this.createVector(stackPointer.stackPosition + 1, vecLength, 2, keysStackValue);
        this.stack.splice(stackPointer.stackPosition, vecLength << 1);
        this.stack.push(valuesStackValue);
    }
    sort(stackPointer) {
        const view = this.view;
        const stack = this.stack;
        function shouldFlip(v1, v2) {
            if (v1.type !== ValueType.KEY || v2.type !== ValueType.KEY) {
                throw `Stack values are not keys ${v1} | ${v2}. Check if you combined [addKey] with add... method calls properly.`;
            }
            let c1, c2;
            let index = 0;
            do {
                c1 = view.getUint8(v1.offset + index);
                c2 = view.getUint8(v2.offset + index);
                if (c2 < c1)
                    return true;
                if (c1 < c2)
                    return false;
                index += 1;
            } while (c1 !== 0 && c2 !== 0);
            return false;
        }
        function swap(stack, flipIndex, i) {
            if (flipIndex === i)
                return;
            const k = stack[flipIndex];
            const v = stack[flipIndex + 1];
            stack[flipIndex] = stack[i];
            stack[flipIndex + 1] = stack[i + 1];
            stack[i] = k;
            stack[i + 1] = v;
        }
        function selectionSort() {
            for (let i = stackPointer.stackPosition; i < stack.length; i += 2) {
                let flipIndex = i;
                for (let j = i + 2; j < stack.length; j += 2) {
                    if (shouldFlip(stack[flipIndex], stack[j])) {
                        flipIndex = j;
                    }
                }
                if (flipIndex !== i) {
                    swap(stack, flipIndex, i);
                }
            }
        }
        function smaller(v1, v2) {
            if (v1.type !== ValueType.KEY || v2.type !== ValueType.KEY) {
                throw `Stack values are not keys ${v1} | ${v2}. Check if you combined [addKey] with add... method calls properly.`;
            }
            if (v1.offset === v2.offset) {
                return false;
            }
            let c1, c2;
            let index = 0;
            do {
                c1 = view.getUint8(v1.offset + index);
                c2 = view.getUint8(v2.offset + index);
                if (c1 < c2)
                    return true;
                if (c2 < c1)
                    return false;
                index += 1;
            } while (c1 !== 0 && c2 !== 0);
            return false;
        }
        function quickSort(left, right) {
            if (left < right) {
                const mid = left + (((right - left) >> 2)) * 2;
                const pivot = stack[mid];
                let left_new = left;
                let right_new = right;
                do {
                    while (smaller(stack[left_new], pivot)) {
                        left_new += 2;
                    }
                    while (smaller(pivot, stack[right_new])) {
                        right_new -= 2;
                    }
                    if (left_new <= right_new) {
                        swap(stack, left_new, right_new);
                        left_new += 2;
                        right_new -= 2;
                    }
                } while (left_new <= right_new);
                quickSort(left, right_new);
                quickSort(left_new, right);
            }
        }
        let sorted = true;
        for (let i = stackPointer.stackPosition; i < this.stack.length - 2; i += 2) {
            if (shouldFlip(this.stack[i], this.stack[i + 2])) {
                sorted = false;
                break;
            }
        }
        if (!sorted) {
            if (this.stack.length - stackPointer.stackPosition > 40) {
                quickSort(stackPointer.stackPosition, this.stack.length - 2);
            }
            else {
                selectionSort();
            }
        }
    }
    end() {
        if (this.stackPointers.length < 1)
            return;
        const pointer = this.stackPointers.pop();
        if (pointer.isVector) {
            this.endVector(pointer);
        }
        else {
            this.endMap(pointer);
        }
    }
    createVector(start, vecLength, step, keys = null) {
        let bitWidth = uwidth(vecLength);
        let prefixElements = 1;
        if (keys !== null) {
            const elementWidth = keys.elementWidth(this.offset, 0);
            if (elementWidth > bitWidth) {
                bitWidth = elementWidth;
            }
            prefixElements += 2;
        }
        let vectorType = ValueType.KEY;
        let typed = keys === null;
        for (let i = start; i < this.stack.length; i += step) {
            const elementWidth = this.stack[i].elementWidth(this.offset, i + prefixElements);
            if (elementWidth > bitWidth) {
                bitWidth = elementWidth;
            }
            if (i === start) {
                vectorType = this.stack[i].type;
                typed = typed && isTypedVectorElement(vectorType);
            }
            else {
                if (vectorType !== this.stack[i].type) {
                    typed = false;
                }
            }
        }
        const byteWidth = this.align(bitWidth);
        const fix = typed && isNumber(vectorType) && vecLength >= 2 && vecLength <= 4;
        if (keys !== null) {
            this.writeStackValue(keys, byteWidth);
            this.writeUInt(1 << keys.width, byteWidth);
        }
        if (!fix) {
            this.writeUInt(vecLength, byteWidth);
        }
        const vecOffset = this.offset;
        for (let i = start; i < this.stack.length; i += step) {
            this.writeStackValue(this.stack[i], byteWidth);
        }
        if (!typed) {
            for (let i = start; i < this.stack.length; i += step) {
                this.writeUInt(this.stack[i].storedPackedType(), 1);
            }
        }
        if (keys !== null) {
            return this.offsetStackValue(vecOffset, ValueType.MAP, bitWidth);
        }
        if (typed) {
            const vType = toTypedVector(vectorType, fix ? vecLength : 0);
            return this.offsetStackValue(vecOffset, vType, bitWidth);
        }
        return this.offsetStackValue(vecOffset, ValueType.VECTOR, bitWidth);
    }
    nullStackValue() {
        return new StackValue(this, ValueType.NULL, BitWidth.WIDTH8);
    }
    boolStackValue(value) {
        return new StackValue(this, ValueType.BOOL, BitWidth.WIDTH8, value);
    }
    intStackValue(value) {
        return new StackValue(this, ValueType.INT, iwidth(value), value);
    }
    uintStackValue(value) {
        return new StackValue(this, ValueType.UINT, uwidth(value), value);
    }
    floatStackValue(value) {
        return new StackValue(this, ValueType.FLOAT, fwidth(value), value);
    }
    offsetStackValue(offset, valueType, bitWidth) {
        return new StackValue(this, valueType, bitWidth, null, offset);
    }
    finishBuffer() {
        if (this.stack.length !== 1) {
            throw `Stack has to be exactly 1, but it is ${this.stack.length}. You have to end all started vectors and maps before calling [finish]`;
        }
        const value = this.stack[0];
        const byteWidth = this.align(value.elementWidth(this.offset, 0));
        this.writeStackValue(value, byteWidth);
        this.writeUInt(value.storedPackedType(), 1);
        this.writeUInt(byteWidth, 1);
        this.finished = true;
    }
    add(value) {
        this.integrityCheckOnValueAddition();
        if (typeof value === 'undefined') {
            throw "You need to provide a value";
        }
        if (value === null) {
            this.stack.push(this.nullStackValue());
        }
        else if (typeof value === "boolean") {
            this.stack.push(this.boolStackValue(value));
        }
        else if (typeof value === "bigint") {
            this.stack.push(this.intStackValue(value));
        }
        else if (typeof value == 'number') {
            if (Number.isInteger(value)) {
                this.stack.push(this.intStackValue(value));
            }
            else {
                this.stack.push(this.floatStackValue(value));
            }
        }
        else if (ArrayBuffer.isView(value)) {
            this.writeBlob(value.buffer);
        }
        else if (typeof value === 'string' || value instanceof String) {
            this.writeString(value);
        }
        else if (Array.isArray(value)) {
            this.startVector();
            for (let i = 0; i < value.length; i++) {
                this.add(value[i]);
            }
            this.end();
        }
        else if (typeof value === 'object') {
            const properties = Object.getOwnPropertyNames(value).sort();
            this.startMap(true);
            for (let i = 0; i < properties.length; i++) {
                const key = properties[i];
                this.addKey(key);
                this.add(value[key]);
            }
            this.end();
        }
        else {
            throw `Unexpected value input ${value}`;
        }
    }
    finish() {
        if (!this.finished) {
            this.finishBuffer();
        }
        const result = this.buffer.slice(0, this.offset);
        return new Uint8Array(result);
    }
    isFinished() {
        return this.finished;
    }
    addKey(key) {
        this.integrityCheckOnKeyAddition();
        this.writeKey(key);
    }
    addInt(value, indirect = false, deduplicate = false) {
        this.integrityCheckOnValueAddition();
        if (!indirect) {
            this.stack.push(this.intStackValue(value));
            return;
        }
        if (deduplicate && Object.prototype.hasOwnProperty.call(this.indirectIntLookup, value)) {
            this.stack.push(this.indirectIntLookup[value]);
            return;
        }
        const stackValue = this.intStackValue(value);
        const byteWidth = this.align(stackValue.width);
        const newOffset = this.computeOffset(byteWidth);
        const valueOffset = this.offset;
        stackValue.writeToBuffer(byteWidth);
        const stackOffset = this.offsetStackValue(valueOffset, ValueType.INDIRECT_INT, stackValue.width);
        this.stack.push(stackOffset);
        this.offset = newOffset;
        if (deduplicate) {
            this.indirectIntLookup[value] = stackOffset;
        }
    }
    addUInt(value, indirect = false, deduplicate = false) {
        this.integrityCheckOnValueAddition();
        if (!indirect) {
            this.stack.push(this.uintStackValue(value));
            return;
        }
        if (deduplicate && Object.prototype.hasOwnProperty.call(this.indirectUIntLookup, value)) {
            this.stack.push(this.indirectUIntLookup[value]);
            return;
        }
        const stackValue = this.uintStackValue(value);
        const byteWidth = this.align(stackValue.width);
        const newOffset = this.computeOffset(byteWidth);
        const valueOffset = this.offset;
        stackValue.writeToBuffer(byteWidth);
        const stackOffset = this.offsetStackValue(valueOffset, ValueType.INDIRECT_UINT, stackValue.width);
        this.stack.push(stackOffset);
        this.offset = newOffset;
        if (deduplicate) {
            this.indirectUIntLookup[value] = stackOffset;
        }
    }
    addFloat(value, indirect = false, deduplicate = false) {
        this.integrityCheckOnValueAddition();
        if (!indirect) {
            this.stack.push(this.floatStackValue(value));
            return;
        }
        if (deduplicate && Object.prototype.hasOwnProperty.call(this.indirectFloatLookup, value)) {
            this.stack.push(this.indirectFloatLookup[value]);
            return;
        }
        const stackValue = this.floatStackValue(value);
        const byteWidth = this.align(stackValue.width);
        const newOffset = this.computeOffset(byteWidth);
        const valueOffset = this.offset;
        stackValue.writeToBuffer(byteWidth);
        const stackOffset = this.offsetStackValue(valueOffset, ValueType.INDIRECT_FLOAT, stackValue.width);
        this.stack.push(stackOffset);
        this.offset = newOffset;
        if (deduplicate) {
            this.indirectFloatLookup[value] = stackOffset;
        }
    }
}
