/**
 * @file Helper module for `Tensor` processing.
 *
 * These functions and classes are only used internally,
 * meaning an end-user shouldn't need to access anything here.
 *
 * @module utils/tensor
 */

import {
    interpolate_data,
    max,
    min,
    permute_data
} from './maths.js';

import {
    Tensor as ONNXTensor, isONNXTensor,
} from '../backends/onnx.js';

import { TensorOpRegistry } from '../ops/registry.js';

export const DataTypeMap = Object.freeze({
    float32: Float32Array,
    // @ts-ignore ts(2552) Limited availability of Float16Array across browsers:
    // https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Float16Array
    float16: typeof Float16Array !== "undefined" ? Float16Array: Uint16Array,
    float64: Float64Array,
    string: Array, // string[]
    int8: Int8Array,
    uint8: Uint8Array,
    int16: Int16Array,
    uint16: Uint16Array,
    int32: Int32Array,
    uint32: Uint32Array,
    int64: BigInt64Array,
    uint64: BigUint64Array,
    bool: Uint8Array,
    uint4: Uint8Array,
    int4: Int8Array,
});

/**
 * @typedef {keyof typeof DataTypeMap} DataType
 * @typedef {import('./maths.js').AnyTypedArray | any[]} DataArray
 */


export class Tensor {
    /** @type {number[]} Dimensions of the tensor. */
    get dims() {
        // @ts-ignore
        return this.ort_tensor.dims;
    }
    set dims(value) {
        // FIXME: ONNXTensor declares dims as readonly so one needs to use the constructor() if dims change.
        // @ts-ignore
        this.ort_tensor.dims = value;
    }

    /** @type {DataType} Type of the tensor. */
    get type() {
        return this.ort_tensor.type;
    };

    /** @type {DataArray} The data stored in the tensor. */
    get data() {
        return this.ort_tensor.data;
    }

    /** @type {number} The number of elements in the tensor. */
    get size() {
        return this.ort_tensor.size;
    };

    /** @type {string} The location of the tensor data. */
    get location() {
        return this.ort_tensor.location;
    };

    ort_tensor;

    /**
     * Create a new Tensor or copy an existing Tensor.
     * @param {[DataType, DataArray, number[]]|[ONNXTensor]} args
     */
    constructor(...args) {
        if (isONNXTensor(args[0])) {
            this.ort_tensor = /** @type {ONNXTensor} */ (args[0]);
        } else {
            // Create new tensor
            this.ort_tensor = new ONNXTensor(
                /** @type {DataType} */(args[0]),
                // @ts-expect-error ts(2769) Type 'number' is not assignable to type 'bigint'.
                /** @type {Exclude<import('./maths.js').AnyTypedArray, Uint8ClampedArray>} */(args[1]),
                args[2],
            );
        }

        return new Proxy(this, {
            get: (obj, key) => {
                if (typeof key === 'string') {
                    let index = Number(key);
                    if (Number.isInteger(index)) {
                        // key is an integer (i.e., index)
                        return obj._getitem(index);
                    }
                }
                // @ts-ignore
                return obj[key];
            },
            set: (obj, key, value) => {
                // TODO allow setting of data

                // @ts-ignore
                return obj[key] = value;
            }
        });
    }

    dispose() {
        this.ort_tensor.dispose();
        // this.ort_tensor = undefined;
    }

    /**
     * Returns an iterator object for iterating over the tensor data in row-major order.
     * If the tensor has more than one dimension, the iterator will yield subarrays.
     * @returns {Iterator} An iterator object for iterating over the tensor data in row-major order.
     */
    *[Symbol.iterator]() {
        const [iterLength, ...iterDims] = this.dims;

        if (iterDims.length > 0) {
            const iterSize = iterDims.reduce((a, b) => a * b);
            for (let i = 0; i < iterLength; ++i) {
                yield this._subarray(i, iterSize, iterDims);
            }
        } else {
            yield* this.data
        }

    }

    /**
     * Index into a Tensor object.
     * @param {number} index The index to access.
     * @returns {Tensor} The data at the specified index.
     */
    _getitem(index) {
        const [iterLength, ...iterDims] = this.dims;

        index = safeIndex(index, iterLength);

        if (iterDims.length > 0) {
            const iterSize = iterDims.reduce((a, b) => a * b);
            return this._subarray(index, iterSize, iterDims);
        } else {
            return new Tensor(this.type, [this.data[index]], iterDims);
        }
    }

    /**
     * @param {number|bigint} item The item to search for in the tensor
     * @returns {number} The index of the first occurrence of item in the tensor data.
     */
    indexOf(item) {
        const this_data = this.data;
        for (let index = 0; index < this_data.length; ++index) {
            // Note: == instead of === so we can match Ints with BigInts
            if (this_data[index] == item) {
                return index;
            }
        }
        return -1;
    }

    /**
     * @param {number} index
     * @param {number} iterSize
     * @param {any} iterDims
     * @returns {Tensor}
     */
    _subarray(index, iterSize, iterDims) {
        const o1 = index * iterSize;
        const o2 = (index + 1) * iterSize;

        // We use subarray if available (typed array), otherwise we use slice (normal array)
        const data =
            ('subarray' in this.data)
                ? this.data.subarray(o1, o2)
                : this.data.slice(o1, o2);
        return new Tensor(this.type, data, iterDims);
    }

    /**
     * Returns the value of this tensor as a standard JavaScript Number. This only works
     * for tensors with one element. For other cases, see `Tensor.tolist()`.
     * @returns {number|bigint} The value of this tensor as a standard JavaScript Number.
     * @throws {Error} If the tensor has more than one element.
     */
    item() {
        const this_data = this.data;
        if (this_data.length !== 1) {
            throw new Error(`a Tensor with ${this_data.length} elements cannot be converted to Scalar`);
        }
        return this_data[0];
    }

    /**
     * Convert tensor data to a n-dimensional JS list
     * @returns {Array}
     */
    tolist() {
        return reshape(this.data, this.dims)
    }

    /**
     * Return a new Tensor with the sigmoid function applied to each element.
     * @returns {Tensor} The tensor with the sigmoid function applied.
     */
    sigmoid() {
        return this.clone().sigmoid_();
    }

    /**
     * Applies the sigmoid function to the tensor in place.
     * @returns {Tensor} Returns `this`.
     */
    sigmoid_() {
        const this_data = this.data;
        for (let i = 0; i < this_data.length; ++i) {
            this_data[i] = 1 / (1 + Math.exp(-this_data[i]));
        }
        return this;
    }

    /**
     * Return a new Tensor with a callback function applied to each element.
     * @param {Function} callback - The function to apply to each element. It should take three arguments:
     *                              the current element, its index, and the tensor's data array.
     * @returns {Tensor} A new Tensor with the callback function applied to each element.
     */
    map(callback) {
        return this.clone().map_(callback);
    }

    /**
     * Apply a callback function to each element of the tensor in place.
     * @param {Function} callback - The function to apply to each element. It should take three arguments:
     *                              the current element, its index, and the tensor's data array.
     * @returns {Tensor} Returns `this`.
     */
    map_(callback) {
        const this_data = this.data;
        for (let i = 0; i < this_data.length; ++i) {
            this_data[i] = callback(this_data[i], i, this_data);
        }
        return this;
    }

    /**
     * Return a new Tensor with every element multiplied by a constant.
     * @param {number} val The value to multiply by.
     * @returns {Tensor} The new tensor.
     */
    mul(val) {
        return this.clone().mul_(val);
    }

    /**
     * Multiply the tensor by a constant in place.
     * @param {number} val The value to multiply by.
     * @returns {Tensor} Returns `this`.
     */
    mul_(val) {
        const this_data = this.data;
        for (let i = 0; i < this_data.length; ++i) {
            this_data[i] *= val;
        }
        return this;
    }

    /**
     * Return a new Tensor with every element divided by a constant.
     * @param {number} val The value to divide by.
     * @returns {Tensor} The new tensor.
     */
    div(val) {
        return this.clone().div_(val);
    }

    /**
     * Divide the tensor by a constant in place.
     * @param {number} val The value to divide by.
     * @returns {Tensor} Returns `this`.
     */
    div_(val) {
        const this_data = this.data;
        for (let i = 0; i < this_data.length; ++i) {
            this_data[i] /= val;
        }
        return this;
    }

    /**
     * Return a new Tensor with every element added by a constant.
     * @param {number} val The value to add by.
     * @returns {Tensor} The new tensor.
     */
    add(val) {
        return this.clone().add_(val);
    }

    /**
     * Add the tensor by a constant in place.
     * @param {number} val The value to add by.
     * @returns {Tensor} Returns `this`.
     */
    add_(val) {
        const this_data = this.data;
        for (let i = 0; i < this_data.length; ++i) {
            this_data[i] += val;
        }
        return this;
    }

    /**
     * Return a new Tensor with every element subtracted by a constant.
     * @param {number} val The value to subtract by.
     * @returns {Tensor} The new tensor.
     */
    sub(val) {
        return this.clone().sub_(val);
    }

    /**
     * Subtract the tensor by a constant in place.
     * @param {number} val The value to subtract by.
     * @returns {Tensor} Returns `this`.
     */
    sub_(val) {
        const this_data = this.data;
        for (let i = 0; i < this_data.length; ++i) {
            this_data[i] -= val;
        }
        return this;
    }

    /**
     * Creates a deep copy of the current Tensor.
     * @returns {Tensor} A new Tensor with the same type, data, and dimensions as the original.
     */
    clone() {
        return new Tensor(this.type, this.data.slice(), this.dims.slice());
    }

    /**
     * Performs a slice operation on the Tensor along specified dimensions.
     *
     * Consider a Tensor that has a dimension of [4, 7]:
     * ```
     * [ 1,  2,  3,  4,  5,  6,  7]
     * [ 8,  9, 10, 11, 12, 13, 14]
     * [15, 16, 17, 18, 19, 20, 21]
     * [22, 23, 24, 25, 26, 27, 28]
     * ```
     * We can slice against the two dims of row and column, for instance in this
     * case we can start at the second element, and return to the second last,
     * like this:
     * ```
     * tensor.slice([1, -1], [1, -1]);
     * ```
     * which would return:
     * ```
     * [  9, 10, 11, 12, 13 ]
     * [ 16, 17, 18, 19, 20 ]
     * ```
     *
     * @param {...(number|number[]|null)} slices The slice specifications for each dimension.
     * - If a number is given, then a single element is selected.
     * - If an array of two numbers is given, then a range of elements [start, end (exclusive)] is selected.
     * - If null is given, then the entire dimension is selected.
     * @returns {Tensor} A new Tensor containing the selected elements.
     * @throws {Error} If the slice input is invalid.
     */
    slice(...slices) {
        // This allows for slicing with ranges and numbers
        const newTensorDims = [];
        const newOffsets = [];

        // slices is an array of numbers or arrays of numbers
        // e.g., slices = [0, [1, 3], null, [0, 3]]
        for (let sliceIndex = 0; sliceIndex < this.dims.length; ++sliceIndex) {
            let slice = slices[sliceIndex];

            if (slice === null || slice === undefined) {
                // null or undefined means take the whole dimension
                newOffsets.push([0, this.dims[sliceIndex]]);
                newTensorDims.push(this.dims[sliceIndex]);

            } else if (typeof slice === 'number') {
                slice = safeIndex(slice, this.dims[sliceIndex], sliceIndex);

                // A number means take a single element
                newOffsets.push([slice, slice + 1]);

            } else if (Array.isArray(slice) && slice.length === 2) {
                // An array of length 2 means take a range of elements
                let [start, end] = slice;
                start = start === null
                    ? 0
                    : safeIndex(start, this.dims[sliceIndex], sliceIndex, false);
                end = end === null
                    ? this.dims[sliceIndex]
                    : safeIndex(end, this.dims[sliceIndex], sliceIndex, false);

                if (start > end) {
                    throw new Error(`Invalid slice: ${slice}`);
                }

                const offsets = [
                    Math.max(start, 0),
                    Math.min(end, this.dims[sliceIndex])
                ];

                newOffsets.push(offsets);
                newTensorDims.push(offsets[1] - offsets[0]);

            } else {
                throw new Error(`Invalid slice: ${slice}`);
            }
        }

        const newDims = newOffsets.map(([start, end]) => end - start);
        const newBufferSize = newDims.reduce((a, b) => a * b);

        const this_data = this.data;
        // Allocate memory
        // @ts-ignore
        const data = new this_data.constructor(newBufferSize);

        // Precompute strides
        const stride = this.stride();

        for (let i = 0; i < newBufferSize; ++i) {
            let originalIndex = 0;
            for (let j = newDims.length - 1, num = i; j >= 0; --j) {
                const size = newDims[j];
                originalIndex += ((num % size) + newOffsets[j][0]) * stride[j];
                num = Math.floor(num / size);
            }
            data[i] = this_data[originalIndex];
        }
        return new Tensor(this.type, data, newTensorDims);
    }

    /**
     * Return a permuted version of this Tensor, according to the provided dimensions.
     * @param  {...number} dims Dimensions to permute.
     * @returns {Tensor} The permuted tensor.
     */
    permute(...dims) {
        return permute(this, dims);
    }

    // TODO: implement transpose. For now (backwards compatibility), it's just an alias for permute()
    transpose(...dims) {
        return this.permute(...dims);
    }

    /**
     * Returns the sum of each row of the input tensor in the given dimension dim.
     *
     * @param {number} [dim=null] The dimension or dimensions to reduce. If `null`, all dimensions are reduced.
     * @param {boolean} keepdim Whether the output tensor has `dim` retained or not.
     * @returns The summed tensor
     */
    sum(dim = null, keepdim = false) {
        return this.norm(1, dim, keepdim);
    }

    /**
     * Returns the matrix norm or vector norm of a given tensor.
     * @param {number|string} [p='fro'] The order of norm
     * @param {number} [dim=null] Specifies which dimension of the tensor to calculate the norm across.
     * If dim is None, the norm will be calculated across all dimensions of input.
     * @param {boolean} [keepdim=false] Whether the output tensors have dim retained or not.
     * @returns {Tensor} The norm of the tensor.
     */
    norm(p = 'fro', dim = null, keepdim = false) {
        if (p === 'fro') {
            // NOTE: Since we only support integer dims, Frobenius norm produces the same result as p=2.
            p = 2;
        } else if (typeof p === 'string') {
            throw Error(`Unsupported norm: ${p}`);
        }

        const this_data = this.data;
        const fn = (a, b) => a + (b ** p);

        if (dim === null) {
            // @ts-ignore
            const val = this_data.reduce(fn, 0) ** (1 / p);
            return new Tensor(this.type, [val], []);
        }

        const [type, result, resultDims] = reduce_helper(fn, this, dim, keepdim);

        if (p !== 1) {
            for (let i = 0; i < result.length; ++i) {
                result[i] = result[i] ** (1 / p);
            }
        }
        return new Tensor(type, result, resultDims);
    }

    /**
     * Performs `L_p` normalization of inputs over specified dimension. Operates in place.
     * @param {number} [p=2] The exponent value in the norm formulation
     * @param {number} [dim=1] The dimension to reduce
     * @returns {Tensor} `this` for operation chaining.
     */
    normalize_(p = 2.0, dim = 1) {
        dim = safeIndex(dim, this.dims.length);

        const norm = this.norm(p, dim, true);

        const this_data = this.data;
        const norm_data = norm.data;
        for (let i = 0; i < this_data.length; ++i) {

            // Calculate the index in the resulting array
            let resultIndex = 0;

            for (let j = this.dims.length - 1, num = i, resultMultiplier = 1; j >= 0; --j) {
                const size = this.dims[j];
                if (j !== dim) {
                    const index = num % size;
                    resultIndex += index * resultMultiplier;
                    resultMultiplier *= this.dims[j];
                }
                num = Math.floor(num / size);
            }

            // Divide by normalized value
            this_data[i] /= norm_data[resultIndex];
        }

        return this;
    }

    /**
     * Performs `L_p` normalization of inputs over specified dimension.
     * @param {number} [p=2] The exponent value in the norm formulation
     * @param {number} [dim=1] The dimension to reduce
     * @returns {Tensor} The normalized tensor.
     */
    normalize(p = 2.0, dim = 1) {
        return this.clone().normalize_(p, dim);
    }

    /**
     * Compute and return the stride of this tensor.
     * Stride is the jump necessary to go from one element to the next one in the specified dimension dim.
     * @returns {number[]} The stride of this tensor.
     */
    stride() {
        return dimsToStride(this.dims);
    }

    /**
     * Returns a tensor with all specified dimensions of input of size 1 removed.
     *
     * NOTE: The returned tensor shares the storage with the input tensor, so changing the contents of one will change the contents of the other.
     * If you would like a copy, use `tensor.clone()` before squeezing.
     *
     * @param {number|number[]} [dim=null] If given, the input will be squeezed only in the specified dimensions.
     * @returns {Tensor} The squeezed tensor
     */
    squeeze(dim = null) {
        return new Tensor(
            this.type,
            this.data,
            calc_squeeze_dims(this.dims, dim)
        )
    }

    /**
     * In-place version of @see {@link Tensor.squeeze}
     */
    squeeze_(dim = null) {
        this.dims = calc_squeeze_dims(this.dims, dim);
        return this;
    }

    /**
     * Returns a new tensor with a dimension of size one inserted at the specified position.
     *
     * NOTE: The returned tensor shares the same underlying data with this tensor.
     *
     * @param {number} dim The index at which to insert the singleton dimension
     * @returns {Tensor} The unsqueezed tensor
     */
    unsqueeze(dim = null) {
        return new Tensor(
            this.type,
            this.data,
            calc_unsqueeze_dims(this.dims, dim)
        );
    }

    /**
     * In-place version of @see {@link Tensor.unsqueeze}
     */
    unsqueeze_(dim = null) {
        this.dims = calc_unsqueeze_dims(this.dims, dim);
        return this;
    }

    /**
     * In-place version of @see {@link Tensor.flatten}
     */
    flatten_(start_dim = 0, end_dim = -1) {
        // TODO validate inputs
        end_dim = (end_dim + this.dims.length) % this.dims.length;

        let dimsToKeepBefore = this.dims.slice(0, start_dim);
        let dimsToFlatten = this.dims.slice(start_dim, end_dim + 1);
        let dimsToKeepAfter = this.dims.slice(end_dim + 1);

        this.dims = [...dimsToKeepBefore, dimsToFlatten.reduce((a, b) => a * b, 1), ...dimsToKeepAfter]
        return this;
    }

    /**
     * Flattens input by reshaping it into a one-dimensional tensor.
     * If `start_dim` or `end_dim` are passed, only dimensions starting with `start_dim`
     * and ending with `end_dim` are flattened. The order of elements in input is unchanged.
     * @param {number} start_dim the first dim to flatten
     * @param {number} end_dim the last dim to flatten
     * @returns {Tensor} The flattened tensor.
     */
    flatten(start_dim = 0, end_dim = -1) {
        return this.clone().flatten_(start_dim, end_dim);
    }

    /**
     * Returns a new tensor with the same data as the `self` tensor but of a different `shape`.
     * @param  {...number} dims the desired size
     * @returns {Tensor} The tensor with the same data but different shape
     */
    view(...dims) {
        // TODO: validate dims
        let inferredIndex = -1;
        for (let i = 0; i < dims.length; ++i) {
            if (dims[i] === -1) {
                if (inferredIndex !== -1) {
                    throw new Error("Only one dimension can be inferred");
                }
                inferredIndex = i;
            }
        }

        const this_data = this.data;
        if (inferredIndex !== -1) {
            // Some dimension must be inferred
            const productOther = dims.reduce((product, curr, index) => {
                return index !== inferredIndex ? product * curr : product
            }, 1);

            dims[inferredIndex] = this_data.length / productOther;
        }
        return new Tensor(this.type, this_data, dims); // NOTE: uses same underlying storage
    }

    neg_() {
        const this_data = this.data;
        for (let i = 0; i < this_data.length; ++i) {
            this_data[i] = -this_data[i];
        }
        return this;
    }
    neg() {
        return this.clone().neg_();
    }

    /**
     * Computes input > val element-wise.
     * @param {number} val The value to compare with.
     * @returns {Tensor} A boolean tensor that is `true` where input is greater than other and `false` elsewhere.
     */
    gt(val) {
        const mask = new Uint8Array(this.data.length);
        const this_data = this.data;
        for (let i = 0; i < this_data.length; ++i) {
            mask[i] = this_data[i] > val ? 1 : 0;
        }
        return new Tensor('bool', mask, this.dims);
    }

    /**
     * Computes input < val element-wise.
     * @param {number} val The value to compare with.
     * @returns {Tensor} A boolean tensor that is `true` where input is less than other and `false` elsewhere.
     */
    lt(val) {
        const mask = new Uint8Array(this.data.length);
        const this_data = this.data;
        for (let i = 0; i < this_data.length; ++i) {
            mask[i] = this_data[i] < val ? 1 : 0;
        }
        return new Tensor('bool', mask, this.dims);
    }

    /**
     * In-place version of @see {@link Tensor.clamp}
     */
    clamp_(min, max) {
        const this_data = this.data;
        for (let i = 0; i < this_data.length; ++i) {
            this_data[i] = Math.min(Math.max(this_data[i], min), max);
        }
        return this;
    }

    /**
     * Clamps all elements in input into the range [ min, max ]
     * @param {number} min lower-bound of the range to be clamped to
     * @param {number} max upper-bound of the range to be clamped to
     * @returns {Tensor} the output tensor.
     */
    clamp(min, max) {
        return this.clone().clamp_(min, max);
    }

    /**
     * In-place version of @see {@link Tensor.round}
     */
    round_() {
        const this_data = this.data;
        for (let i = 0; i < this_data.length; ++i) {
            this_data[i] = Math.round(this_data[i]);
        }
        return this;
    }

    /**
     * Rounds elements of input to the nearest integer.
     * @returns {Tensor} the output tensor.
     */
    round() {
        return this.clone().round_();
    }

    mean(dim = null, keepdim = false) {
        return mean(this, dim, keepdim);
    }

    min(dim = null, keepdim = false) {
        if (dim === null) {
            // None to reduce over all dimensions.
            const val = min(this.data)[0];
            return new Tensor(this.type, [val], [/* scalar */]);
        }
        const [type, result, resultDims] = reduce_helper((a, b) => Math.min(a, b), this, dim, keepdim, Infinity);
        return new Tensor(type, result, resultDims);
    }

    max(dim = null, keepdim = false) {
        if (dim === null) {
            // None to reduce over all dimensions.
            const val = max(this.data)[0];
            return new Tensor(this.type, [val], [/* scalar */]);
        }
        const [type, result, resultDims] = reduce_helper((a, b) => Math.max(a, b), this, dim, keepdim, -Infinity);
        return new Tensor(type, result, resultDims);
    }

    argmin(dim = null, keepdim = false) {
        if (dim !== null) {
            throw new Error("`dim !== null` not yet implemented.");
        }
        const index = min(this.data)[1];
        return new Tensor('int64', [BigInt(index)], []);
    }
    argmax(dim = null, keepdim = false) {
        if (dim !== null) {
            throw new Error("`dim !== null` not yet implemented.");
        }
        const index = max(this.data)[1];
        return new Tensor('int64', [BigInt(index)], []);
    }

    /**
     * Performs Tensor dtype conversion.
     * @param {DataType} type The desired data type.
     * @returns {Tensor} The converted tensor.
     */
    to(type) {
        // If the self Tensor already has the correct dtype, then self is returned.
        if (this.type === type) return this;

        // Otherwise, the returned tensor is a copy of self with the desired dtype.
        if (!DataTypeMap.hasOwnProperty(type)) {
            throw new Error(`Unsupported type: ${type}`);
        }

        // Handle special cases where a mapping function is needed (e.g., where one type is a bigint and the other is a number)
        let map_fn;
        const is_source_bigint = ['int64', 'uint64'].includes(this.type);
        const is_dest_bigint = ['int64', 'uint64'].includes(type);
        if (is_source_bigint && !is_dest_bigint) {
            // TypeError: Cannot convert a BigInt value to a number
            map_fn = Number;
        } else if (!is_source_bigint && is_dest_bigint) {
            // TypeError: Cannot convert [x] to a BigInt
            map_fn = BigInt;
        }

        // @ts-ignore
        return new Tensor(type, DataTypeMap[type].from(this.data, map_fn), this.dims);
    }
}

/**
 * This creates a nested array of a given type and depth (see examples).
 *
 * @example
 *   NestArray<string, 1>; // string[]
 * @example
 *   NestArray<number, 2>; // number[][]
 * @example
 *   NestArray<string, 3>; // string[][][] etc.
 * @template T
 * @template {number} Depth
 * @template {never[]} [Acc=[]]
 * @typedef {Acc['length'] extends Depth ? T : NestArray<T[], Depth, [...Acc, never]>} NestArray
 */

/**
 * Reshapes a 1-dimensional array into an n-dimensional array, according to the provided dimensions.
 *
 * @example
 *   reshape([10                    ], [1      ]); // Type: number[]      Value: [10]
 *   reshape([1, 2, 3, 4            ], [2, 2   ]); // Type: number[][]    Value: [[1, 2], [3, 4]]
 *   reshape([1, 2, 3, 4, 5, 6, 7, 8], [2, 2, 2]); // Type: number[][][]  Value: [[[1, 2], [3, 4]], [[5, 6], [7, 8]]]
 *   reshape([1, 2, 3, 4, 5, 6, 7, 8], [4, 2   ]); // Type: number[][]    Value: [[1, 2], [3, 4], [5, 6], [7, 8]]
 * @param {T[]|DataArray} data The input array to reshape.
 * @param {DIM} dimensions The target shape/dimensions.
 * @template T
 * @template {[number]|number[]} DIM
 * @returns {NestArray<T, DIM["length"]>} The reshaped array.
 */
function reshape(data, dimensions) {

    const totalElements = data.length;
    const dimensionSize = dimensions.reduce((a, b) => a * b);

    if (totalElements !== dimensionSize) {
        throw Error(`cannot reshape array of size ${totalElements} into shape (${dimensions})`);
    }

    /** @type {any} */
    let reshapedArray = data;

    for (let i = dimensions.length - 1; i >= 0; i--) {
        reshapedArray = reshapedArray.reduce((acc, val) => {
            let lastArray = acc[acc.length - 1];

            if (lastArray.length < dimensions[i]) {
                lastArray.push(val);
            } else {
                acc.push([val]);
            }

            return acc;
        }, [[]]);
    }

    return reshapedArray[0];
}

/**
 * Permutes a tensor according to the provided axes.
 * @param {any} tensor The input tensor to permute.
 * @param {Array} axes The axes to permute the tensor along.
 * @returns {Tensor} The permuted tensor.
 */
export function permute(tensor, axes) {
    const [permutedData, shape] = permute_data(tensor.data, tensor.dims, axes);
    return new Tensor(tensor.type, permutedData, shape);
}


/**
 * Interpolates an Tensor to the given size.
 * @param {Tensor} input The input tensor to interpolate. Data must be channel-first (i.e., [c, h, w])
 * @param {number[]} size The output size of the image
 * @param {string} mode The interpolation mode
 * @param {boolean} align_corners Whether to align corners.
 * @returns {Tensor} The interpolated tensor.
 */
export function interpolate(input, [out_height, out_width], mode = 'bilinear', align_corners = false) {

    // Input image dimensions
    const in_channels = input.dims.at(-3) ?? 1;
    const in_height = input.dims.at(-2);
    const in_width = input.dims.at(-1);

    let output = interpolate_data(
        /** @type {import('./maths.js').TypedArray}*/(input.data),
        [in_channels, in_height, in_width],
        [out_height, out_width],
        mode,
        align_corners
    );
    return new Tensor(input.type, output, [in_channels, out_height, out_width]);
}


/**
 * Down/up samples the input.
 * Inspired by https://pytorch.org/docs/stable/generated/torch.nn.functional.interpolate.html.
 * @param {Tensor} input the input tensor
 * @param {Object} options the options for the interpolation
 * @param {[number, number]|[number, number, number]|[number, number, number, number]} [options.size=null] output spatial size.
 * @param {"nearest"|"bilinear"|"bicubic"} [options.mode='bilinear'] algorithm used for upsampling
 * @returns {Promise<Tensor>} The interpolated tensor.
 */
export async function interpolate_4d(input, {
    size = null,
    mode = 'bilinear',
} = {}) {

    // Error checking
    if (input.dims.length !== 4) {
        throw new Error('`interpolate_4d` currently only supports 4D input.');
    }
    if (!size) {
        // TODO: support scale_factor
        throw new Error('`interpolate_4d` requires a `size` argument.');
    }

    // Fill in missing dimensions
    let targetDims;
    if (size.length === 2) {
        targetDims = [...input.dims.slice(0, 2), ...size];
    } else if (size.length === 3) {
        targetDims = [input.dims[0], ...size];
    } else if (size.length === 4) {
        targetDims = size;
    } else {
        throw new Error('`size` must be of length 2, 3, or 4.');
    }

    let op;
    if (mode === 'nearest') {
        op = await TensorOpRegistry.nearest_interpolate_4d;
    } else if (mode === 'bilinear') {
        op = await TensorOpRegistry.bilinear_interpolate_4d;
    } else if (mode === 'bicubic') {
        op = await TensorOpRegistry.bicubic_interpolate_4d;
    } else {
        throw new Error(`Unsupported mode: ${mode}`);
    }

    const sizeTensor = new Tensor('int64', new BigInt64Array(targetDims.map(BigInt)), [targetDims.length]);
    return await op({ x: input, s: sizeTensor });
}

/**
 * Matrix product of two tensors.
 * Inspired by https://pytorch.org/docs/stable/generated/torch.matmul.html
 * @param {Tensor} a the first tensor to be multiplied
 * @param {Tensor} b the second tensor to be multiplied
 * @returns {Promise<Tensor>} The matrix product of the two tensors.
 */
export async function matmul(a, b) {
    const op = await TensorOpRegistry.matmul;
    return await op({ a, b });
}

/**
 * Computes the one dimensional Fourier transform of real-valued input.
 * Inspired by https://pytorch.org/docs/stable/generated/torch.fft.rfft.html
 * @param {Tensor} x the real input tensor
 * @param {Tensor} a The dimension along which to take the one dimensional real FFT.
 * @returns {Promise<Tensor>} the output tensor.
 */
export async function rfft(x, a) {
    const op = await TensorOpRegistry.rfft;
    return await op({ x, a });
}


/**
 * Returns the k largest elements of the given input tensor.
 * Inspired by https://pytorch.org/docs/stable/generated/torch.topk.html
 * @param {Tensor} x the input tensor
 * @param {number} [k] the k in "top-k"
 * @returns {Promise<[Tensor, Tensor]>} the output tuple of (Tensor, LongTensor) of top-k elements and their indices.
 */
export async function topk(x, k) {
    const op = await TensorOpRegistry.top_k;

    if (k == null) {
        k = x.dims.at(-1);
    } else {
        k = Math.min(k, x.dims.at(-1));
    }
    return await op({
        x,
        k: new Tensor(
            'int64',
            [BigInt(k)],
            [1]
        )
    });
}


const arrayToIndexTensor = (array) => new Tensor('int64', array, [array.length]);
/**
 * Slice a multidimensional float32 tensor.
 * @param {Tensor} data: Tensor of data to extract slices from
 * @param {number[]} starts: 1-D array of starting indices of corresponding axis in axes
 * @param {number[]} ends: 1-D array of ending indices (exclusive) of corresponding axis in axes
 * @param {number[]} axes: 1-D array of axes that starts and ends apply to
 * @param {number[]} [steps]: 1-D array of slice step of corresponding axis in axes.
 * @returns {Promise<Tensor>} Sliced data tensor.
 */
export async function slice(data, starts, ends, axes, steps) {
    const op = await TensorOpRegistry.slice;
    return await op({
        x: data,
        s: arrayToIndexTensor(starts),
        e: arrayToIndexTensor(ends),
        a: arrayToIndexTensor(axes),
        t: arrayToIndexTensor(steps ?? new Array(axes.length).fill(1)),
    });
}


/**
 * Perform mean pooling of the last hidden state followed by a normalization step.
 * @param {Tensor} last_hidden_state Tensor of shape [batchSize, seqLength, embedDim]
 * @param {Tensor} attention_mask Tensor of shape [batchSize, seqLength]
 * @returns {Tensor} Returns a new Tensor of shape [batchSize, embedDim].
 */
export function mean_pooling(last_hidden_state, attention_mask) {
    // last_hidden_state: [batchSize, seqLength, embedDim]
    // attention_mask:    [batchSize, seqLength]
    const lastHiddenStateData = last_hidden_state.data;
    const attentionMaskData = attention_mask.data;

    const shape = [last_hidden_state.dims[0], last_hidden_state.dims[2]];

    // @ts-ignore
    const returnedData = new lastHiddenStateData.constructor(shape[0] * shape[1]);
    const [batchSize, seqLength, embedDim] = last_hidden_state.dims;

    let outIndex = 0;
    for (let i = 0; i < batchSize; ++i) {
        const offset = i * embedDim * seqLength;

        for (let k = 0; k < embedDim; ++k) {
            let sum = 0;
            let count = 0;

            const attnMaskOffset = i * seqLength;
            const offset2 = offset + k;
            // Pool over all words in sequence
            for (let j = 0; j < seqLength; ++j) {
                // index into attention mask
                const attn = Number(attentionMaskData[attnMaskOffset + j]);

                count += attn;
                sum += lastHiddenStateData[offset2 + j * embedDim] * attn;
            }

            const avg = sum / count;
            returnedData[outIndex++] = avg;
        }
    }

    return new Tensor(
        last_hidden_state.type,
        returnedData,
        shape
    )
}

/**
 * Apply Layer Normalization for last certain number of dimensions.
 * @param {Tensor} input The input tensor
 * @param {number[]} normalized_shape input shape from an expected input of size
 * @param {Object} options The options for the layer normalization
 * @param {number} [options.eps=1e-5] A value added to the denominator for numerical stability.
 * @returns {Tensor} The normalized tensor.
 */
export function layer_norm(input, normalized_shape, {
    eps = 1e-5,
} = {}) {
    if (input.dims.length !== 2) {
        throw new Error('`layer_norm` currently only supports 2D input.');
    }

    const [batchSize, featureDim] = input.dims;

    if (normalized_shape.length !== 1 && normalized_shape[0] !== featureDim) {
        throw new Error('`normalized_shape` must be a 1D array with shape `[input.dims[1]]`.');
    }

    const [std, mean] = std_mean(input, 1, 0, true);
    const stdData = /** @type {Float32Array} */(std.data);
    const meanData = /** @type {Float32Array} */(mean.data);

    const inputData = /** @type {Float32Array} */(input.data);

    // @ts-ignore
    const returnedData = new inputData.constructor(inputData.length);

    for (let i = 0; i < batchSize; ++i) {
        const offset = i * featureDim;
        for (let j = 0; j < featureDim; ++j) {
            const offset2 = offset + j;
            returnedData[offset2] = (inputData[offset2] - meanData[i]) / (stdData[i] + eps);
        }
    }
    return new Tensor(input.type, returnedData, input.dims);
}

/**
 * Helper function to calculate new dimensions when performing a squeeze operation.
 * @param {number[]} dims The dimensions of the tensor.
 * @param {number|number[]|null} dim The dimension(s) to squeeze.
 * @returns {number[]} The new dimensions.
 * @private
 */
function calc_squeeze_dims(dims, dim) {
    dims = dims.slice();
    if (dim === null) {
        dims = dims.filter((d) => d !== 1);
    } else if (typeof dim === 'number') {
        if (dims[dim] === 1) {
            dims.splice(dim, 1);
        }
    } else if (Array.isArray(dim)) {
        dims = dims.filter((x, i) => {
            return x !== 1 || !dim.includes(i);
        });
    }
    return dims;
}

/**
 * Helper function to calculate new dimensions when performing an unsqueeze operation.
 * @param {number[]} dims The dimensions of the tensor.
 * @param {number} dim The dimension to unsqueeze.
 * @returns {number[]} The new dimensions.
 * @private
 */
function calc_unsqueeze_dims(dims, dim) {
    // Dimension out of range (e.g., "expected to be in range of [-4, 3], but got 4")
    // + 1 since we allow inserting at the end (i.e. dim = -1)
    dim = safeIndex(dim, dims.length + 1);
    dims = dims.slice();
    // Insert 1 into specified dimension
    dims.splice(dim, 0, 1);
    return dims;
}

/**
 * Safely calculate the index for an array of a given size, allowing negative indexing.
 * @param {number} index The index that will be used.
 * @param {number} size The size of the array.
 * @param {number} [dimension=null] The dimension that the index is for (optional).
 * @returns {number} The index, guaranteed to be non-negative and less than `arrayLength`.
 *
 * @throws {Error} If the index is out of range.
 * @private
 */
function safeIndex(index, size, dimension = null, boundsCheck = true) {
    if (index < -size || index >= size) {
        if (boundsCheck) {
            throw new Error(`IndexError: index ${index} is out of bounds for dimension${dimension === null ? '' : ' ' + dimension} with size ${size}`);
        } else {
            return index < -size ? 0 : size;
        }
    }

    if (index < 0) {
        // Negative indexing, ensuring positive index
        index = ((index % size) + size) % size;
    }
    return index;
}

/**
 * Concatenates an array of tensors along a specified dimension.
 * @param {Tensor[]} tensors The array of tensors to concatenate.
 * @param {number} dim The dimension to concatenate along.
 * @returns {Tensor} The concatenated tensor.
 */
export function cat(tensors, dim = 0) {
    dim = safeIndex(dim, tensors[0].dims.length);

    // TODO do validation of shapes

    const resultDims = tensors[0].dims.slice();
    resultDims[dim] = tensors.reduce((a, b) => a + b.dims[dim], 0);

    // Create a new array to store the accumulated values
    const resultSize = resultDims.reduce((a, b) => a * b, 1);
    // @ts-ignore
    const result = new tensors[0].data.constructor(resultSize);

    // Create output tensor of same type as first
    const resultType = tensors[0].type;

    if (dim === 0) {
        // Handle special case for performance reasons

        let offset = 0;
        for (const tensor of tensors) {
            const tensorData = tensor.data;
            result.set(tensorData, offset);
            offset += tensorData.length;
        }

    } else {

        let currentDim = 0;

        for (let t = 0; t < tensors.length; ++t) {
            const { data, dims } = tensors[t];

            // Iterate over the data array
            for (let i = 0; i < data.length; ++i) {
                // Calculate the index in the resulting array
                let resultIndex = 0;

                for (let j = dims.length - 1, num = i, resultMultiplier = 1; j >= 0; --j) {
                    const size = dims[j];
                    let index = num % size;
                    if (j === dim) {
                        index += currentDim;
                    }
                    resultIndex += index * resultMultiplier;
                    resultMultiplier *= resultDims[j];
                    num = Math.floor(num / size);
                }
                // Accumulate the value at the current index
                result[resultIndex] = data[i];
            }

            currentDim += dims[dim];
        }
    }
    return new Tensor(resultType, result, resultDims);
}

/**
 * Stack an array of tensors along a specified dimension.
 * @param {Tensor[]} tensors The array of tensors to stack.
 * @param {number} dim The dimension to stack along.
 * @returns {Tensor} The stacked tensor.
 */
export function stack(tensors, dim = 0) {
    // TODO do validation of shapes
    // NOTE: stack expects each tensor to be equal size
    return cat(tensors.map(t => t.unsqueeze(dim)), dim);
}


/**
 * @param {(previousValue: any, currentValue: any, currentIndex?: number, resultIndex?: number) => any} callbackfn
 * @param {Tensor} input the input tensor.
 * @param {number|null} dim the dimension to reduce.
 * @param {boolean} keepdim whether the output tensor has dim retained or not.
 * @returns {[DataType, any, number[]]} The reduced tensor data.
 */
function reduce_helper(callbackfn, input, dim = null, keepdim = false, initialValue = null) {
    const inputData = input.data;
    const inputDims = input.dims;

    // Negative indexing
    dim = safeIndex(dim, inputDims.length);

    // Calculate the shape of the resulting array after summation
    const resultDims = inputDims.slice(); // Copy the original dimensions
    resultDims[dim] = 1; // Remove the specified axis

    // Create a new array to store the accumulated values
    // @ts-ignore
    const result = new inputData.constructor(inputData.length / inputDims[dim]);
    if (initialValue !== null) {
        result.fill(initialValue);
    }

    // Iterate over the data array
    for (let i = 0; i < inputData.length; ++i) {

        // Calculate the index in the resulting array
        let resultIndex = 0;

        for (let j = inputDims.length - 1, num = i, resultMultiplier = 1; j >= 0; --j) {
            const size = inputDims[j];
            if (j !== dim) {
                const index = num % size;
                resultIndex += index * resultMultiplier;
                resultMultiplier *= resultDims[j];
            }
            num = Math.floor(num / size);
        }

        // Accumulate the value at the current index
        result[resultIndex] = callbackfn(result[resultIndex], inputData[i], i, resultIndex);
    }

    if (!keepdim) resultDims.splice(dim, 1);

    return [input.type, result, resultDims];
}


/**
 * Calculates the standard deviation and mean over the dimensions specified by dim. dim can be a single dimension or `null` to reduce over all dimensions.
 * @param {Tensor} input the input tenso
 * @param {number|null} dim the dimension to reduce. If None, all dimensions are reduced.
 * @param {number} correction difference between the sample size and sample degrees of freedom. Defaults to Bessel's correction, correction=1.
 * @param {boolean} keepdim whether the output tensor has dim retained or not.
 * @returns {Tensor[]} A tuple of (std, mean) tensors.
 */
export function std_mean(input, dim = null, correction = 1, keepdim = false) {
    const inputData = /** @type {Float32Array} */(input.data);
    const inputDims = input.dims;

    if (dim === null) {
        // None to reduce over all dimensions.
        const sum = inputData.reduce((a, b) => a + b, 0);
        const mean = sum / inputData.length;
        const std = Math.sqrt(inputData.reduce((a, b) => a + (b - mean) ** 2, 0) / (inputData.length - correction));

        const meanTensor = new Tensor(input.type, [mean], [/* scalar */]);
        const stdTensor = new Tensor(input.type, [std], [/* scalar */]);

        return [stdTensor, meanTensor];
    }
    dim = safeIndex(dim, inputDims.length);
    const meanTensor = mean(input, dim, keepdim);
    const meanTensorData = meanTensor.data;

    // Compute squared sum
    const [type, result, resultDims] = reduce_helper((a, b, i, j) => a + (b - meanTensorData[j]) ** 2, input, dim, keepdim);

    // Square root of the squared sum
    for (let i = 0; i < result.length; ++i) {
        result[i] = Math.sqrt(result[i] / (inputDims[dim] - correction));
    }

    const stdTensor = new Tensor(type, result, resultDims);

    return [stdTensor, meanTensor];
}

/**
 * Returns the mean value of each row of the input tensor in the given dimension dim.
 * @param {Tensor} input the input tensor.
 * @param {number|null} dim the dimension to reduce.
 * @param {boolean} keepdim whether the output tensor has dim retained or not.
 * @returns {Tensor} A new tensor with means taken along the specified dimension.
 */
export function mean(input, dim = null, keepdim = false) {
    const inputDims = input.dims;
    const inputData = /** @type {Float32Array} */(input.data);

    if (dim === null) {
        // None to reduce over all dimensions.
        const val = inputData.reduce((a, b) => a + b, 0);
        return new Tensor(input.type, [val / inputData.length], [/* scalar */]);
    }
    dim = safeIndex(dim, inputDims.length);

    // Compute sum
    const [type, result, resultDims] = reduce_helper((a, b) => a + b, input, dim, keepdim);

    // Divide by number of elements in the dimension
    if (inputDims[dim] !== 1) {
        for (let i = 0; i < result.length; ++i) {
            result[i] /= inputDims[dim];
        }
    }

    return new Tensor(type, result, resultDims);
}


function dimsToStride(dims) {
    const stride = new Array(dims.length);
    for (let i = dims.length - 1, s2 = 1; i >= 0; --i) {
        stride[i] = s2;
        s2 *= dims[i];
    }
    return stride;
}

function fullHelper(size, fill_value, dtype, cls) {
    const numElements = size.reduce((a, b) => a * b, 1);
    return new Tensor(
        dtype,
        new cls(numElements).fill(fill_value),
        size
    )
}

/**
 * Creates a tensor of size size filled with fill_value. The tensor's dtype is inferred from fill_value.
 * @param {number[]} size A sequence of integers defining the shape of the output tensor.
 * @param {number|bigint|boolean} fill_value The value to fill the output tensor with.
 * @returns {Tensor} The filled tensor.
 */
export function full(size, fill_value) {
    let dtype;
    let typedArrayCls;
    if (typeof fill_value === 'number') {
        dtype = 'float32';
        typedArrayCls = Float32Array;
    } else if (typeof fill_value === 'bigint') {
        dtype = 'int64';
        typedArrayCls = BigInt64Array;
    } else if (typeof fill_value === 'boolean') {
        dtype = 'bool';
        typedArrayCls = Uint8Array;
    } else {
        // TODO: support other dtypes
        throw new Error(`Unsupported data type: ${typeof fill_value}`);
    }
    return fullHelper(size, fill_value, dtype, typedArrayCls);
}

export function full_like(tensor, fill_value) {
    return full(tensor.dims, fill_value);
}

/**
 * Returns a tensor filled with the scalar value 1, with the shape defined by the variable argument size.
 * @param {number[]} size A sequence of integers defining the shape of the output tensor.
 * @returns {Tensor} The ones tensor.
 */
export function ones(size) {
    return fullHelper(size, 1n, 'int64', BigInt64Array);
}

/**
 * Returns a tensor filled with the scalar value 1, with the same size as input.
 * @param {Tensor} tensor The size of input will determine size of the output tensor.
 * @returns {Tensor} The ones tensor.
 */
export function ones_like(tensor) {
    return ones(tensor.dims);
}

/**
 * Returns a tensor filled with the scalar value 0, with the shape defined by the variable argument size.
 * @param {number[]} size A sequence of integers defining the shape of the output tensor.
 * @returns {Tensor} The zeros tensor.
 */
export function zeros(size) {
    return fullHelper(size, 0n, 'int64', BigInt64Array);
}

/**
 * Returns a tensor filled with the scalar value 0, with the same size as input.
 * @param {Tensor} tensor The size of input will determine size of the output tensor.
 * @returns {Tensor} The zeros tensor.
 */
export function zeros_like(tensor) {
    return zeros(tensor.dims);
}

/**
 * Returns a tensor filled with random numbers from a uniform distribution on the interval [0, 1)
 * @param {number[]} size A sequence of integers defining the shape of the output tensor.
 * @returns {Tensor} The random tensor.
 */
export function rand(size) {
    const length = size.reduce((a, b) => a * b, 1);
    return new Tensor(
        "float32",
        Float32Array.from({ length }, () => Math.random()),
        size,
    )
}

/**
 * Quantizes the embeddings tensor to binary or unsigned binary precision.
 * @param {Tensor} tensor The tensor to quantize.
 * @param {'binary'|'ubinary'} precision The precision to use for quantization.
 * @returns {Tensor} The quantized tensor.
 */
export function quantize_embeddings(tensor, precision) {
    if (tensor.dims.length !== 2) {
        throw new Error("The tensor must have 2 dimensions");
    }
    if (tensor.dims.at(-1) % 8 !== 0) {
        throw new Error("The last dimension of the tensor must be a multiple of 8");
    }
    if (!['binary', 'ubinary'].includes(precision)) {
        throw new Error("The precision must be either 'binary' or 'ubinary'");
    }

    const signed = precision === 'binary';
    const dtype = signed ? 'int8' : 'uint8';

    // Create a typed array to store the packed bits
    const cls = signed ? Int8Array : Uint8Array;
    const inputData = tensor.data;
    const outputData = new cls(inputData.length / 8);

    // Iterate over each number in the array
    for (let i = 0; i < inputData.length; ++i) {
        // Determine if the number is greater than 0
        const bit = inputData[i] > 0 ? 1 : 0;

        // Calculate the index in the typed array and the position within the byte
        const arrayIndex = Math.floor(i / 8);
        const bitPosition = i % 8;

        // Pack the bit into the typed array
        outputData[arrayIndex] |= bit << (7 - bitPosition);
        if (signed && bitPosition === 0) {
            outputData[arrayIndex] -= 128;
        }
    };

    return new Tensor(dtype, outputData, [tensor.dims[0], tensor.dims[1] / 8]);
}
