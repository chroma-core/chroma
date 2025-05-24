/**
 * Permutes a tensor according to the provided axes.
 * @param {any} tensor The input tensor to permute.
 * @param {Array} axes The axes to permute the tensor along.
 * @returns {Tensor} The permuted tensor.
 */
export function permute(tensor: any, axes: any[]): Tensor;
/**
 * Interpolates an Tensor to the given size.
 * @param {Tensor} input The input tensor to interpolate. Data must be channel-first (i.e., [c, h, w])
 * @param {number[]} size The output size of the image
 * @param {string} mode The interpolation mode
 * @param {boolean} align_corners Whether to align corners.
 * @returns {Tensor} The interpolated tensor.
 */
export function interpolate(input: Tensor, [out_height, out_width]: number[], mode?: string, align_corners?: boolean): Tensor;
/**
 * Down/up samples the input.
 * Inspired by https://pytorch.org/docs/stable/generated/torch.nn.functional.interpolate.html.
 * @param {Tensor} input the input tensor
 * @param {Object} options the options for the interpolation
 * @param {[number, number]|[number, number, number]|[number, number, number, number]} [options.size=null] output spatial size.
 * @param {"nearest"|"bilinear"|"bicubic"} [options.mode='bilinear'] algorithm used for upsampling
 * @returns {Promise<Tensor>} The interpolated tensor.
 */
export function interpolate_4d(input: Tensor, { size, mode, }?: {
    size?: [number, number] | [number, number, number] | [number, number, number, number];
    mode?: "nearest" | "bilinear" | "bicubic";
}): Promise<Tensor>;
/**
 * Matrix product of two tensors.
 * Inspired by https://pytorch.org/docs/stable/generated/torch.matmul.html
 * @param {Tensor} a the first tensor to be multiplied
 * @param {Tensor} b the second tensor to be multiplied
 * @returns {Promise<Tensor>} The matrix product of the two tensors.
 */
export function matmul(a: Tensor, b: Tensor): Promise<Tensor>;
/**
 * Computes the one dimensional Fourier transform of real-valued input.
 * Inspired by https://pytorch.org/docs/stable/generated/torch.fft.rfft.html
 * @param {Tensor} x the real input tensor
 * @param {Tensor} a The dimension along which to take the one dimensional real FFT.
 * @returns {Promise<Tensor>} the output tensor.
 */
export function rfft(x: Tensor, a: Tensor): Promise<Tensor>;
/**
 * Returns the k largest elements of the given input tensor.
 * Inspired by https://pytorch.org/docs/stable/generated/torch.topk.html
 * @param {Tensor} x the input tensor
 * @param {number} [k] the k in "top-k"
 * @returns {Promise<[Tensor, Tensor]>} the output tuple of (Tensor, LongTensor) of top-k elements and their indices.
 */
export function topk(x: Tensor, k?: number): Promise<[Tensor, Tensor]>;
/**
 * Slice a multidimensional float32 tensor.
 * @param {Tensor} data: Tensor of data to extract slices from
 * @param {number[]} starts: 1-D array of starting indices of corresponding axis in axes
 * @param {number[]} ends: 1-D array of ending indices (exclusive) of corresponding axis in axes
 * @param {number[]} axes: 1-D array of axes that starts and ends apply to
 * @param {number[]} [steps]: 1-D array of slice step of corresponding axis in axes.
 * @returns {Promise<Tensor>} Sliced data tensor.
 */
export function slice(data: Tensor, starts: number[], ends: number[], axes: number[], steps?: number[]): Promise<Tensor>;
/**
 * Perform mean pooling of the last hidden state followed by a normalization step.
 * @param {Tensor} last_hidden_state Tensor of shape [batchSize, seqLength, embedDim]
 * @param {Tensor} attention_mask Tensor of shape [batchSize, seqLength]
 * @returns {Tensor} Returns a new Tensor of shape [batchSize, embedDim].
 */
export function mean_pooling(last_hidden_state: Tensor, attention_mask: Tensor): Tensor;
/**
 * Apply Layer Normalization for last certain number of dimensions.
 * @param {Tensor} input The input tensor
 * @param {number[]} normalized_shape input shape from an expected input of size
 * @param {Object} options The options for the layer normalization
 * @param {number} [options.eps=1e-5] A value added to the denominator for numerical stability.
 * @returns {Tensor} The normalized tensor.
 */
export function layer_norm(input: Tensor, normalized_shape: number[], { eps, }?: {
    eps?: number;
}): Tensor;
/**
 * Concatenates an array of tensors along a specified dimension.
 * @param {Tensor[]} tensors The array of tensors to concatenate.
 * @param {number} dim The dimension to concatenate along.
 * @returns {Tensor} The concatenated tensor.
 */
export function cat(tensors: Tensor[], dim?: number): Tensor;
/**
 * Stack an array of tensors along a specified dimension.
 * @param {Tensor[]} tensors The array of tensors to stack.
 * @param {number} dim The dimension to stack along.
 * @returns {Tensor} The stacked tensor.
 */
export function stack(tensors: Tensor[], dim?: number): Tensor;
/**
 * Calculates the standard deviation and mean over the dimensions specified by dim. dim can be a single dimension or `null` to reduce over all dimensions.
 * @param {Tensor} input the input tenso
 * @param {number|null} dim the dimension to reduce. If None, all dimensions are reduced.
 * @param {number} correction difference between the sample size and sample degrees of freedom. Defaults to Bessel's correction, correction=1.
 * @param {boolean} keepdim whether the output tensor has dim retained or not.
 * @returns {Tensor[]} A tuple of (std, mean) tensors.
 */
export function std_mean(input: Tensor, dim?: number | null, correction?: number, keepdim?: boolean): Tensor[];
/**
 * Returns the mean value of each row of the input tensor in the given dimension dim.
 * @param {Tensor} input the input tensor.
 * @param {number|null} dim the dimension to reduce.
 * @param {boolean} keepdim whether the output tensor has dim retained or not.
 * @returns {Tensor} A new tensor with means taken along the specified dimension.
 */
export function mean(input: Tensor, dim?: number | null, keepdim?: boolean): Tensor;
/**
 * Creates a tensor of size size filled with fill_value. The tensor's dtype is inferred from fill_value.
 * @param {number[]} size A sequence of integers defining the shape of the output tensor.
 * @param {number|bigint|boolean} fill_value The value to fill the output tensor with.
 * @returns {Tensor} The filled tensor.
 */
export function full(size: number[], fill_value: number | bigint | boolean): Tensor;
export function full_like(tensor: any, fill_value: any): Tensor;
/**
 * Returns a tensor filled with the scalar value 1, with the shape defined by the variable argument size.
 * @param {number[]} size A sequence of integers defining the shape of the output tensor.
 * @returns {Tensor} The ones tensor.
 */
export function ones(size: number[]): Tensor;
/**
 * Returns a tensor filled with the scalar value 1, with the same size as input.
 * @param {Tensor} tensor The size of input will determine size of the output tensor.
 * @returns {Tensor} The ones tensor.
 */
export function ones_like(tensor: Tensor): Tensor;
/**
 * Returns a tensor filled with the scalar value 0, with the shape defined by the variable argument size.
 * @param {number[]} size A sequence of integers defining the shape of the output tensor.
 * @returns {Tensor} The zeros tensor.
 */
export function zeros(size: number[]): Tensor;
/**
 * Returns a tensor filled with the scalar value 0, with the same size as input.
 * @param {Tensor} tensor The size of input will determine size of the output tensor.
 * @returns {Tensor} The zeros tensor.
 */
export function zeros_like(tensor: Tensor): Tensor;
/**
 * Returns a tensor filled with random numbers from a uniform distribution on the interval [0, 1)
 * @param {number[]} size A sequence of integers defining the shape of the output tensor.
 * @returns {Tensor} The random tensor.
 */
export function rand(size: number[]): Tensor;
/**
 * Quantizes the embeddings tensor to binary or unsigned binary precision.
 * @param {Tensor} tensor The tensor to quantize.
 * @param {'binary'|'ubinary'} precision The precision to use for quantization.
 * @returns {Tensor} The quantized tensor.
 */
export function quantize_embeddings(tensor: Tensor, precision: "binary" | "ubinary"): Tensor;
export const DataTypeMap: Readonly<{
    float32: Float32ArrayConstructor;
    float16: Uint16ArrayConstructor | Float16ArrayConstructor;
    float64: Float64ArrayConstructor;
    string: ArrayConstructor;
    int8: Int8ArrayConstructor;
    uint8: Uint8ArrayConstructor;
    int16: Int16ArrayConstructor;
    uint16: Uint16ArrayConstructor;
    int32: Int32ArrayConstructor;
    uint32: Uint32ArrayConstructor;
    int64: BigInt64ArrayConstructor;
    uint64: BigUint64ArrayConstructor;
    bool: Uint8ArrayConstructor;
    uint4: Uint8ArrayConstructor;
    int4: Int8ArrayConstructor;
}>;
/**
 * @typedef {keyof typeof DataTypeMap} DataType
 * @typedef {import('./maths.js').AnyTypedArray | any[]} DataArray
 */
export class Tensor {
    /**
     * Create a new Tensor or copy an existing Tensor.
     * @param {[DataType, DataArray, number[]]|[ONNXTensor]} args
     */
    constructor(...args: [DataType, DataArray, number[]] | [ONNXTensor]);
    set dims(value: number[]);
    /** @type {number[]} Dimensions of the tensor. */
    get dims(): number[];
    /** @type {DataType} Type of the tensor. */
    get type(): DataType;
    /** @type {DataArray} The data stored in the tensor. */
    get data(): DataArray;
    /** @type {number} The number of elements in the tensor. */
    get size(): number;
    /** @type {string} The location of the tensor data. */
    get location(): string;
    ort_tensor: ONNXTensor;
    dispose(): void;
    /**
     * Index into a Tensor object.
     * @param {number} index The index to access.
     * @returns {Tensor} The data at the specified index.
     */
    _getitem(index: number): Tensor;
    /**
     * @param {number|bigint} item The item to search for in the tensor
     * @returns {number} The index of the first occurrence of item in the tensor data.
     */
    indexOf(item: number | bigint): number;
    /**
     * @param {number} index
     * @param {number} iterSize
     * @param {any} iterDims
     * @returns {Tensor}
     */
    _subarray(index: number, iterSize: number, iterDims: any): Tensor;
    /**
     * Returns the value of this tensor as a standard JavaScript Number. This only works
     * for tensors with one element. For other cases, see `Tensor.tolist()`.
     * @returns {number|bigint} The value of this tensor as a standard JavaScript Number.
     * @throws {Error} If the tensor has more than one element.
     */
    item(): number | bigint;
    /**
     * Convert tensor data to a n-dimensional JS list
     * @returns {Array}
     */
    tolist(): any[];
    /**
     * Return a new Tensor with the sigmoid function applied to each element.
     * @returns {Tensor} The tensor with the sigmoid function applied.
     */
    sigmoid(): Tensor;
    /**
     * Applies the sigmoid function to the tensor in place.
     * @returns {Tensor} Returns `this`.
     */
    sigmoid_(): Tensor;
    /**
     * Return a new Tensor with a callback function applied to each element.
     * @param {Function} callback - The function to apply to each element. It should take three arguments:
     *                              the current element, its index, and the tensor's data array.
     * @returns {Tensor} A new Tensor with the callback function applied to each element.
     */
    map(callback: Function): Tensor;
    /**
     * Apply a callback function to each element of the tensor in place.
     * @param {Function} callback - The function to apply to each element. It should take three arguments:
     *                              the current element, its index, and the tensor's data array.
     * @returns {Tensor} Returns `this`.
     */
    map_(callback: Function): Tensor;
    /**
     * Return a new Tensor with every element multiplied by a constant.
     * @param {number} val The value to multiply by.
     * @returns {Tensor} The new tensor.
     */
    mul(val: number): Tensor;
    /**
     * Multiply the tensor by a constant in place.
     * @param {number} val The value to multiply by.
     * @returns {Tensor} Returns `this`.
     */
    mul_(val: number): Tensor;
    /**
     * Return a new Tensor with every element divided by a constant.
     * @param {number} val The value to divide by.
     * @returns {Tensor} The new tensor.
     */
    div(val: number): Tensor;
    /**
     * Divide the tensor by a constant in place.
     * @param {number} val The value to divide by.
     * @returns {Tensor} Returns `this`.
     */
    div_(val: number): Tensor;
    /**
     * Return a new Tensor with every element added by a constant.
     * @param {number} val The value to add by.
     * @returns {Tensor} The new tensor.
     */
    add(val: number): Tensor;
    /**
     * Add the tensor by a constant in place.
     * @param {number} val The value to add by.
     * @returns {Tensor} Returns `this`.
     */
    add_(val: number): Tensor;
    /**
     * Return a new Tensor with every element subtracted by a constant.
     * @param {number} val The value to subtract by.
     * @returns {Tensor} The new tensor.
     */
    sub(val: number): Tensor;
    /**
     * Subtract the tensor by a constant in place.
     * @param {number} val The value to subtract by.
     * @returns {Tensor} Returns `this`.
     */
    sub_(val: number): Tensor;
    /**
     * Creates a deep copy of the current Tensor.
     * @returns {Tensor} A new Tensor with the same type, data, and dimensions as the original.
     */
    clone(): Tensor;
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
    slice(...slices: (number | number[] | null)[]): Tensor;
    /**
     * Return a permuted version of this Tensor, according to the provided dimensions.
     * @param  {...number} dims Dimensions to permute.
     * @returns {Tensor} The permuted tensor.
     */
    permute(...dims: number[]): Tensor;
    transpose(...dims: any[]): Tensor;
    /**
     * Returns the sum of each row of the input tensor in the given dimension dim.
     *
     * @param {number} [dim=null] The dimension or dimensions to reduce. If `null`, all dimensions are reduced.
     * @param {boolean} keepdim Whether the output tensor has `dim` retained or not.
     * @returns The summed tensor
     */
    sum(dim?: number, keepdim?: boolean): Tensor;
    /**
     * Returns the matrix norm or vector norm of a given tensor.
     * @param {number|string} [p='fro'] The order of norm
     * @param {number} [dim=null] Specifies which dimension of the tensor to calculate the norm across.
     * If dim is None, the norm will be calculated across all dimensions of input.
     * @param {boolean} [keepdim=false] Whether the output tensors have dim retained or not.
     * @returns {Tensor} The norm of the tensor.
     */
    norm(p?: number | string, dim?: number, keepdim?: boolean): Tensor;
    /**
     * Performs `L_p` normalization of inputs over specified dimension. Operates in place.
     * @param {number} [p=2] The exponent value in the norm formulation
     * @param {number} [dim=1] The dimension to reduce
     * @returns {Tensor} `this` for operation chaining.
     */
    normalize_(p?: number, dim?: number): Tensor;
    /**
     * Performs `L_p` normalization of inputs over specified dimension.
     * @param {number} [p=2] The exponent value in the norm formulation
     * @param {number} [dim=1] The dimension to reduce
     * @returns {Tensor} The normalized tensor.
     */
    normalize(p?: number, dim?: number): Tensor;
    /**
     * Compute and return the stride of this tensor.
     * Stride is the jump necessary to go from one element to the next one in the specified dimension dim.
     * @returns {number[]} The stride of this tensor.
     */
    stride(): number[];
    /**
     * Returns a tensor with all specified dimensions of input of size 1 removed.
     *
     * NOTE: The returned tensor shares the storage with the input tensor, so changing the contents of one will change the contents of the other.
     * If you would like a copy, use `tensor.clone()` before squeezing.
     *
     * @param {number|number[]} [dim=null] If given, the input will be squeezed only in the specified dimensions.
     * @returns {Tensor} The squeezed tensor
     */
    squeeze(dim?: number | number[]): Tensor;
    /**
     * In-place version of @see {@link Tensor.squeeze}
     */
    squeeze_(dim?: any): this;
    /**
     * Returns a new tensor with a dimension of size one inserted at the specified position.
     *
     * NOTE: The returned tensor shares the same underlying data with this tensor.
     *
     * @param {number} dim The index at which to insert the singleton dimension
     * @returns {Tensor} The unsqueezed tensor
     */
    unsqueeze(dim?: number): Tensor;
    /**
     * In-place version of @see {@link Tensor.unsqueeze}
     */
    unsqueeze_(dim?: any): this;
    /**
     * In-place version of @see {@link Tensor.flatten}
     */
    flatten_(start_dim?: number, end_dim?: number): this;
    /**
     * Flattens input by reshaping it into a one-dimensional tensor.
     * If `start_dim` or `end_dim` are passed, only dimensions starting with `start_dim`
     * and ending with `end_dim` are flattened. The order of elements in input is unchanged.
     * @param {number} start_dim the first dim to flatten
     * @param {number} end_dim the last dim to flatten
     * @returns {Tensor} The flattened tensor.
     */
    flatten(start_dim?: number, end_dim?: number): Tensor;
    /**
     * Returns a new tensor with the same data as the `self` tensor but of a different `shape`.
     * @param  {...number} dims the desired size
     * @returns {Tensor} The tensor with the same data but different shape
     */
    view(...dims: number[]): Tensor;
    neg_(): this;
    neg(): Tensor;
    /**
     * Computes input > val element-wise.
     * @param {number} val The value to compare with.
     * @returns {Tensor} A boolean tensor that is `true` where input is greater than other and `false` elsewhere.
     */
    gt(val: number): Tensor;
    /**
     * Computes input < val element-wise.
     * @param {number} val The value to compare with.
     * @returns {Tensor} A boolean tensor that is `true` where input is less than other and `false` elsewhere.
     */
    lt(val: number): Tensor;
    /**
     * In-place version of @see {@link Tensor.clamp}
     */
    clamp_(min: any, max: any): this;
    /**
     * Clamps all elements in input into the range [ min, max ]
     * @param {number} min lower-bound of the range to be clamped to
     * @param {number} max upper-bound of the range to be clamped to
     * @returns {Tensor} the output tensor.
     */
    clamp(min: number, max: number): Tensor;
    /**
     * In-place version of @see {@link Tensor.round}
     */
    round_(): this;
    /**
     * Rounds elements of input to the nearest integer.
     * @returns {Tensor} the output tensor.
     */
    round(): Tensor;
    mean(dim?: any, keepdim?: boolean): Tensor;
    min(dim?: any, keepdim?: boolean): Tensor;
    max(dim?: any, keepdim?: boolean): Tensor;
    argmin(dim?: any, keepdim?: boolean): Tensor;
    argmax(dim?: any, keepdim?: boolean): Tensor;
    /**
     * Performs Tensor dtype conversion.
     * @param {DataType} type The desired data type.
     * @returns {Tensor} The converted tensor.
     */
    to(type: DataType): Tensor;
    /**
     * Returns an iterator object for iterating over the tensor data in row-major order.
     * If the tensor has more than one dimension, the iterator will yield subarrays.
     * @returns {Iterator} An iterator object for iterating over the tensor data in row-major order.
     */
    [Symbol.iterator](): Iterator<any, any, any>;
}
/**
 * This creates a nested array of a given type and depth (see examples).
 */
export type NestArray<T, Depth extends number, Acc extends never[] = []> = Acc["length"] extends Depth ? T : NestArray<T[], Depth, [...Acc, never]>;
export type DataType = keyof typeof DataTypeMap;
export type DataArray = import("./maths.js").AnyTypedArray | any[];
import { Tensor as ONNXTensor } from '../backends/onnx.js';
//# sourceMappingURL=tensor.d.ts.map