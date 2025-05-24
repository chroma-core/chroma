/**
 * Map a device to the execution providers to use for the given device.
 * @param {import("../utils/devices.js").DeviceType|"auto"|null} [device=null] (Optional) The device to run the inference on.
 * @returns {ONNXExecutionProviders[]} The execution providers to use for the given device.
 */
export function deviceToExecutionProviders(device?: import("../utils/devices.js").DeviceType | "auto" | null): ONNXExecutionProviders[];
/**
 * Create an ONNX inference session.
 * @param {Uint8Array|string} buffer_or_path The ONNX model buffer or path.
 * @param {import('onnxruntime-common').InferenceSession.SessionOptions} session_options ONNX inference session options.
 * @param {Object} session_config ONNX inference session configuration.
 * @returns {Promise<import('onnxruntime-common').InferenceSession & { config: Object}>} The ONNX inference session.
 */
export function createInferenceSession(buffer_or_path: Uint8Array | string, session_options: import("onnxruntime-common").InferenceSession.SessionOptions, session_config: any): Promise<import("onnxruntime-common").InferenceSession & {
    config: any;
}>;
/**
 * Check if an object is an ONNX tensor.
 * @param {any} x The object to check
 * @returns {boolean} Whether the object is an ONNX tensor.
 */
export function isONNXTensor(x: any): boolean;
/**
 * Check if ONNX's WASM backend is being proxied.
 * @returns {boolean} Whether ONNX's WASM backend is being proxied.
 */
export function isONNXProxy(): boolean;
export { Tensor } from "onnxruntime-common";
export type ONNXExecutionProviders = import("onnxruntime-common").InferenceSession.ExecutionProviderConfig;
//# sourceMappingURL=onnx.d.ts.map