/**
 * The list of devices supported by Transformers.js
 */
export const DEVICE_TYPES: Readonly<{
    auto: "auto";
    gpu: "gpu";
    cpu: "cpu";
    wasm: "wasm";
    webgpu: "webgpu";
    cuda: "cuda";
    dml: "dml";
    webnn: "webnn";
    'webnn-npu': "webnn-npu";
    'webnn-gpu': "webnn-gpu";
    'webnn-cpu': "webnn-cpu";
}>;
export type DeviceType = keyof typeof DEVICE_TYPES;
//# sourceMappingURL=devices.d.ts.map