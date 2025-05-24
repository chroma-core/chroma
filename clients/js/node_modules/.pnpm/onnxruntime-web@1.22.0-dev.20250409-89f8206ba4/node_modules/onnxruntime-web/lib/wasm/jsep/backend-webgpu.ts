// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Env, Tensor, TRACE, TRACE_FUNC_BEGIN, TRACE_FUNC_END } from 'onnxruntime-common';

import { DataType, tensorDataTypeEnumToString } from '../wasm-common';

import { configureLogger, LOG_DEBUG } from './log';
import { createView, TensorView } from './tensor-view';
import { createGpuDataManager, downloadGpuData, GpuDataManager } from './webgpu/gpu-data-manager';
import { RunFunction, WEBGPU_OP_RESOLVE_RULES } from './webgpu/op-resolve-rules';
import { ProgramManager } from './webgpu/program-manager';
import {
  AdapterInfo,
  ComputeContext,
  GpuArchitecture,
  GpuData,
  GpuVendor,
  ProgramInfo,
  ProgramInputTensorInfoDependency,
  SessionState,
  TimestampQuery,
} from './webgpu/types';

interface CommandInfo {
  readonly kernelId: number;
  readonly computePipeline: GPUComputePipeline;
  readonly bindGroup: GPUBindGroup;
  readonly dispatchGroup: [number, number, number];
}

interface KernelInfo {
  readonly kernelType: string;
  readonly kernelName: string;
  readonly kernelEntry: RunFunction;
  readonly attributes: [((attribute: unknown) => unknown) | undefined, unknown];
}

interface PendingKernelInfo {
  readonly kernelId: number;
  readonly programName: string;
  readonly inputTensorViews: readonly TensorView[];
  readonly outputTensorViews: readonly TensorView[];
}

const getProgramInputTensorInfoDependencyKey = (
  inputTensors: readonly TensorView[],
  inputDependencies: readonly ProgramInputTensorInfoDependency[],
): string => {
  if (inputDependencies.length !== inputTensors.length) {
    throw new Error(
      `inputDependencies length ${inputDependencies.length} is not equal to inputTensors length ${
        inputTensors.length
      }.`,
    );
  }

  const inputInfos: string[] = [];
  for (let i = 0; i < inputTensors.length; ++i) {
    const type = inputTensors[i].dataType;
    switch (inputDependencies[i]) {
      case 'none': {
        inputInfos.push('');
        break;
      }
      case 'type': {
        inputInfos.push(`${type}`);
        break;
      }
      case 'rank': {
        const rank = inputTensors[i].dims.length;
        inputInfos.push(`${type};${rank}`);
        break;
      }
      case 'dims': {
        const dims = inputTensors[i].dims.join(',');
        inputInfos.push(`${type};${dims}`);
        break;
      }
      default:
        throw new Error(`unsupported input dependency: ${inputDependencies[i]}`);
    }
  }

  return inputInfos.join('|');
};

/**
 * get a unique key representing the program from the program info, input shapes and types.
 *
 * @returns a unique key is a shorter string than the shader source, which contains all the information to identify a
 * program. if the key is the same, the program shader source should be the same, so we can reuse the program.
 *
 */
const getProgramInfoUniqueKey = (
  programInfo: ProgramInfo,
  inputTensors: readonly TensorView[],
  is1DimensionDispatch: boolean,
): string => {
  // final key format:
  // <PROGRAM_NAME>[<PROGRAM_CUSTOM_CACHE_HINT>]:is1DimensionDispatch:<INPUTS_INFO_0>|<INPUTS_INFO_1>|...
  let key = programInfo.name;
  if (programInfo.shaderCache?.hint) {
    key += '[' + programInfo.shaderCache.hint + ']';
  }
  key +=
    ':' +
    is1DimensionDispatch +
    `:${getProgramInputTensorInfoDependencyKey(
      inputTensors,
      programInfo.shaderCache?.inputDependencies ??
        new Array<ProgramInputTensorInfoDependency>(inputTensors.length).fill('dims'),
    )}`;
  return key;
};

class AdapterInfoImpl implements AdapterInfo {
  readonly architecture?: string;
  readonly vendor?: string;

  constructor(adapterInfo: GPUAdapterInfo) {
    if (adapterInfo) {
      this.architecture = adapterInfo.architecture;
      this.vendor = adapterInfo.vendor;
    }
  }

  isArchitecture(architecture: GpuArchitecture): boolean {
    return this.architecture === architecture;
  }

  isVendor(vendor: GpuVendor): boolean {
    return this.vendor === vendor;
  }
}

/**
 * this class is designed to store status and being used as a singleton for JSEP. It will be passed to jsepInit() as
 * the first parameter so that it is stored for future use.
 */
export class WebGpuBackend {
  adapterInfo: AdapterInfoImpl;
  device: GPUDevice;
  /**
   * an instance of GpuDataManager to manage a GpuDataId -> GpuBuffer mapping
   */
  gpuDataManager: GpuDataManager;
  /**
   * an instance of ProgramManager to build and run WebGPU compute shader program, and manage a ProgramKey -> Program
   * artifacts mapping
   */
  programManager: ProgramManager;

  /**
   * representing the session ID of which is currently being run.
   * `null` means no session is being run.
   * only valid when session.run is executed.
   */
  currentSessionId: number | null = null;

  /**
   * representing the kernel ID of which is currently being computed (CPU code perspective).
   * `null` means no kernel is being computed.
   * only one kernel can be computed at a moment.
   */
  currentKernelId: number | null = null;
  /**
   * a list of temporary GPU data for the current kernel. should release when the kernel done computation.
   */
  private temporaryData: GpuData[];
  /**
   * a KernelID -> a GPU data list, which stores persistent GPU data owned by the specific kernel.
   */
  private kernelPersistentData: Map<number, GpuData[]>;
  /**
   * a KernelID -> a custom data, which stores custom data owned by the specific kernel.
   */
  private kernelCustomData: Map<number, { [key: string]: unknown }>;
  /**
   * get the custom data of the current kernel
   */
  get currentKernelCustomData(): { [key: string]: unknown } {
    if (this.currentKernelId === null) {
      throw new Error('currentKernelCustomData(): currentKernelId is null. (should not happen)');
    }

    let data = this.kernelCustomData.get(this.currentKernelId);
    if (!data) {
      data = {};
      this.kernelCustomData.set(this.currentKernelId, data);
    }

    return data;
  }

  // KernelID -> kernelInfo mapping
  kernels: Map<number, KernelInfo>;
  private commandEncoder: GPUCommandEncoder | null = null;
  private computePassEncoder: GPUComputePassEncoder | null = null;
  maxDispatchNumber = 16;
  pendingDispatchNumber = 0;

  // info of kernels pending submission for a single batch
  private pendingKernels: PendingKernelInfo[] = [];
  // queryReadBuffer -> pendingKernels mapping for all the batches
  private pendingQueries: Map<GPUBuffer, PendingKernelInfo[]> = new Map();
  private queryResolveBuffer?: GPUBuffer;
  private querySet?: GPUQuerySet;
  private queryTimeBase?: bigint;
  queryType: TimestampQuery;

  env: Env;
  sessionStatus: SessionState = 'default';
  /**
   * a SessionID -> CommandInfo[] mapping. It's used to record all GPU commands for corresponding session.
   */
  capturedCommandList: Map<number, CommandInfo[]> = new Map();

  /**
   * a SessionID -> PendingKernelInfo[] mapping for profiling.
   */
  private capturedPendingKernels: Map<number, PendingKernelInfo[]> = new Map();

  /**
   * a SessionID -> a Map of (InputOutputIndex -> [ID, GPUBuffer]) mapping.
   */
  sessionExternalDataMapping: Map<number, Map<number, [number, GPUBuffer]>> = new Map();

  async initialize(env: Env, adapter: GPUAdapter): Promise<void> {
    this.env = env;
    const requiredFeatures: GPUFeatureName[] = [];
    const deviceDescriptor: GPUDeviceDescriptor = {
      requiredLimits: {
        maxComputeWorkgroupStorageSize: adapter.limits.maxComputeWorkgroupStorageSize,
        maxComputeWorkgroupsPerDimension: adapter.limits.maxComputeWorkgroupsPerDimension,
        maxStorageBufferBindingSize: adapter.limits.maxStorageBufferBindingSize,
        maxBufferSize: adapter.limits.maxBufferSize,
        maxComputeInvocationsPerWorkgroup: adapter.limits.maxComputeInvocationsPerWorkgroup,
        maxComputeWorkgroupSizeX: adapter.limits.maxComputeWorkgroupSizeX,
        maxComputeWorkgroupSizeY: adapter.limits.maxComputeWorkgroupSizeY,
        maxComputeWorkgroupSizeZ: adapter.limits.maxComputeWorkgroupSizeZ,
      },
      requiredFeatures,
    };

    // Try requiring WebGPU features
    const requireFeatureIfAvailable = (feature: GPUFeatureName) =>
      adapter.features.has(feature) && requiredFeatures.push(feature) && true;
    // Try chromium-experimental-timestamp-query-inside-passes and fallback to timestamp-query
    if (!requireFeatureIfAvailable('chromium-experimental-timestamp-query-inside-passes' as GPUFeatureName)) {
      requireFeatureIfAvailable('timestamp-query');
    }
    requireFeatureIfAvailable('shader-f16');
    // Try subgroups
    requireFeatureIfAvailable('subgroups' as GPUFeatureName);

    this.device = await adapter.requestDevice(deviceDescriptor);
    this.adapterInfo = new AdapterInfoImpl(adapter.info || (await adapter.requestAdapterInfo()));
    this.gpuDataManager = createGpuDataManager(this);
    this.programManager = new ProgramManager(this);
    this.kernels = new Map();
    this.kernelPersistentData = new Map();
    this.kernelCustomData = new Map();

    // set up flags for logger
    configureLogger(env.logLevel!, !!env.debug);

    // TODO: set up flags

    this.device.onuncapturederror = (ev) => {
      if (ev.error instanceof GPUValidationError) {
        // eslint-disable-next-line no-console
        console.error(`An uncaught WebGPU validation error was raised: ${ev.error.message}`);
      }
    };

    Object.defineProperty(this.env.webgpu, 'device', {
      value: this.device,
      writable: false,
      enumerable: true,
      configurable: false,
    });
    Object.defineProperty(this.env.webgpu, 'adapter', {
      value: adapter,
      writable: false,
      enumerable: true,
      configurable: false,
    });

    // init queryType, which is necessary for InferenceSession.create
    this.setQueryType();
  }

  dispose(): void {
    if (typeof this.querySet !== 'undefined') {
      this.querySet.destroy();
    }
    this.gpuDataManager.dispose();
  }

  getCommandEncoder(): GPUCommandEncoder {
    if (!this.commandEncoder) {
      this.commandEncoder = this.device.createCommandEncoder();
    }
    return this.commandEncoder;
  }

  getComputePassEncoder(): GPUComputePassEncoder {
    if (!this.computePassEncoder) {
      const commandEncoder = this.getCommandEncoder();
      const computePassDescriptor: GPUComputePassDescriptor = {};

      if (this.queryType === 'at-passes') {
        computePassDescriptor.timestampWrites = {
          querySet: this.querySet!,
          beginningOfPassWriteIndex: this.pendingDispatchNumber * 2,
          endOfPassWriteIndex: this.pendingDispatchNumber * 2 + 1,
        };
      }

      this.computePassEncoder = commandEncoder.beginComputePass(computePassDescriptor);
    }
    return this.computePassEncoder;
  }

  endComputePass(): void {
    if (this.computePassEncoder) {
      this.computePassEncoder.end();
      this.computePassEncoder = null;
    }
  }

  flush(): void {
    if (!this.commandEncoder) {
      return;
    }

    TRACE_FUNC_BEGIN();

    this.endComputePass();
    let queryReadBuffer: GPUBuffer;
    if (this.queryType !== 'none') {
      this.commandEncoder.resolveQuerySet(
        this.querySet!,
        0,
        this.pendingDispatchNumber * 2,
        this.queryResolveBuffer!,
        0,
      );

      queryReadBuffer = this.device.createBuffer(
        // eslint-disable-next-line no-bitwise
        { size: this.pendingDispatchNumber * 2 * 8, usage: GPUBufferUsage.MAP_READ | GPUBufferUsage.COPY_DST },
      );

      this.pendingQueries.set(queryReadBuffer, this.pendingKernels);
      this.pendingKernels = [];
      this.commandEncoder.copyBufferToBuffer(
        this.queryResolveBuffer!,
        0,
        queryReadBuffer,
        0,
        this.pendingDispatchNumber * 2 * 8,
      );
    }

    this.device.queue.submit([this.commandEncoder.finish()]);
    this.gpuDataManager.refreshPendingBuffers();
    this.commandEncoder = null;
    this.pendingDispatchNumber = 0;

    if (this.queryType !== 'none') {
      void queryReadBuffer!.mapAsync(GPUMapMode.READ).then(() => {
        const mappedData = new BigUint64Array(queryReadBuffer.getMappedRange());
        const pendingKernels = this.pendingQueries.get(queryReadBuffer)!;
        for (let i = 0; i < mappedData.length / 2; i++) {
          const pendingKernelInfo = pendingKernels[i];
          const kernelId = pendingKernelInfo.kernelId;
          const kernelInfo = this.kernels.get(kernelId)!;
          const kernelType = kernelInfo.kernelType;
          const kernelName = kernelInfo.kernelName;
          const programName = pendingKernelInfo.programName;
          const inputTensorViews = pendingKernelInfo.inputTensorViews;
          const outputTensorViews = pendingKernelInfo.outputTensorViews;
          const startTimeU64 = mappedData[i * 2];
          const endTimeU64 = mappedData[i * 2 + 1];

          if (typeof this.queryTimeBase === 'undefined') {
            this.queryTimeBase = startTimeU64;
          }

          const startTime = Number(startTimeU64 - this.queryTimeBase);
          const endTime = Number(endTimeU64 - this.queryTimeBase);

          if (!Number.isSafeInteger(startTime) || !Number.isSafeInteger(endTime)) {
            throw new RangeError('incorrect timestamp range');
          }

          if (this.env.webgpu.profiling?.ondata) {
            this.env.webgpu.profiling.ondata({
              version: 1,
              inputsMetadata: inputTensorViews.map((value) => ({
                dims: value.dims,
                dataType: tensorDataTypeEnumToString(value.dataType),
              })),
              outputsMetadata: outputTensorViews.map((value) => ({
                dims: value.dims,
                dataType: tensorDataTypeEnumToString(value.dataType),
              })),
              kernelId,
              kernelType,
              kernelName,
              programName,
              startTime,
              endTime,
            });
          } else {
            // if no callback is provided, print the profiling message to console
            let inputShapes = '';
            inputTensorViews.forEach((value, i) => {
              inputShapes += `input[${i}]: [${value.dims}] | ${tensorDataTypeEnumToString(value.dataType)}, `;
            });
            let outputShapes = '';
            outputTensorViews.forEach((value, i) => {
              outputShapes += `output[${i}]: [${value.dims}] | ${tensorDataTypeEnumToString(value.dataType)}, `;
            });
            // eslint-disable-next-line no-console
            console.log(
              `[profiling] kernel "${kernelId}|${kernelType}|${kernelName}|${programName}" ${inputShapes}${
                outputShapes
              }execution time: ${endTime - startTime} ns`,
            );
          }
          TRACE('GPU', `${programName}::${startTimeU64}::${endTimeU64}`);
        }
        queryReadBuffer.unmap();
        this.pendingQueries.delete(queryReadBuffer);
      });
    }
    TRACE_FUNC_END();
  }

  /**
   * run a WebGPU program.
   * @param program a ProgramInfo instance
   * @param inputTensorViews a TensorView array. each element represents a value already exists in GPU.
   * @param outputIndices an indices array. each element can be either -1 (temporary data), -2 (persistent data) or an
   * index to the kernel's output.
   * @param createKernelOutput a callback function that create a value to kernel's output with the given index
   * @param createIntermediateOutput a callback function that create a value as a intermediate value, either temporary
   * or persistent (owned by the current kernel)
   * @returns a TensorView array representing the result.
   */
  run(
    program: ProgramInfo,
    inputTensorViews: readonly TensorView[],
    outputIndices: readonly number[],
    createKernelOutput: (index: number, dataType: number, dims: readonly number[]) => TensorView,
    createIntermediateOutput: (dataType: number, dims: readonly number[]) => TensorView,
    outputCount: number,
  ): TensorView[] {
    TRACE_FUNC_BEGIN(program.name);
    // create info for inputs
    const inputDatas: GpuData[] = [];
    for (let i = 0; i < inputTensorViews.length; ++i) {
      const data = inputTensorViews[i].data;
      // if tensor view data is 0, it means the output is zero-sized tensor, and there is no GPU data for it.
      if (data === 0) {
        continue;
      }
      const gpuData = this.gpuDataManager.get(data);
      if (!gpuData) {
        throw new Error(`no GPU data for input: ${data}`);
      }
      inputDatas.push(gpuData);
    }

    const { outputs, dispatchGroup, programUniforms } = program.getRunData(inputTensorViews);

    // check output indices
    const validatedOutputIndices = outputIndices.length === 0 ? outputs.map((_, i) => i) : outputIndices;
    if (validatedOutputIndices.length !== outputs.length) {
      throw new Error(`Output size ${validatedOutputIndices.length} must be equal to ${outputs.length}.`);
    }

    // create info for outputs
    const outputTensorViews: TensorView[] = [];
    const outputDatas: GpuData[] = [];
    for (let i = 0; i < outputs.length; ++i) {
      // value -1 and -2 are used for creating temporary and persistent outputs.
      // value -3 is used for placeholder output. So -3, -2, -1 and 0, 1, 2, ... are valid
      // output indices. see type definition of ComputeContextInputsOutputsMapping for more details.
      if (
        !Number.isInteger(validatedOutputIndices[i]) ||
        validatedOutputIndices[i] < -3 ||
        validatedOutputIndices[i] >= outputCount
      ) {
        throw new Error(`Invalid output index: ${validatedOutputIndices[i]}`);
      }
      if (validatedOutputIndices[i] === -3) {
        continue;
      }
      const isTemporary = validatedOutputIndices[i] === -1;
      const isPersistent = validatedOutputIndices[i] === -2;
      const tensorView =
        isTemporary || isPersistent
          ? createIntermediateOutput(outputs[i].dataType, outputs[i].dims)
          : createKernelOutput(validatedOutputIndices[i], outputs[i].dataType, outputs[i].dims);
      outputTensorViews.push(tensorView);
      // if tensor view data is 0, it means the output is zero-sized tensor, and there is no GPU data for it.
      if (tensorView.data === 0) {
        continue;
      }
      const gpuData = this.gpuDataManager.get(tensorView.data);
      if (!gpuData) {
        throw new Error(`no GPU data for output: ${tensorView.data}`);
      }
      if (isTemporary) {
        this.temporaryData.push(gpuData);
      }
      if (isPersistent) {
        let persistentData = this.kernelPersistentData.get(this.currentKernelId!);
        if (!persistentData) {
          persistentData = [];
          this.kernelPersistentData.set(this.currentKernelId!, persistentData);
        }
        persistentData.push(gpuData);
      }
      outputDatas.push(gpuData);
    }

    // when there are any zero-sized tensor in the inputs or outputs, we should report error unless all outputs are
    // zero-sized tensors.
    if (inputDatas.length !== inputTensorViews.length || outputDatas.length !== outputTensorViews.length) {
      // if all outputs are zero-sized tensors, there is no need to run the program.
      if (outputDatas.length === 0) {
        TRACE_FUNC_END(program.name);
        return outputTensorViews;
      }
      // if some outputs are zero-sized tensors, report an error.
      //
      // TODO: so far we don't see any use case that outputs include both zero-sized tensors and non-zero-sized tensors.
      // If we see such use case, we need to make a change here to support it.
      throw new Error(
        `Program ${program.name} has zero-sized tensor(s) in inputs or outputs. This is not supported now.`,
      );
    }

    // load uniforms
    // TODO: add cache for uniform (is it necessary?)
    //
    let uniformBufferBinding: GPUBindingResource | undefined;
    if (programUniforms) {
      let currentOffset = 0;
      const offsets: number[] = [];

      programUniforms.forEach((v) => {
        const data = typeof v.data === 'number' ? [v.data] : v.data;
        if (data.length === 0) {
          return;
        }
        // https://www.w3.org/TR/WGSL/#alignof
        const sizeOfElement = v.type === DataType.float16 ? 2 : 4;
        let sizeOfVecOrMat;
        let baseAlignment;
        if (v.type === DataType.float16) {
          baseAlignment = data.length > 4 ? 16 : data.length > 2 ? 8 : data.length * sizeOfElement;
          sizeOfVecOrMat = data.length > 4 ? 16 : sizeOfElement * data.length;
        } else {
          baseAlignment = data.length <= 2 ? data.length * sizeOfElement : 16;
          sizeOfVecOrMat = 16;
        }
        currentOffset = Math.ceil(currentOffset / baseAlignment) * baseAlignment;
        offsets.push(currentOffset);
        // For non-float16 type, when data.length > 4, the uniform variable is of type array<vec4<i32|u32|f32>,N>, where
        // N = Math.ceil(data.length / 4) and SizeOf(vec4<i32|u32|f32>) = 16. The total byte length is N *
        // SizeOf(vec4<i32|u32|f32>). For float16 type, when data.length > 4, the uniform variable is of type
        // array<mat2x4<f16>,N>, where N = Math.ceil(data.length / 8) and SizeOf(mat2x4<f16>) = 16. The total byte
        // length is N * SizeOf(mat2x4<f16>).
        const elementPerVecOrMat = v.type === DataType.float16 ? 8 : 4;
        currentOffset +=
          data.length > 4 ? Math.ceil(data.length / elementPerVecOrMat) * sizeOfVecOrMat : data.length * sizeOfElement;
      });

      // Meet alignment of struct here: https://www.w3.org/TR/WGSL/#alignment-and-size. For simplicity, set
      // maxAlignmentOfField to 16 since the underlying buffer has been rounded up to 16.
      const maxAlignmentOfField = 16;
      currentOffset = Math.ceil(currentOffset / maxAlignmentOfField) * maxAlignmentOfField;
      const arrayBuffer = new ArrayBuffer(currentOffset);
      programUniforms.forEach((v, i) => {
        const offset = offsets[i];
        const data = typeof v.data === 'number' ? [v.data] : v.data;
        if (v.type === DataType.int32) {
          new Int32Array(arrayBuffer, offset, data.length).set(data);
        } else if (v.type === DataType.uint32) {
          new Uint32Array(arrayBuffer, offset, data.length).set(data);
        } else if (v.type === DataType.float16) {
          new Uint16Array(arrayBuffer, offset, data.length).set(data);
        } else if (v.type === DataType.float) {
          new Float32Array(arrayBuffer, offset, data.length).set(data);
        } else {
          throw new Error(`Unsupported uniform type: ${tensorDataTypeEnumToString(v.type)}`);
        }
      });

      const uniformBufferData =
        // eslint-disable-next-line no-bitwise
        this.gpuDataManager.create(currentOffset, GPUBufferUsage.COPY_DST | GPUBufferUsage.UNIFORM);
      this.device.queue.writeBuffer(uniformBufferData.buffer, 0, arrayBuffer, 0, currentOffset);
      this.gpuDataManager.release(uniformBufferData.id);
      uniformBufferBinding = { offset: 0, size: currentOffset, buffer: uniformBufferData.buffer };
    }

    const normalizedDispatchGroup = this.programManager.normalizeDispatchGroupSize(dispatchGroup);
    const is1DimensionDispatch = normalizedDispatchGroup[1] === 1 && normalizedDispatchGroup[2] === 1;
    // get program info
    const key = getProgramInfoUniqueKey(program, inputTensorViews, is1DimensionDispatch);
    let artifact = this.programManager.getArtifact(key);
    if (!artifact) {
      artifact = this.programManager.build(program, normalizedDispatchGroup);
      this.programManager.setArtifact(key, artifact);
      LOG_DEBUG('info', () => `[artifact] key: ${key}, programName: ${program.name}`);
    }

    // validate uniform variables
    if (programUniforms && artifact.uniformVariablesInfo) {
      if (programUniforms.length !== artifact.uniformVariablesInfo.length) {
        throw new Error(
          `Uniform variables count mismatch: expect ${artifact.uniformVariablesInfo.length}, got ${
            programUniforms.length
          } in program "${artifact.programInfo.name}".`,
        );
      }
      for (let i = 0; i < programUniforms.length; i++) {
        const uniform = programUniforms[i];
        const actualType = uniform.type;
        const actualLength = typeof uniform.data === 'number' ? 1 : uniform.data.length;
        const [type, length] = artifact.uniformVariablesInfo[i];
        if (actualType !== type || actualLength !== length) {
          throw new Error(
            `Uniform variable ${i} mismatch: expect type ${type} with size ${length}, got type ${
              actualType
            } with size ${actualLength} in program "${artifact.programInfo.name}".`,
          );
        }
      }
    }

    LOG_DEBUG(
      'info',
      () =>
        `[ProgramManager] run "${program.name}" (key=${key}) with ${normalizedDispatchGroup[0]}x${
          normalizedDispatchGroup[1]
        }x${normalizedDispatchGroup[2]}`,
    );

    if (this.queryType !== 'none' || this.sessionStatus === 'capturing') {
      const pendingKernelInfo: PendingKernelInfo = {
        kernelId: this.currentKernelId!,
        programName: artifact.programInfo.name,
        inputTensorViews,
        outputTensorViews,
      };
      this.pendingKernels.push(pendingKernelInfo);

      if (this.sessionStatus === 'capturing') {
        const sessionPendingKernels = this.capturedPendingKernels.get(this.currentSessionId!);
        sessionPendingKernels!.push(pendingKernelInfo);
      }
    }

    this.programManager.run(artifact, inputDatas, outputDatas, normalizedDispatchGroup, uniformBufferBinding);

    TRACE_FUNC_END(program.name);
    return outputTensorViews;
  }

  upload(gpuDataId: number, data: Uint8Array): void {
    this.gpuDataManager.upload(gpuDataId, data);
  }

  memcpy(src: number, dst: number): void {
    this.gpuDataManager.memcpy(src, dst);
  }

  async download(gpuDataId: number, getTargetBuffer: () => Uint8Array): Promise<void> {
    // the underlying buffer may be changed after the async function is called. so we use a getter function to make sure
    // the buffer is up-to-date.
    await this.gpuDataManager.download(gpuDataId, getTargetBuffer);
  }

  alloc(size: number): number {
    return this.gpuDataManager.create(size).id;
  }

  free(ptr: number): number {
    return this.gpuDataManager.release(ptr);
  }

  createKernel(kernelType: string, kernelId: number, attribute: unknown, kernelName: string): void {
    const op = WEBGPU_OP_RESOLVE_RULES.get(kernelType);
    if (!op) {
      throw new Error(`kernel not implemented: ${kernelType}`);
    }

    const kernelInfo: KernelInfo = {
      kernelType,
      kernelName,
      kernelEntry: op[0],
      attributes: [op[1], attribute],
    };
    this.kernels.set(kernelId, kernelInfo);
  }

  releaseKernel(kernelId: number): void {
    const persistentData = this.kernelPersistentData.get(kernelId);
    if (persistentData) {
      for (const data of persistentData) {
        this.gpuDataManager.release(data.id);
      }
      this.kernelPersistentData.delete(kernelId);
    }

    this.kernelCustomData.delete(kernelId);
    this.kernels.delete(kernelId);
  }

  computeKernel(kernelId: number, context: ComputeContext, errors: Array<Promise<string | null>>): number {
    const kernel = this.kernels.get(kernelId);
    if (!kernel) {
      throw new Error(`kernel not created: ${kernelId}`);
    }
    const kernelType = kernel.kernelType;
    const kernelName = kernel.kernelName;
    const kernelEntry = kernel.kernelEntry;
    const attributes = kernel.attributes;
    if (this.currentKernelId !== null) {
      throw new Error(`kernel "[${kernelType}] ${kernelName}" is not allowed to be called recursively`);
    }
    this.currentKernelId = kernelId;

    // parse attributes if necessary
    if (attributes[0]) {
      attributes[1] = attributes[0](attributes[1]);
      attributes[0] = undefined;
    }

    LOG_DEBUG('info', () => `[WebGPU] Start to run kernel "[${kernelType}] ${kernelName}"...`);

    const useErrorScope = this.env.debug;

    this.temporaryData = [];
    try {
      if (useErrorScope) {
        this.device.pushErrorScope('validation');
      }

      kernelEntry(context, attributes[1]);
      return 0; // ORT_OK
    } catch (e) {
      errors.push(Promise.resolve(`[WebGPU] Kernel "[${kernelType}] ${kernelName}" failed. ${e}`));
      return 1; // ORT_FAIL
    } finally {
      if (useErrorScope) {
        errors.push(
          this.device
            .popErrorScope()
            .then((err) =>
              err ? `GPU validation error for kernel "[${kernelType}] ${kernelName}": ${err.message}` : null,
            ),
        );
      }

      for (const data of this.temporaryData) {
        this.gpuDataManager.release(data.id);
      }
      this.temporaryData = [];
      this.currentKernelId = null;
    }
  }

  // #region external buffer
  registerBuffer(sessionId: number, index: number, buffer: GPUBuffer, size: number): number {
    let sessionInputOutputMapping = this.sessionExternalDataMapping.get(sessionId);
    if (!sessionInputOutputMapping) {
      sessionInputOutputMapping = new Map();
      this.sessionExternalDataMapping.set(sessionId, sessionInputOutputMapping);
    }

    // the buffer may be user created, or managed by GPU data manager.
    // The GPU data manager will not manage these buffers. we register them as external buffers.
    //
    // The map `sessionInputOutputMapping` is used to store the data ID and buffer for each input/output. Once a
    // specific input/output is registered, the data ID will not change.
    const previousBuffer = sessionInputOutputMapping.get(index);
    const id = this.gpuDataManager.registerExternalBuffer(buffer, size, previousBuffer);
    sessionInputOutputMapping.set(index, [id, buffer]);
    return id;
  }
  unregisterBuffers(sessionId: number): void {
    const sessionInputOutputMapping = this.sessionExternalDataMapping.get(sessionId);
    if (sessionInputOutputMapping) {
      sessionInputOutputMapping.forEach((bufferInfo) => this.gpuDataManager.unregisterExternalBuffer(bufferInfo[0]));
      this.sessionExternalDataMapping.delete(sessionId);
    }
  }
  getBuffer(gpuDataId: number): GPUBuffer {
    const gpuData = this.gpuDataManager.get(gpuDataId);
    if (!gpuData) {
      throw new Error(`no GPU data for buffer: ${gpuDataId}`);
    }
    return gpuData.buffer;
  }
  createDownloader(
    gpuBuffer: GPUBuffer,
    size: number,
    type: Tensor.GpuBufferDataTypes,
  ): () => Promise<Tensor.DataType> {
    return async () => {
      const data = await downloadGpuData(this, gpuBuffer, size);
      return createView(data.buffer, type);
    };
  }
  // #endregion
  writeTimestamp(index: number): void {
    if (this.queryType !== 'inside-passes') {
      return;
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (this.computePassEncoder as any).writeTimestamp(this.querySet, index);
  }
  setQueryType(): void {
    this.queryType = 'none';
    if (
      this.env.webgpu.profiling?.mode === 'default' ||
      (typeof this.env.trace === 'undefined' ? this.env.wasm.trace : this.env.trace)
    ) {
      if (this.device.features.has('chromium-experimental-timestamp-query-inside-passes')) {
        this.queryType = 'inside-passes';
      } else if (this.device.features.has('timestamp-query')) {
        this.queryType = 'at-passes';
      }

      if (this.queryType !== 'none' && typeof this.querySet === 'undefined') {
        this.querySet = this.device.createQuerySet({
          type: 'timestamp',
          count: this.maxDispatchNumber * 2,
        });
        this.queryResolveBuffer = this.device.createBuffer(
          // eslint-disable-next-line no-bitwise
          { size: this.maxDispatchNumber * 2 * 8, usage: GPUBufferUsage.COPY_SRC | GPUBufferUsage.QUERY_RESOLVE },
        );
      }
    }
  }

  captureBegin(): void {
    LOG_DEBUG('info', 'captureBegin');
    if (!this.capturedCommandList.get(this.currentSessionId!)) {
      this.capturedCommandList.set(this.currentSessionId!, []);
    }
    if (!this.capturedPendingKernels.get(this.currentSessionId!)) {
      this.capturedPendingKernels.set(this.currentSessionId!, []);
    }
    // flush the left commands before we change the status.
    this.flush();
    this.sessionStatus = 'capturing';
  }
  captureEnd(): void {
    LOG_DEBUG('info', 'captureEnd');
    // flush the left commands before we change the status.
    this.flush();
    this.sessionStatus = 'default';
  }
  replay(): void {
    LOG_DEBUG('info', 'replay');
    this.sessionStatus = 'replaying';
    const sessionCommandList = this.capturedCommandList.get(this.currentSessionId!);
    const sessionPendingKernels = this.capturedPendingKernels.get(this.currentSessionId!);
    const length = sessionCommandList!.length;
    this.pendingKernels = [];
    for (let i = 0; i < length; i++) {
      const computePassEncoder = this.getComputePassEncoder();
      const command = sessionCommandList![i];
      this.writeTimestamp(this.pendingDispatchNumber * 2);
      computePassEncoder.setPipeline(command.computePipeline);
      computePassEncoder.setBindGroup(0, command.bindGroup);
      computePassEncoder.dispatchWorkgroups(...command.dispatchGroup);
      this.writeTimestamp(this.pendingDispatchNumber * 2 + 1);
      this.pendingDispatchNumber++;
      if (this.queryType !== 'none') {
        this.pendingKernels.push(sessionPendingKernels![i]);
      }
      if (this.pendingDispatchNumber >= this.maxDispatchNumber || this.queryType === 'at-passes') {
        this.endComputePass();
      }
      if (this.pendingDispatchNumber >= this.maxDispatchNumber) {
        this.flush();
      }
    }
    // flush the left commands before we change the status.
    this.flush();
    this.sessionStatus = 'default';
  }

  onCreateSession(): void {
    this.gpuDataManager.onCreateSession();
  }

  onReleaseSession(sessionId: number): void {
    this.unregisterBuffers(sessionId);
    if (this.capturedCommandList.has(sessionId)) {
      this.capturedCommandList.delete(sessionId);
    }
    if (this.capturedPendingKernels.has(sessionId)) {
      this.capturedPendingKernels.delete(sessionId);
    }
    this.gpuDataManager.onReleaseSession(sessionId);
  }

  onRunStart(sessionId: number): void {
    this.currentSessionId = sessionId;
    this.setQueryType();
  }
}
