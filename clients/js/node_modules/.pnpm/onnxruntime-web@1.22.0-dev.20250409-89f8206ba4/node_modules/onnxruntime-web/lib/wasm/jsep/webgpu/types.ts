// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../wasm-common';
import { TensorView } from '../tensor-view';

import { ShaderHelper } from './ops/common';

export type SessionState = 'default' | 'capturing' | 'replaying';

export enum GpuDataType {
  default = 0,
  upload = 1,
  profile = 2,
}
export type GpuDataId = number;

export type GpuArchitecture = 'ampere' | 'gen-12lp';
export type GpuVendor = 'amd' | 'intel' | 'nvidia';
export interface AdapterInfo {
  isArchitecture: (architecture: GpuArchitecture) => boolean;
  isVendor: (vendor: GpuVendor) => boolean;
}

export interface GpuData {
  type: GpuDataType;
  id: GpuDataId;
  buffer: GPUBuffer;
}

export interface TensorInfo {
  dims: readonly number[];
  dataType: number;
}

export interface ProgramUniform {
  type: DataType;
  data: number | readonly number[];
}

export type ProgramUniformVariableInfo = [type: DataType, length: number];

/**
 * Represent the dependency of a program on a specific input tensor.
 *
 * - 'none': the shader/uniform does not depend on this input's info
 * - 'type': the shader/uniform depends on data type of this input
 * - 'rank': the shader/uniform depends on data type and the rank of this input
 * - 'dims': the shader/uniform depends on data type and the dims of this input
 * - 'data': the shader/uniform depends on data type, the dims and the data of this input
 */
export type ProgramInputTensorInfoDependency = 'none' | 'type' | 'rank' | 'dims' | 'data';

/**
 * Represent information about a program's cache for shader.
 */
export interface ProgramShaderCacheInfo {
  /**
   * an optional string as a cache hint in the artifact cache. If this is not specified, the cache hint will be empty.
   *
   * This hint string should only contains initializing-time information, such as the attributes or any information of
   * initializers. It should NOT contain any runtime information, such as the shape of inputs.
   */
  hint?: string;

  /**
   * an optional list of dependencies of the program on the input tensors. If this is not specified, the program depends
   * on 'dims' of all inputs.
   */
  inputDependencies?: ProgramInputTensorInfoDependency[];
}

/**
 * Represent information about a program's cache for uniform.
 */
export interface ProgramUniformCacheInfo {
  /**
   * an optional string as a cache hint in the uniform cache. If this is not specified, the cache hint will be empty.
   *
   * This hint string should only contains runtime information, such as the shape of inputs.
   */
  hint?: string;

  /**
   * an optional list of dependencies of the program on the input tensors. If this is not specified, the program depends
   * on 'none' of all inputs.
   */
  inputDependencies?: ProgramInputTensorInfoDependency[];
}

/**
 * A set of data that represent a shader program
 */
export interface ProgramInfo {
  /**
   * the name of the program. used for debugging and profiling
   */
  name: string;

  /**
   * an optional object describing the cache information of the program shader.
   *
   * If this is not specified, assume hint is empty and inputDependencies are ['dims'] for all inputs.
   */
  shaderCache?: ProgramShaderCacheInfo;

  /**
   * the shader's processing source code.
   *
   * This function will be called when shader cache missed.
   */
  getShaderSource: (shaderHelper: ShaderHelper) => string;

  /**
   * A function to get run data required to run the program.
   *
   * This function will be called every time the program is executed. Should keep this function as simple as possible.
   */
  getRunData: (inputs: readonly TensorView[]) => {
    outputs: readonly TensorInfo[];
    dispatchGroup: { x: number; y?: number; z?: number };
    programUniforms?: readonly ProgramUniform[];
  };
}

export interface Artifact {
  programInfo: ProgramInfo;
  computePipeline: GPUComputePipeline;
  uniformVariablesInfo: readonly ProgramUniformVariableInfo[] | undefined;
}

export interface ComputeContextInputsOutputsMapping {
  /**
   * specify the mapping to the program's inputs. the value can be a number or a tensor view.
   * - if it's a number, it's the index of the kernel's input
   * - if it's a tensor view, it's an existing tensor view that will be used as the input
   *
   * if inputs is not specified, the mapping will be the kernel's inputs in order.
   */
  readonly inputs?: ReadonlyArray<TensorView | number>;
  /**
   * specify the mapping to the program's outputs. the value must be a number.
   * - if it's a non-negative number, it's the index of the kernel's output
   * - if it's -1, it's an output that will be created as a temporary value. this value will be released after
   * the kernel is executed.
   * - if it's -2, it's an output that will be created as a persistent value. this value will be released when the
   * kernel is released.
   *
   * if outputs is not specified, the mapping will be the kernel's outputs in order.
   */
  readonly outputs?: readonly number[];
}

/**
 * A ComputeContext instance carries the states that representing the current running of a kernel.
 */
export interface ComputeContext {
  /**
   * gpu adapter info
   */
  readonly adapterInfo: AdapterInfo;

  /**
   * stores the pointer to OpKernelContext
   */
  readonly opKernelContext: number;

  /**
   * a list of inputs, each input is an instance of TensorView
   */
  readonly inputs: readonly TensorView[];

  /**
   * a custom data object that can be used to store any data that is needed by the kernel
   */
  readonly kernelCustomData: { [key: string]: unknown };

  /**
   * a buffer that can be used to access custom data created each time the kernel is executed
   */
  readonly customDataBuffer: Uint8Array;

  /**
   * a number of outputs for the node
   */
  readonly outputCount: number;

  compute(program: ProgramInfo, inputsOutputsMapping?: ComputeContextInputsOutputsMapping): TensorView[];
  output(index: number, dims: readonly number[]): number;
}

export type TimestampQuery = 'none' | 'inside-passes' | 'at-passes';
