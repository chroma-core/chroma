// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { TRACE_FUNC_BEGIN, TRACE_FUNC_END } from 'onnxruntime-common';

import { WebGpuBackend } from '../backend-webgpu';
import { LOG_DEBUG } from '../log';

import { createShaderHelper } from './ops/common';
import { Artifact, GpuData, ProgramInfo } from './types';

/**
 * ProgramManager is the main class behind running computations
 * It builds ProgramInfo's into Artifacts
 * It compiles given ProgramInfo's into WebGL Prorams (cached as Artifacts)
 * Uses the artifact to run the computation by calling Draw on
 * the WebGL drawing buffer
 * ProgramManager automatically maps (binds) input variables to their
 * corresponding Location's in the binary program
 */
export class ProgramManager {
  repo: Map<unknown, Artifact>; // this should be per-session object
  attributesBound: boolean;

  constructor(private backend: WebGpuBackend) {
    this.repo = new Map();
    this.attributesBound = false;
  }
  getArtifact(key: unknown): Artifact | undefined {
    return this.repo.get(key);
  }
  setArtifact(key: unknown, artifact: Artifact): void {
    this.repo.set(key, artifact);
  }
  run(
    buildArtifact: Artifact,
    inputs: GpuData[],
    outputs: GpuData[],
    dispatchGroup: [number, number, number],
    uniformBufferBinding: GPUBindingResource | undefined,
  ): void {
    TRACE_FUNC_BEGIN(buildArtifact.programInfo.name);
    const device = this.backend.device;
    const computePassEncoder = this.backend.getComputePassEncoder();
    this.backend.writeTimestamp(this.backend.pendingDispatchNumber * 2);
    const entries = [];
    for (const input of inputs) {
      entries.push({ binding: entries.length, resource: { buffer: input.buffer } });
    }
    for (const output of outputs) {
      entries.push({ binding: entries.length, resource: { buffer: output.buffer } });
    }
    if (uniformBufferBinding) {
      entries.push({ binding: entries.length, resource: uniformBufferBinding });
    }
    const bindGroup = device.createBindGroup({
      layout: buildArtifact.computePipeline.getBindGroupLayout(0),
      entries,
      label: buildArtifact.programInfo.name,
    });

    if (this.backend.sessionStatus === 'capturing') {
      const commandInfo = {
        kernelId: this.backend.currentKernelId!,
        computePipeline: buildArtifact.computePipeline,
        bindGroup,
        dispatchGroup,
      };
      const sessionCommandList = this.backend.capturedCommandList.get(this.backend.currentSessionId!);
      sessionCommandList!.push(commandInfo);
    }

    computePassEncoder.setPipeline(buildArtifact.computePipeline);
    computePassEncoder.setBindGroup(0, bindGroup);
    computePassEncoder.dispatchWorkgroups(...dispatchGroup);
    this.backend.writeTimestamp(this.backend.pendingDispatchNumber * 2 + 1);
    this.backend.pendingDispatchNumber++;

    if (
      this.backend.pendingDispatchNumber >= this.backend.maxDispatchNumber ||
      this.backend.queryType === 'at-passes'
    ) {
      this.backend.endComputePass();
    }
    if (this.backend.pendingDispatchNumber >= this.backend.maxDispatchNumber) {
      this.backend.flush();
    }
    TRACE_FUNC_END(buildArtifact.programInfo.name);
  }
  dispose(): void {
    // this.repo.forEach(a => this.glContext.deleteProgram(a.program));
  }
  build(programInfo: ProgramInfo, normalizedDispatchGroupSize: [number, number, number]): Artifact {
    TRACE_FUNC_BEGIN(programInfo.name);
    const device = this.backend.device;
    const enableDirectives: string[] = [];

    // Enable WGSL extensions based on available WebGPU features
    const extensionsInfo: Array<{ feature: GPUFeatureName; extension: string }> = [
      { feature: 'shader-f16', extension: 'f16' },
      { feature: 'subgroups' as GPUFeatureName, extension: 'subgroups' },
    ];
    extensionsInfo.forEach((info) => {
      if (device.features.has(info.feature)) {
        enableDirectives.push(`enable ${info.extension};`);
      }
    });

    const shaderHelper = createShaderHelper(normalizedDispatchGroupSize, this.backend.device.limits);
    const userCode = programInfo.getShaderSource(shaderHelper);
    const code = `${enableDirectives.join('\n')}\n${shaderHelper.additionalImplementations}\n${userCode}`;
    const shaderModule = device.createShaderModule({ code, label: programInfo.name });
    LOG_DEBUG('verbose', () => `[WebGPU] ${programInfo.name} shader code: ${code}`);

    const computePipeline = device.createComputePipeline({
      compute: { module: shaderModule, entryPoint: 'main' },
      layout: 'auto',
      label: programInfo.name,
    });

    TRACE_FUNC_END(programInfo.name);
    return { programInfo, computePipeline, uniformVariablesInfo: shaderHelper.variablesInfo };
  }

  normalizeDispatchGroupSize(
    dispatchGroup: ReturnType<ProgramInfo['getRunData']>['dispatchGroup'],
  ): [number, number, number] {
    const x = typeof dispatchGroup === 'number' ? dispatchGroup : dispatchGroup.x;
    const y = typeof dispatchGroup === 'number' ? 1 : dispatchGroup.y || 1;
    const z = typeof dispatchGroup === 'number' ? 1 : dispatchGroup.z || 1;
    const limitPerDimension = this.backend.device.limits.maxComputeWorkgroupsPerDimension;
    if (x <= limitPerDimension && y <= limitPerDimension && z <= limitPerDimension) {
      return [x, y, z];
    }
    const size = x * y * z;
    let dispatchAverage = Math.ceil(Math.sqrt(size));
    if (dispatchAverage > limitPerDimension) {
      dispatchAverage = Math.ceil(Math.cbrt(size));
      if (dispatchAverage > limitPerDimension) {
        throw new Error('Total dispatch size exceeds WebGPU maximum.');
      }
      return [dispatchAverage, dispatchAverage, dispatchAverage];
    } else {
      return [dispatchAverage, dispatchAverage, 1];
    }
  }
}
