// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { WebGpuBackend } from '../backend-webgpu';
import { LOG_DEBUG } from '../log';

import { GpuData, GpuDataId, GpuDataType } from './types';

/**
 * manages GpuDataId -> GpuBuffer
 */
export interface GpuDataManager {
  /**
   * copy data from CPU to GPU.
   */
  upload(id: GpuDataId, data: Uint8Array): void;
  /**
   * copy data from GPU to GPU.
   */
  memcpy(sourceId: GpuDataId, destinationId: GpuDataId): void;
  /**
   * create new data on GPU.
   */
  create(size: number, usage?: number): GpuData;
  /**
   * get GPU data by ID.
   */
  get(id: GpuDataId): GpuData | undefined;
  /**
   * release the data on GPU by ID.
   *
   * @return size of the data released
   */
  release(id: GpuDataId): number;
  /**
   * copy data from GPU to CPU.
   */
  download(id: GpuDataId, getTargetBuffer: () => Uint8Array): Promise<void>;

  /**
   * refresh the buffers that marked for release.
   *
   * when release() is called, the buffer is not released immediately. this is because we need to wait for the commands
   * to be submitted to the GPU. this function is called after the commands are submitted so that the buffers can be
   * actually released.
   */
  refreshPendingBuffers(): void;

  /**
   * register an external buffer for IO Binding. If the buffer is already registered, return the existing GPU data ID.
   *
   * GPU data manager only manages a mapping between the buffer and the GPU data ID. It will not manage the lifecycle of
   * the external buffer.
   */
  registerExternalBuffer(buffer: GPUBuffer, originalSize: number, previous?: [GpuDataId, GPUBuffer]): number;

  /**
   * unregister an external buffer for IO Binding.
   */
  unregisterExternalBuffer(id: GpuDataId): void;

  /**
   * destroy all gpu buffers.
   */
  dispose(): void;

  /**
   * create session related data.
   */
  onCreateSession(): void;

  /**
   * release session related data.
   * @param sessionId - specify the session ID.
   */
  onReleaseSession(sessionId: number): void;
}

interface StorageCacheValue {
  gpuData: GpuData;
  originalSize: number;
}

const bucketFreelist: Map<number, number> = new Map([
  [64, 250],
  [128, 200],
  [256, 200],
  [512, 200],
  [2048, 230],
  [4096, 200],
  [8192, 50],
  [16384, 50],
  [32768, 50],
  [65536, 50],
  [131072, 50],
  [262144, 50],
  [524288, 50],
  [1048576, 50],
  [2097152, 30],
  [4194304, 20],
  [8388608, 10],
  [12582912, 10],
  [16777216, 10],
  [26214400, 15],
  [33554432, 22],
  [44236800, 2],
  [58982400, 6],
  // we don't want to cache the bucket sizes below but not caching them
  // results in some major performance hits for models like sd-turbo.
  [67108864, 6],
  [134217728, 6],
  [167772160, 6],
]);

const bucketArr: number[] = [];

/**
 * normalize the buffer size so that it fits the 128-bits (16 bytes) alignment.
 */
const calcNormalizedBufferSize = (size: number) => Math.ceil(Number(size) / 16) * 16;

/**
 * calculate the buffer size so that it fits into buckets.
 */
const calcBucketBufferSize = (size: number) => {
  for (let idx = 0; idx < bucketArr.length; idx++) {
    const sizeForBucket = bucketArr[idx];
    if (size <= sizeForBucket) {
      return sizeForBucket;
    }
  }
  // not in bucket list -> caller will not cache, round up to 16.
  return Math.ceil(size / 16) * 16;
};

let guid = 1;
const createNewGpuDataId = () => guid++;

/**
 * exported standard download function. This function is used by the session to download the data from GPU, and also by
 * factory to create GPU tensors with the capacity of downloading data from GPU.
 *
 * @param backend - the WebGPU backend
 * @param gpuBuffer - the GPU buffer to download
 * @param originalSize - the original size of the data
 * @param getTargetBuffer - optional. If provided, the data will be copied to the target buffer. Otherwise, a new buffer
 * will be created and returned.
 */
export const downloadGpuData = async (
  backend: WebGpuBackend,
  gpuBuffer: GPUBuffer,
  originalSize: number,
  getTargetBuffer?: () => Uint8Array,
): Promise<Uint8Array> => {
  const bufferSize = calcNormalizedBufferSize(originalSize);
  const gpuReadBuffer = backend.device.createBuffer(
    // eslint-disable-next-line no-bitwise
    { size: bufferSize, usage: GPUBufferUsage.COPY_DST | GPUBufferUsage.MAP_READ },
  );
  try {
    const commandEncoder = backend.getCommandEncoder();
    backend.endComputePass();
    commandEncoder.copyBufferToBuffer(
      gpuBuffer /* source buffer */,
      0 /* source offset */,
      gpuReadBuffer /* destination buffer */,
      0 /* destination offset */,
      bufferSize /* size */,
    );
    backend.flush();

    await gpuReadBuffer.mapAsync(GPUMapMode.READ);

    const arrayBuffer = gpuReadBuffer.getMappedRange();
    if (getTargetBuffer) {
      // if we already have a CPU buffer to accept the data, no need to clone the ArrayBuffer.
      const targetBuffer = getTargetBuffer();
      targetBuffer.set(new Uint8Array(arrayBuffer, 0, originalSize));
      return targetBuffer;
    } else {
      // the mapped ArrayBuffer will be released when the GPU buffer is destroyed. Need to clone the
      // ArrayBuffer.
      return new Uint8Array(arrayBuffer.slice(0, originalSize));
    }
  } finally {
    gpuReadBuffer.destroy();
  }
};

class GpuDataManagerImpl implements GpuDataManager {
  // GPU Data ID => GPU Data ( storage buffer )
  private storageCache: Map<GpuDataId, StorageCacheValue>;

  // pending buffers for computing
  private buffersPending: GPUBuffer[];

  // The reusable storage buffers for computing.
  private freeBuffers: Map<number, GPUBuffer[]>;
  // The reusable uniform buffers
  private freeUniformBuffers: Map<number, GPUBuffer[]>;

  // The pendingBuffers for capture graph.
  // a SessionID -> GPUBuffer[] mapping.
  private capturedPendingBuffers: Map<number, GPUBuffer[]>;

  // The session count.
  private sessionCount: number;

  constructor(private backend: WebGpuBackend) {
    this.storageCache = new Map();
    this.freeBuffers = new Map();
    this.freeUniformBuffers = new Map();
    this.buffersPending = [];
    this.capturedPendingBuffers = new Map();

    for (const [key] of bucketFreelist) {
      bucketArr.push(key);
      this.freeBuffers.set(key, []);
      this.freeUniformBuffers.set(key, []);
    }

    this.sessionCount = 0;
  }

  upload(id: GpuDataId, data: Uint8Array): void {
    const srcArrayBuffer = data.buffer;
    const srcOffset = data.byteOffset;
    const srcLength = data.byteLength;
    const size = calcNormalizedBufferSize(srcLength);

    // get destination gpu buffer
    const gpuDataCache = this.storageCache.get(id);
    if (!gpuDataCache) {
      throw new Error('gpu data for uploading does not exist');
    }
    if (Number(gpuDataCache.originalSize) !== srcLength) {
      throw new Error(`inconsistent data size. gpu data size=${gpuDataCache.originalSize}, data size=${srcLength}`);
    }

    // create gpu buffer
    const gpuBufferForUploading = this.backend.device.createBuffer(
      // eslint-disable-next-line no-bitwise
      { mappedAtCreation: true, size, usage: GPUBufferUsage.MAP_WRITE | GPUBufferUsage.COPY_SRC },
    );

    // copy (upload) data
    const arrayBuffer = gpuBufferForUploading.getMappedRange();
    new Uint8Array(arrayBuffer).set(new Uint8Array(srcArrayBuffer, srcOffset, srcLength));
    gpuBufferForUploading.unmap();

    // GPU copy
    const commandEncoder = this.backend.device.createCommandEncoder();
    commandEncoder.copyBufferToBuffer(gpuBufferForUploading, 0, gpuDataCache.gpuData.buffer, 0, size);
    this.backend.device.queue.submit([commandEncoder.finish()]);
    gpuBufferForUploading.destroy();

    LOG_DEBUG('verbose', () => `[WebGPU] GpuDataManager.upload(id=${id})`);
  }

  memcpy(sourceId: GpuDataId, destinationId: GpuDataId): void {
    // get source gpu buffer
    const sourceGpuDataCache = this.storageCache.get(sourceId);
    if (!sourceGpuDataCache) {
      throw new Error('source gpu data for memcpy does not exist');
    }
    // get destination gpu buffer
    const destinationGpuDataCache = this.storageCache.get(destinationId);
    if (!destinationGpuDataCache) {
      throw new Error('destination gpu data for memcpy does not exist');
    }
    if (sourceGpuDataCache.originalSize !== destinationGpuDataCache.originalSize) {
      throw new Error('inconsistent source and destination gpu data size');
    }

    const size = calcNormalizedBufferSize(sourceGpuDataCache.originalSize);

    // GPU copy
    const commandEncoder = this.backend.getCommandEncoder();
    this.backend.endComputePass();
    commandEncoder.copyBufferToBuffer(
      sourceGpuDataCache.gpuData.buffer,
      0,
      destinationGpuDataCache.gpuData.buffer,
      0,
      size,
    );
  }

  registerExternalBuffer(buffer: GPUBuffer, originalSize: number, previous?: [GpuDataId, GPUBuffer]): number {
    let id: number | undefined;
    if (previous) {
      id = previous[0];
      if (buffer === previous[1]) {
        LOG_DEBUG(
          'verbose',
          () =>
            `[WebGPU] GpuDataManager.registerExternalBuffer(size=${originalSize}) => id=${id}, buffer is the same, skip.`,
        );
        return id;
      } else if (this.backend.capturedCommandList.has(this.backend.currentSessionId!)) {
        throw new Error(`Registering a different external buffer under graph capture mode is not supported yet.
             Please use the previous external buffer!`);
      }
    } else {
      id = createNewGpuDataId();
    }

    this.storageCache.set(id, { gpuData: { id, type: GpuDataType.default, buffer }, originalSize });
    LOG_DEBUG(
      'verbose',
      () => `[WebGPU] GpuDataManager.registerExternalBuffer(size=${originalSize}) => id=${id}, registered.`,
    );
    return id;
  }

  unregisterExternalBuffer(id: GpuDataId): void {
    if (id !== undefined) {
      this.storageCache.delete(id);
      LOG_DEBUG('verbose', () => `[WebGPU] GpuDataManager.unregisterExternalBuffer() => id=${id}`);
    }
  }

  // eslint-disable-next-line no-bitwise
  create(size: number, usage = GPUBufferUsage.STORAGE | GPUBufferUsage.COPY_SRC | GPUBufferUsage.COPY_DST): GpuData {
    const bufferSize = calcBucketBufferSize(size);

    let gpuBuffer;
    // Currently, only storage buffers are reused.
    // eslint-disable-next-line no-bitwise
    const isStorage = (usage & GPUBufferUsage.STORAGE) === GPUBufferUsage.STORAGE;
    // eslint-disable-next-line no-bitwise
    const isUniform = (usage & GPUBufferUsage.UNIFORM) === GPUBufferUsage.UNIFORM;
    if (isStorage || isUniform) {
      const freeBuffers = isStorage ? this.freeBuffers : this.freeUniformBuffers;
      const buffers = freeBuffers.get(bufferSize);
      if (!buffers) {
        // no such bucket/freelist - create gpu buffer
        gpuBuffer = this.backend.device.createBuffer({ size: bufferSize, usage });
      } else {
        if (buffers.length > 0) {
          // in freelist, use it
          gpuBuffer = buffers.pop() as GPUBuffer;
        } else {
          // bucket empty, create gpu buffer
          gpuBuffer = this.backend.device.createBuffer({ size: bufferSize, usage });
        }
      }
    } else {
      // create gpu buffer
      gpuBuffer = this.backend.device.createBuffer({ size: bufferSize, usage });
    }

    const gpuData = { id: createNewGpuDataId(), type: GpuDataType.default, buffer: gpuBuffer };
    this.storageCache.set(gpuData.id, { gpuData, originalSize: Number(size) });

    LOG_DEBUG('verbose', () => `[WebGPU] GpuDataManager.create(size=${size}) => id=${gpuData.id}`);
    return gpuData;
  }

  get(id: GpuDataId): GpuData | undefined {
    return this.storageCache.get(id)?.gpuData;
  }

  release(idInput: GpuDataId): number {
    const id = typeof idInput === 'bigint' ? Number(idInput) : idInput;
    const cachedData = this.storageCache.get(id);
    if (!cachedData) {
      if (this.storageCache.size === 0) {
        // cache was previously cleared, no need to release anything.
        return 0;
      } else {
        throw new Error('releasing data does not exist');
      }
    }

    LOG_DEBUG('verbose', () => `[WebGPU] GpuDataManager.release(id=${id}), gpuDataId=${cachedData.gpuData.id}`);

    this.storageCache.delete(id);
    this.buffersPending.push(cachedData.gpuData.buffer);
    // cachedData.gpuData.buffer.destroy();

    return cachedData.originalSize;
  }

  async download(id: GpuDataId, getTargetBuffer: () => Uint8Array): Promise<void> {
    const cachedData = this.storageCache.get(Number(id));
    if (!cachedData) {
      throw new Error('data does not exist');
    }
    await downloadGpuData(this.backend, cachedData.gpuData.buffer, cachedData.originalSize, getTargetBuffer);
  }

  refreshPendingBuffers(): void {
    if (this.buffersPending.length === 0) {
      return;
    }

    if (this.backend.sessionStatus === 'default') {
      for (const buffer of this.buffersPending) {
        const maxInFreeList = bucketFreelist.get(buffer.size);

        // eslint-disable-next-line no-bitwise
        if ((buffer.usage & GPUBufferUsage.STORAGE) === GPUBufferUsage.STORAGE) {
          // Put the pending buffer to freeBuffers list instead of really destroying it for buffer reusing.
          const freelist = this.freeBuffers.get(buffer.size) || [];
          if (maxInFreeList === undefined || freelist.length >= maxInFreeList) {
            buffer.destroy();
          } else {
            freelist.push(buffer);
          }
          // eslint-disable-next-line no-bitwise
        } else if ((buffer.usage & GPUBufferUsage.UNIFORM) === GPUBufferUsage.UNIFORM) {
          // Put the pending buffer to freeUniformBuffers list instead of really destroying it for buffer reusing.
          const freelist = this.freeUniformBuffers.get(buffer.size) || [];
          if (maxInFreeList === undefined || freelist.length >= maxInFreeList) {
            buffer.destroy();
          } else {
            freelist.push(buffer);
          }
        } else {
          buffer.destroy();
        }
      }
      this.buffersPending = [];
    } else {
      // Don't release intermediate tensors in non-default mode.
      // TODO: reuse the storage buffers in non-default mode.
      let capturedBuffers = this.capturedPendingBuffers.get(this.backend.currentSessionId!);
      if (!capturedBuffers) {
        capturedBuffers = [];
        this.capturedPendingBuffers.set(this.backend.currentSessionId!, capturedBuffers);
      }
      for (const buffer of this.buffersPending) {
        capturedBuffers.push(buffer);
      }
      this.buffersPending = [];
    }
  }

  dispose() {
    this.freeBuffers.forEach((buffers) => {
      buffers.forEach((buffer) => {
        buffer.destroy();
      });
    });
    this.freeUniformBuffers.forEach((buffers) => {
      buffers.forEach((buffer) => {
        buffer.destroy();
      });
    });

    this.storageCache.forEach((storage) => {
      storage.gpuData.buffer.destroy();
    });

    this.capturedPendingBuffers.forEach((buffers) => {
      buffers.forEach((buffer) => {
        buffer.destroy();
      });
    });
    this.storageCache = new Map();
    this.freeBuffers = new Map();
    this.freeUniformBuffers = new Map();
    this.capturedPendingBuffers = new Map();
  }

  onCreateSession() {
    this.sessionCount += 1;
  }

  onReleaseSession(sessionId: number) {
    // release the captured pending buffers.
    const pendingBuffers = this.capturedPendingBuffers.get(sessionId);
    if (pendingBuffers) {
      pendingBuffers.forEach((buffer) => {
        buffer.destroy();
      });
      this.capturedPendingBuffers.delete(sessionId);
    }

    // release the storage cache if no active sessions.
    this.sessionCount -= 1;
    if (this.sessionCount === 0) {
      LOG_DEBUG('warning', () => '[WebGPU] Clearing webgpu buffer cache');
      this.storageCache.forEach((storage) => {
        storage.gpuData.buffer.destroy();
      });
      this.storageCache = new Map();
    }
  }
}

export const createGpuDataManager = (...args: ConstructorParameters<typeof GpuDataManagerImpl>): GpuDataManager =>
  new GpuDataManagerImpl(...args);
