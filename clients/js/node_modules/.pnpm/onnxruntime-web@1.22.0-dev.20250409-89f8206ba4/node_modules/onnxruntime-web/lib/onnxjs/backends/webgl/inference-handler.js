'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.WebGLInferenceHandler = void 0;
const instrument_1 = require('../../instrument');
const tensor_1 = require('../../tensor');
const util_1 = require('../../util');
const pack_1 = require('./ops/pack');
const reshape_packed_1 = require('./ops/reshape-packed');
const uint8_encode_1 = require('./ops/uint8-encode');
const unpack_1 = require('./ops/unpack');
const texture_layout_1 = require('./texture-layout');
const types_1 = require('./types');
const getProgramInfoUniqueKey = (programInfo, inputTextureDatas) => {
  const inputs = inputTextureDatas
    .map((texture) => `${texture.unpackedShape.join(',')};${texture.width}x${texture.height}`)
    .join('_');
  let key = programInfo.name;
  if (programInfo.cacheHint) {
    key += '[' + programInfo.cacheHint + ']';
  }
  key += ':' + inputs;
  return key;
};
class WebGLInferenceHandler {
  constructor(session) {
    this.session = session;
    this.packedTextureDataCache = new Map();
    this.unpackedTextureDataCache = new Map();
  }
  /**
   * @returns [width, height]
   */
  calculateTextureWidthAndHeight(shape, textureType) {
    return (0, texture_layout_1.calculateTextureWidthAndHeight)(this.session.layoutStrategy, shape, textureType);
  }
  executeProgram(program, inputs) {
    if (inputs.length < program.inputNames.length) {
      throw new Error(`Input size mustn't be less than ${program.inputNames.length}.`);
    }
    if (program.inputNames.length !== program.inputTypes.length) {
      throw new Error('input names size does not match input types');
    }
    // create texture info for input
    const inputTextureDatas = [];
    for (let i = 0; i < program.inputNames.length; ++i) {
      inputTextureDatas[i] = this.getOrCreateTextureData(inputs[i], program.inputTypes[i]);
    }
    const key = getProgramInfoUniqueKey(program, inputTextureDatas);
    let artifact = this.session.programManager.getArtifact(key);
    const programInfo = artifact ? artifact.programInfo : typeof program.get === 'function' ? program.get() : program;
    // create texture info for output
    const outputTextureLayout = (0, texture_layout_1.createTextureLayoutFromTextureType)(
      this.session.layoutStrategy,
      programInfo.output.dims,
      programInfo.output.textureType,
    );
    const outputTextureData = this.createTextureData(outputTextureLayout, programInfo.output.type);
    if (!artifact) {
      artifact = this.session.programManager.build(programInfo, inputTextureDatas, outputTextureData);
      this.session.programManager.setArtifact(key, artifact);
    }
    this.runProgram(artifact, inputTextureDatas, outputTextureData);
    return outputTextureData;
  }
  run(program, inputs) {
    const outputTextureData = this.executeProgram(program, inputs);
    return outputTextureData.tensor;
  }
  runProgram(artifact, inputs, output) {
    // input should match
    for (let i = 0; i < inputs.length; ++i) {
      if (!!inputs[i].isPacked !== (artifact.programInfo.inputTypes[i] === types_1.TextureType.packed)) {
        throw new Error(`input[${i}] property packed inconsistent`);
      }
    }
    // output should match
    if (!!output.isPacked !== (artifact.programInfo.output.textureType === types_1.TextureType.packed)) {
      throw new Error('output property packed inconsistent');
    }
    this.session.programManager.run(artifact, inputs, output);
  }
  /**
   * Create a TextureData object from a tensor.
   * Usage = EncoderUsage.UploadOnly.
   * If a related texture data is found in cache, returns it;
   * Otherwise:
   *   Creates a new texture layout if not provided;
   *   Creates WebGLTexture with the layout;
   *   Upload tensor data to the texture;
   *   Creates a texture data object associated with the given tensor.
   * @param tensor the tensor with data to upload
   */
  getOrCreateTextureData(tensor, textureType) {
    let td = this.getTextureData(tensor.dataId, textureType === types_1.TextureType.packed);
    if (!td) {
      // check if we have texture data in different type
      td = this.getTextureData(tensor.dataId, textureType !== types_1.TextureType.packed);
      if (td) {
        if (textureType === types_1.TextureType.packed) {
          return this.pack(td);
        } else {
          return this.unpack(td);
        }
      }
    }
    if (!td) {
      const layout = (0, texture_layout_1.createTextureLayoutFromTextureType)(
        this.session.layoutStrategy,
        tensor.dims,
        textureType,
      );
      if (textureType === types_1.TextureType.packedLastDimension) {
        const group = 1;
        const channels = 4;
        const shape = tensor.dims;
        if (shape.length === 4) {
          // pre-processing for kernel data of Conv.
          //
          // TODO: currently this is a hacking to overwrite Conv's weight. The correct way to do this should be:
          // 1. implement texture based const-folding
          // 2. create a WebGL program "preprocessConvWeight" to do the same work as below
          // 3. run the program before dotProduct.
          //
          const adjustedKernelShape = [shape[0], Math.ceil((shape[1] * shape[2] * shape[3]) / channels)];
          const adjustedLayout = (0, texture_layout_1.createTextureLayoutFromTextureType)(
            this.session.layoutStrategy,
            adjustedKernelShape,
            textureType,
          );
          let buffer = tensor.numberData;
          if ((shape[1] * shape[2] * shape[3]) % channels !== 0) {
            const numFeatureMaps = shape[0];
            const oldRowSize = shape[1] * shape[2] * shape[3];
            const newRowSize = Math.ceil((oldRowSize * group) / channels) * channels;
            const newSize = numFeatureMaps * newRowSize;
            buffer = new Float32Array(newSize);
            for (let f = 0; f < numFeatureMaps; ++f) {
              const oldOffset = f * oldRowSize;
              const newOffset = f * newRowSize + (f % group) * oldRowSize;
              buffer.set(tensor.numberData.subarray(oldOffset, oldOffset + oldRowSize), newOffset);
            }
          }
          return this.createTextureData(adjustedLayout, tensor.type, buffer, tensor, 1 /* EncoderUsage.UploadOnly */);
        }
      }
      if (textureType === types_1.TextureType.packed) {
        const unpackedTextureLayout = (0, texture_layout_1.createTextureLayoutFromShape)(
          this.session.layoutStrategy,
          tensor.dims,
          1,
          [],
          {
            reverseWH: true,
          },
        );
        const unpackedTextureData = this.createTextureData(
          unpackedTextureLayout,
          tensor.type,
          tensor.numberData,
          tensor,
          1 /* EncoderUsage.UploadOnly */,
        );
        td = this.pack(unpackedTextureData);
      } else {
        td = this.createTextureData(layout, tensor.type, tensor.numberData, tensor, 1 /* EncoderUsage.UploadOnly */);
      }
    }
    return td;
  }
  /**
   * Create a TextureData object using the given data and bind to the given tensor.
   * Usage = EncoderUsage.UploadOnly.
   * NOTE: this function is a hack for Conv implementation. should remove this function, after rewriting Conv
   * implementation by Graph.Transformer
   * @param dataType the tensor data type
   * @param data the actual data to upload
   * @param tensor the tensor to bind. tensor's data is ignored.
   */
  createTextureDataFromLayoutBindTensor(layout, dataType, data, tensor) {
    return this.createTextureData(layout, dataType, data, tensor, 1 /* EncoderUsage.UploadOnly */);
  }
  createTextureData(layout, dataType, data, tensor, usage) {
    instrument_1.Logger.verbose('InferenceHandler', `Creating TextureData: layout:[${JSON.stringify(layout)}]`);
    const texture = this.session.textureManager.createTextureFromLayout(dataType, layout, data, usage);
    return this.createTextureDataFromTexture(layout, dataType, texture, tensor);
  }
  reshapeUnpacked(input, reshapedDims) {
    const inputTD = this.getOrCreateTextureData(input, types_1.TextureType.unpacked);
    const newTextureLayout = {
      channels: inputTD.channels,
      height: inputTD.height,
      width: inputTD.width,
      // handle reshaping into scalar Tensors
      shape: reshapedDims.length !== 0 ? reshapedDims : [1],
      strides: util_1.ShapeUtil.computeStrides(reshapedDims),
      unpackedShape: reshapedDims,
    };
    const newTextureData = this.createTextureDataFromTexture(newTextureLayout, input.type, inputTD.texture);
    return newTextureData.tensor;
  }
  reshapePacked(input, reshapedDims) {
    const inputTD = this.getOrCreateTextureData(input, types_1.TextureType.packed);
    // check if the reshape is 'cheap'
    if ((0, reshape_packed_1.isReshapeCheap)(input.dims, reshapedDims)) {
      const newTextureLayout = {
        channels: inputTD.channels,
        height: inputTD.height,
        width: inputTD.width,
        // handle reshaping into scalar Tensors
        shape: reshapedDims.length !== 0 ? reshapedDims : [1],
        strides: util_1.ShapeUtil.computeStrides(reshapedDims),
        unpackedShape: reshapedDims,
        isPacked: true,
      };
      const newTextureData = this.createTextureDataFromTexture(newTextureLayout, input.type, inputTD.texture);
      return newTextureData.tensor;
    }
    const squeezedInputShape = (0, reshape_packed_1.processDims3D)(input.dims);
    const squeezedOutputShape = (0, reshape_packed_1.processDims3D)(reshapedDims);
    const squeezedInputTensor = this.reshapePacked(input, squeezedInputShape);
    const squeezedOutputTensor = this.run(
      (0, reshape_packed_1.createPackedReshape3DProgramInfoLoader)(this, squeezedInputTensor, squeezedOutputShape),
      [squeezedInputTensor],
    );
    const outputTensor = this.reshapePacked(squeezedOutputTensor, reshapedDims);
    return outputTensor;
  }
  cast(input, type) {
    const inputTD = this.getOrCreateTextureData(input, types_1.TextureType.unpacked);
    const newTextureData = this.createTextureDataFromTexture(inputTD, type, inputTD.texture);
    return newTextureData.tensor;
  }
  createTextureDataFromTexture(layout, dataType, texture, tensor, tensorId) {
    const textureData = {
      ...layout,
      tensor:
        tensor ||
        new tensor_1.Tensor(
          layout.unpackedShape,
          dataType,
          (_id) => this.readTexture(textureData),
          async (_id) => this.readTextureAsync(textureData),
          undefined,
          tensorId,
        ),
      texture,
    };
    this.setTextureData(textureData.tensor.dataId, textureData, layout.isPacked);
    return textureData;
  }
  getTextureData(tensorId, isPacked = false) {
    return this.session.isInitializer(tensorId)
      ? this.session.getTextureData(tensorId, isPacked)
      : isPacked
        ? this.packedTextureDataCache.get(tensorId)
        : this.unpackedTextureDataCache.get(tensorId);
  }
  setTextureData(tensorId, td, isPacked = false) {
    if (this.session.isInitializer(tensorId)) {
      this.session.setTextureData(tensorId, td, isPacked);
    } else {
      (isPacked ? this.packedTextureDataCache : this.unpackedTextureDataCache).set(tensorId, td);
    }
  }
  isTextureLayoutCached(tensor, isPacked = false) {
    return !!this.getTextureData(tensor.dataId, isPacked);
  }
  dispose() {
    this.session.textureManager.clearActiveTextures();
    this.packedTextureDataCache.forEach((td) => this.session.textureManager.releaseTexture(td));
    this.packedTextureDataCache = new Map();
    this.unpackedTextureDataCache.forEach((td) => this.session.textureManager.releaseTexture(td));
    this.unpackedTextureDataCache = new Map();
  }
  readTexture(textureData) {
    if (textureData.isPacked) {
      return this.readTexture(this.unpack(textureData));
    }
    if (!this.session.backend.glContext.isFloat32DownloadSupported) {
      return this.session.textureManager.readUint8TextureAsFloat((0, uint8_encode_1.encodeAsUint8)(this, textureData));
    }
    return this.session.textureManager.readTexture(textureData, textureData.tensor.type, textureData.channels);
  }
  async readTextureAsync(textureData) {
    if (textureData.isPacked) {
      return this.readTextureAsync(this.unpack(textureData));
    }
    if (!this.session.backend.glContext.isFloat32DownloadSupported) {
      return this.session.textureManager.readUint8TextureAsFloat((0, uint8_encode_1.encodeAsUint8)(this, textureData));
    }
    return this.session.textureManager.readTextureAsync(textureData, textureData.tensor.type, textureData.channels);
  }
  pack(input) {
    const outputTextureData = this.executeProgram((0, pack_1.createPackProgramInfoLoader)(this, input.tensor), [
      input.tensor,
    ]);
    return outputTextureData;
  }
  unpack(input) {
    const outputTextureData = this.executeProgram((0, unpack_1.createUnpackProgramInfoLoader)(this, input.tensor), [
      input.tensor,
    ]);
    return outputTextureData;
  }
}
exports.WebGLInferenceHandler = WebGLInferenceHandler;
//# sourceMappingURL=inference-handler.js.map
