'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
var __createBinding =
  (this && this.__createBinding) ||
  (Object.create
    ? function (o, m, k, k2) {
        if (k2 === undefined) k2 = k;
        var desc = Object.getOwnPropertyDescriptor(m, k);
        if (!desc || ('get' in desc ? !m.__esModule : desc.writable || desc.configurable)) {
          desc = {
            enumerable: true,
            get: function () {
              return m[k];
            },
          };
        }
        Object.defineProperty(o, k2, desc);
      }
    : function (o, m, k, k2) {
        if (k2 === undefined) k2 = k;
        o[k2] = m[k];
      });
var __setModuleDefault =
  (this && this.__setModuleDefault) ||
  (Object.create
    ? function (o, v) {
        Object.defineProperty(o, 'default', { enumerable: true, value: v });
      }
    : function (o, v) {
        o['default'] = v;
      });
var __importStar =
  (this && this.__importStar) ||
  function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null)
      for (var k in mod)
        if (k !== 'default' && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
  };
Object.defineProperty(exports, '__esModule', { value: true });
exports.Model = void 0;
const flatbuffers = __importStar(require('flatbuffers'));
const graph_1 = require('./graph');
const ortFbs = __importStar(require('./ort-schema/flatbuffers/ort-generated'));
const onnx_1 = require('./ort-schema/protobuf/onnx');
const util_1 = require('./util');
class Model {
  // empty model
  constructor() {}
  load(buf, graphInitializer, isOrtFormat) {
    let onnxError;
    if (!isOrtFormat) {
      // isOrtFormat === false || isOrtFormat === undefined
      try {
        this.loadFromOnnxFormat(buf, graphInitializer);
        return;
      } catch (e) {
        if (isOrtFormat !== undefined) {
          throw e;
        }
        onnxError = e;
      }
    }
    try {
      this.loadFromOrtFormat(buf, graphInitializer);
    } catch (e) {
      if (isOrtFormat !== undefined) {
        throw e;
      }
      // Tried both formats and failed (when isOrtFormat === undefined)
      throw new Error(`Failed to load model as ONNX format: ${onnxError}\nas ORT format: ${e}`);
    }
  }
  loadFromOnnxFormat(buf, graphInitializer) {
    const modelProto = onnx_1.onnx.ModelProto.decode(buf);
    const irVersion = util_1.LongUtil.longToNumber(modelProto.irVersion);
    if (irVersion < 3) {
      throw new Error('only support ONNX model with IR_VERSION>=3');
    }
    this._opsets = modelProto.opsetImport.map((i) => ({
      domain: i.domain,
      version: util_1.LongUtil.longToNumber(i.version),
    }));
    this._graph = graph_1.Graph.from(modelProto.graph, graphInitializer);
  }
  loadFromOrtFormat(buf, graphInitializer) {
    const fb = new flatbuffers.ByteBuffer(buf);
    const ortModel = ortFbs.InferenceSession.getRootAsInferenceSession(fb).model();
    const irVersion = util_1.LongUtil.longToNumber(ortModel.irVersion());
    if (irVersion < 3) {
      throw new Error('only support ONNX model with IR_VERSION>=3');
    }
    this._opsets = [];
    for (let i = 0; i < ortModel.opsetImportLength(); i++) {
      const opsetId = ortModel.opsetImport(i);
      this._opsets.push({ domain: opsetId?.domain(), version: util_1.LongUtil.longToNumber(opsetId.version()) });
    }
    this._graph = graph_1.Graph.from(ortModel.graph(), graphInitializer);
  }
  get graph() {
    return this._graph;
  }
  get opsets() {
    return this._opsets;
  }
}
exports.Model = Model;
//# sourceMappingURL=model.js.map
