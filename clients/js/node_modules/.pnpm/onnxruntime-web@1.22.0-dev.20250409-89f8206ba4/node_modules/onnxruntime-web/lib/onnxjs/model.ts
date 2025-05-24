// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import * as flatbuffers from 'flatbuffers';

import { Graph } from './graph';
import { OpSet } from './opset';
import * as ortFbs from './ort-schema/flatbuffers/ort-generated';
import { onnx } from './ort-schema/protobuf/onnx';
import { LongUtil } from './util';

export class Model {
  // empty model
  constructor() {}

  load(buf: Uint8Array, graphInitializer?: Graph.Initializer, isOrtFormat?: boolean): void {
    let onnxError: Error | undefined;
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

  private loadFromOnnxFormat(buf: Uint8Array, graphInitializer?: Graph.Initializer): void {
    const modelProto = onnx.ModelProto.decode(buf);
    const irVersion = LongUtil.longToNumber(modelProto.irVersion);
    if (irVersion < 3) {
      throw new Error('only support ONNX model with IR_VERSION>=3');
    }

    this._opsets = modelProto.opsetImport.map((i) => ({
      domain: i.domain as string,
      version: LongUtil.longToNumber(i.version!),
    }));

    this._graph = Graph.from(modelProto.graph!, graphInitializer);
  }

  private loadFromOrtFormat(buf: Uint8Array, graphInitializer?: Graph.Initializer): void {
    const fb = new flatbuffers.ByteBuffer(buf);
    const ortModel = ortFbs.InferenceSession.getRootAsInferenceSession(fb).model()!;
    const irVersion = LongUtil.longToNumber(ortModel.irVersion());
    if (irVersion < 3) {
      throw new Error('only support ONNX model with IR_VERSION>=3');
    }
    this._opsets = [];
    for (let i = 0; i < ortModel.opsetImportLength(); i++) {
      const opsetId = ortModel.opsetImport(i)!;
      this._opsets.push({ domain: opsetId?.domain() as string, version: LongUtil.longToNumber(opsetId.version()!) });
    }

    this._graph = Graph.from(ortModel.graph()!, graphInitializer);
  }

  private _graph: Graph;
  get graph(): Graph {
    return this._graph;
  }

  private _opsets: OpSet[];
  get opsets(): readonly OpSet[] {
    return this._opsets;
  }
}
