// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { SessionHandler } from './backend';
import { Graph } from './graph';
import { Logger, Profiler } from './instrument';
import { Operator } from './operators';
import { Tensor } from './tensor';

class KernelOp {
  constructor(
    public op: Operator,
    public node: Graph.Node,
  ) {}
}

export class ExecutionPlan {
  constructor(
    private graph: Graph,
    ops: Operator[],
    private profiler: Readonly<Profiler>,
  ) {
    this.initialize(ops);
  }

  initialize(ops: Operator[]) {
    this.profiler.event('session', 'ExecutionPlan.initialize', () => {
      const graphNodes = this.graph.getNodes();
      if (graphNodes.length !== ops.length) {
        throw new Error('The size of nodes and OPs do not match.');
      }

      this._ops = ops.map((op, i) => new KernelOp(op, graphNodes[i]));
      this.reset();

      // look for starter node(s)
      this._starter = [];
      this._ops.forEach((op, i) => {
        let resolved = true;
        for (const input of op.node.inputs) {
          if (
            !this._values[input] && // not an initialized input
            this.graph.getInputIndices().indexOf(input) === -1 // not model input
          ) {
            resolved = false;
            break;
          }
        }
        if (resolved) {
          this._starter.push(i);
        }
      });
    });
  }

  reset() {
    this._values = this.graph.getValues().map((i) => i.tensor);
  }

  async execute(sessionHandler: SessionHandler, modelInputs: Tensor[]): Promise<Tensor[]> {
    return this.profiler.event('session', 'ExecutionPlan.execute', async () => {
      // reset mediem result
      this.reset();

      // create inference handler
      const inferenceHandler = sessionHandler.createInferenceHandler();

      // populate inputs value
      const graphInputs = this.graph.getInputIndices();
      if (modelInputs.length !== graphInputs.length) {
        throw new Error(
          `number of input tensors don't match the number of inputs to the model: actual: ${
            modelInputs.length
          } expected: ${graphInputs.length}`,
        );
      }

      modelInputs.forEach((input, i) => {
        const index = graphInputs[i];
        this._values[index] = input;
      });

      // prepare running sequence
      const sequence: number[] = this._starter.slice(0);

      // execution iterations
      const graphValues = this.graph.getValues();
      const graphNodes = this.graph.getNodes();

      let rear = 0;
      while (rear < sequence.length) {
        const thisOpIndex = sequence[rear++];
        const thisOp = this._ops[thisOpIndex];

        // check input
        const inputList = thisOp.node.inputs.map((i) => this._values[i]);
        if (inputList.indexOf(undefined) !== -1) {
          throw new Error(`unresolved input detected: op: ${thisOp.node}`);
        }

        // run
        const inputTensors = inputList as Tensor[];
        Logger.verbose(
          'ExecPlan',
          `Running op:${thisOp.node.name} (${inputTensors
            .map((t, i) => `'${thisOp.node.inputs[i]}': ${t.type}[${t.dims.join(',')}]`)
            .join(', ')})`,
        );

        const outputList = await this.profiler.event('node', thisOp.node.name, async () =>
          thisOp.op.impl(inferenceHandler, inputTensors, thisOp.op.context),
        );

        // check output
        if (outputList.length !== thisOp.node.outputs.length) {
          throw new Error('the size of output does not match model definition.');
        }

        // fill value
        outputList.forEach((output, i) => {
          const j = thisOp.node.outputs[i];
          if (this._values[j]) {
            throw new Error(`output [${j}] already has value: op:${thisOp.node.name}`);
          }
          this._values[j] = output;
        });

        // resolve downstream nodes
        const downstreamNodes = new Set<number>();
        outputList.forEach((_output, i) => {
          const j = thisOp.node.outputs[i];
          for (const currentDownstreamNodeIndex of graphValues[j].to) {
            const currentDownstreamNode = graphNodes[currentDownstreamNodeIndex];
            let resolved = true;
            for (const k of currentDownstreamNode.inputs) {
              if (!this._values[k]) {
                resolved = false;
                break;
              }
            }
            if (resolved) {
              downstreamNodes.add(currentDownstreamNodeIndex);
            }
          }
        });
        sequence.push(...downstreamNodes);
      }

      const output: Tensor[] = [];
      for (let i = 0; i < this.graph.getOutputIndices().length; i++) {
        const outputIndex = this.graph.getOutputIndices()[i];
        const outputTensor = this._values[outputIndex];
        if (outputTensor === undefined) {
          throw new Error(`required output [${outputIndex}] does not have value`);
        }
        if (outputIndex === 0) {
          await outputTensor.getData();
        } else {
          // eslint-disable-next-line no-unused-expressions
          outputTensor.data;
        }
        output.push(outputTensor);
      }
      Logger.verbose('ExecPlan', 'disposing of inferenceHandler');
      inferenceHandler.dispose();
      return output;
    });
  }

  _values: Array<Tensor | undefined>;
  _ops: KernelOp[];
  _starter: number[];
}
