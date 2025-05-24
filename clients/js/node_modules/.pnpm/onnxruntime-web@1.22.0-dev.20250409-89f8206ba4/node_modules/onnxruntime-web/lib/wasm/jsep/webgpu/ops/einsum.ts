// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { ShapeUtil } from '../../util';
import { AttributeWithCacheKey, createAttributeWithCacheKey } from '../attribute-with-cache-key';
import { ComputeContext, ProgramInfo, ProgramUniform } from '../types';

import { createTensorShapeVariables, inputVariable, outputVariable, ShaderHelper } from './common';

export interface EinsumAttributes extends AttributeWithCacheKey {
  readonly equation: string;
}
// The equation attribute value is a string which consists of left hand side (LHS) and optionally right hand side (RHS)
// separated by '->'. Ex. "ij,jk -> ik" expresses matrix multiplication
//     "ij->ji" expresses matrix transpose
//      "ii->i" diagonal elements of a square matrix
// LHS consists of a sequence of terms separated by commas. Each term corresponds to an input variable.
// Each symbol corresponds to a dimension in the input variable. The symbol can be either a letter, 'a' to 'z' or 'A' to
// 'Z' or '...' to represent arbitrary dimensions.

const symbolPattern = '[a-zA-Z]|\\.\\.\\.'; // The pattern each symbol in each term in the symbolic equation should match
const termPattern = '(' + symbolPattern + ')+'; // The pattern each term in the symbolic equation should match
const termPatternOnly = '^' + termPattern + '$'; // The patterns only matchs a term begin to end.
const lhsPattern = '(' + termPattern + ',)*' + termPattern; // The pattern the LHS should match
const lhsPatternOnly = '^' + lhsPattern + '$'; // The patterns only matchs a LHS begin to end.

interface SymbolInfo {
  count: number; // Symbol corresponding to a dimmension of an input
  inputIndices: number[]; // Number of input variables the symbol corresponds to
  dimValue: number; // Number of dimensions the symbol corresponds to
}

class EinsumTerm {
  constructor(inputIndex = -1) {
    this.symbolToIndices = new Map<string, number[]>();
    this.inputIndex = inputIndex;
  }

  // Add a symbol to the term
  addSymbol(symbol: string, index: number) {
    let value = this.symbolToIndices.get(symbol);
    if (value === undefined) {
      value = [index];
    } else {
      value.push(index);
    }
    this.symbolToIndices.set(symbol, value);
  }

  symbolToIndices: Map<string, number[]>; // Map from symbol to dimensions of the input corresponding to the term
  inputIndex: number; // -1 for output and 0, 1, 2, ... for inputs
}

class EinsumEquation {
  constructor(
    inputs: readonly TensorView[],
    public readonly equation: string,
  ) {
    this.hasEllipsis = false;
    this.symbolToInfo = new Map<string, SymbolInfo>();
    this.lhs = new Array<EinsumTerm>();
    this.outputDims = [];
    // As rhs needs to be updated allow using let instead of const for both lhs and rhs.
    // eslint-disable-next-line prefer-const
    let [lhs, rhs] = equation.includes('->') ? equation.split('->', 2) : [equation, ''];
    if (!lhs.match(RegExp(lhsPatternOnly))) {
      throw new Error('Invalid LHS term');
    }
    const inputTerms = lhs.split(',');
    inputTerms.forEach((inputTerm, index) => {
      const dims = inputs[index].dims.slice();
      if (!inputTerm.match(RegExp(termPatternOnly))) {
        throw new Error('Invalid LHS term');
      }
      const einsumTerm = this.processTerm(inputTerm, true, dims, index);
      this.lhs.push(einsumTerm);
    });

    // Initialize the RHS if not specified
    if (rhs === '') {
      // Construct RHS from LHS terms/symbols
      rhs += [...this.symbolToInfo.entries()]
        .filter(([sym, info]) => info.count === 1 || sym === '...')
        .map(([sym]) => sym)
        .join('');
    } else {
      if (!rhs.match(RegExp(termPattern))) {
        throw new Error('Invalid RHS');
      }
    }

    // Compute output dims
    const rhsSymbols = rhs.match(RegExp(symbolPattern, 'g'));
    rhsSymbols?.forEach((symbol) => {
      if (symbol === '...') {
        this.outputDims = this.outputDims.concat(this.ellipsisDims);
      } else {
        const info = this.symbolToInfo.get(symbol);
        if (info === undefined) {
          throw new Error('Invalid RHS symbol');
        }
        this.outputDims.push(info.dimValue);
      }
    });
    this.rhs = this.processTerm(rhs, false, this.outputDims);
  } // End of EinsumEqation constructor

  // Add a symbol to the equation
  addSymbol(symbol: string, dimValue: number, inputIndex: number) {
    let info = this.symbolToInfo.get(symbol);
    if (info !== undefined) {
      if (info.dimValue !== dimValue && info.count !== 1) {
        throw new Error('Dimension mismatch');
      } else {
        info.count++;
        info.inputIndices.push(inputIndex);
      }
    } else {
      info = { count: 1, dimValue, inputIndices: [inputIndex] };
    }
    this.symbolToInfo.set(symbol, info);
  }

  // Process one input/output term
  processTerm(term: string, isInput: boolean, dims: readonly number[], index = -1): EinsumTerm {
    const rank = dims.length;
    let ellipsis = false;
    let ellipsisDims = [];
    let nextDim = 0;
    // For output empty string is allowed because the output may be reduced to a scalar value
    if (!term.match(RegExp(termPatternOnly)) && !isInput && term !== '') {
      throw new Error('Invalid LHS term');
    }
    const indexSymbols = term.match(RegExp(symbolPattern, 'g'));
    const einsumTerm = new EinsumTerm(index);
    // symbol can be either a lettre, 'a' to 'z' or 'A' to 'Z', or '...'
    indexSymbols?.forEach((symbol: string, i: number) => {
      if (symbol === '...') {
        if (ellipsis) {
          throw new Error('Only one ellipsis is allowed per input term');
        }
        ellipsis = true;
        const ellipsisDimLength = rank - indexSymbols.length + 1;
        if (ellipsisDimLength < 0) {
          throw new Error('Ellipsis out of bounds');
        }
        ellipsisDims = dims.slice(nextDim, nextDim + ellipsisDimLength);
        if (this.hasEllipsis) {
          if (
            this.ellipsisDims.length !== ellipsisDims.length ||
            this.ellipsisDims.toString() !== ellipsisDims.toString()
          ) {
            throw new Error('Ellipsis dimensions mismatch');
          }
        } else if (isInput) {
          this.hasEllipsis = true;
          this.ellipsisDims = ellipsisDims;
        } else {
          throw new Error('Ellipsis must be specified in the LHS');
        }
        // Add '0', '1', '2', '3', '4', etc to represent ellipsis dimensions to avoid special handling
        for (let j = 0; j < ellipsisDims.length; j++) {
          const symbol = String.fromCharCode('0'.charCodeAt(0) + j);
          einsumTerm.addSymbol(symbol, i + j);
          this.addSymbol(symbol, dims[nextDim++], index);
        }
      } else {
        einsumTerm.addSymbol(symbol, i + (this.hasEllipsis ? this.ellipsisDims.length - 1 : 0));
        this.addSymbol(symbol, dims[nextDim++], index);
      }
    });
    return einsumTerm;
  }

  symbolToInfo: Map<string, SymbolInfo>; // All symbols in the equation
  hasEllipsis: boolean; // The equation has ellipsis or not
  ellipsisDims: number[]; // The dimensions of the equation ellipsis corresponds to.
  lhs: EinsumTerm[]; // Terms on the left-hand side of the equation
  rhs: EinsumTerm; // Term on the right-hand side of the equation
  outputDims: number[]; // Output dimensions of the equation
} // End of class EinsumEquation

const appendMax = (name: string): string => name + '_max';

const createEinsumProgramInfo = (
  inputShapes: Array<readonly number[]>,
  dataType: number,
  einsumEquation: EinsumEquation,
  outputShape: readonly number[],
): ProgramInfo => {
  const ranks = inputShapes.map((dims) => dims.length);
  const inputVars = ranks.map((rank, index) => inputVariable(`input${index}`, dataType, rank));
  const outputSize = ShapeUtil.size(outputShape);
  const output = outputVariable('output', dataType, outputShape.length);
  const uniformsSymbols = [...einsumEquation.symbolToInfo.keys()].filter(
    (symbol) => !einsumEquation.rhs.symbolToIndices.has(symbol),
  );
  const getShaderSource = (shaderHelper: ShaderHelper) => {
    const idxCopy: string[] = [];
    const initProd = 'var prod = 1.0;';
    const initSum = 'var sum = 0.0;';
    const updateSum = 'sum += prod;';
    const reduceOpsSetIndices: string[] = [];
    const reduceOpsLoopHeaders: string[] = [];
    const reduceOpsLoopFooters: string[] = [];
    const reduceOpCompute: string[] = [];
    const isReduceOpsWithoutLoop = einsumEquation.symbolToInfo.size === einsumEquation.rhs.symbolToIndices.size;
    einsumEquation.symbolToInfo.forEach((info, symbol) => {
      if (einsumEquation.rhs.symbolToIndices.has(symbol)) {
        const outputIndex = einsumEquation.rhs.symbolToIndices.get(symbol)?.[0];
        if (outputIndex !== undefined) {
          einsumEquation.lhs.forEach((term, i) => {
            if (info.inputIndices.includes(i)) {
              const indices = term.symbolToIndices.get(symbol);
              if (indices === undefined) {
                throw new Error('Invalid symbol error');
              }
              indices.forEach((index) => {
                idxCopy.push(
                  `${inputVars[i].indicesSet(
                    `input${i}Indices`,
                    index,
                    output.indicesGet('outputIndices', outputIndex),
                  )}`,
                );
              });
            }
          });
        }
      } else {
        einsumEquation.lhs.forEach((term, i) => {
          if (info.inputIndices.includes(i)) {
            const indices = term.symbolToIndices.get(symbol);
            if (indices === undefined) {
              throw new Error('Invalid symbol error');
            }
            indices.forEach((index) => {
              reduceOpsSetIndices.push(`${inputVars[i].indicesSet(`input${i}Indices`, index, `${symbol}`)}`);
            });
            reduceOpCompute.push(`prod *= ${inputVars[i].getByIndices(`input${i}Indices`)};`);
          }
        });
        reduceOpsLoopHeaders.push(
          `for(var ${symbol}: u32 = 0; ${symbol} < uniforms.${appendMax(symbol)}; ${symbol}++) {`,
        );
        reduceOpsLoopFooters.push('}');
      }
    });
    const reduceOps = isReduceOpsWithoutLoop
      ? [
          ...idxCopy,
          `let sum = ${inputVars.map((inputVar, i) => inputVar.getByIndices(`input${i}Indices`)).join(' * ')};`,
        ]
      : [
          ...idxCopy,
          initSum,
          ...reduceOpsLoopHeaders,
          ...reduceOpsSetIndices,
          initProd,
          ...reduceOpCompute,
          updateSum,
          ...reduceOpsLoopFooters,
        ];
    return `
            ${shaderHelper
              .registerUniforms(uniformsSymbols.map((symbol) => ({ name: `${appendMax(symbol)}`, type: 'u32' })))
              .registerUniform('outputSize', 'u32')
              .declareVariables(...inputVars, output)}

            ${shaderHelper.mainStart()}
            ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.outputSize')}
            var outputIndices = ${output.offsetToIndices('global_idx')};
            ${inputVars.map((_var, i) => `var input${i}Indices: ${inputVars[i].type.indices};`).join('\n')}
            ${reduceOps.join('\n')};
            ${output.setByOffset('global_idx', 'sum')};
          }`;
  };
  return {
    name: 'Einsum',
    shaderCache: { hint: einsumEquation.equation, inputDependencies: inputShapes.map(() => 'rank') },
    getRunData: () => {
      // The symbols from uniformSymbols array are guaranteed to exist in einsumEquations.symbolToInfo map. The
      // filter is added to make sure that dimValue is never 0.
      const programUniformsInit: ProgramUniform[] = uniformsSymbols
        .filter((symbol) => einsumEquation.symbolToInfo.has(symbol))
        .map((symbol) => ({ type: DataType.uint32, data: einsumEquation.symbolToInfo.get(symbol)?.dimValue || 0 }));
      programUniformsInit.push({ type: DataType.uint32, data: outputSize });
      const programUniforms: ProgramUniform[] = inputShapes
        .map((dims, _) => [...createTensorShapeVariables(dims)])
        .reduce((acc, inputProgramUniforms) => acc.concat(inputProgramUniforms), programUniformsInit);
      programUniforms.push(...createTensorShapeVariables(outputShape));
      return {
        outputs: [{ dims: outputShape, dataType }],
        dispatchGroup: { x: Math.ceil(outputSize / 64 /* workgroup size */) },
        programUniforms,
      };
    },
    getShaderSource,
  };
};

export const einsum = (context: ComputeContext, attributes: EinsumAttributes): void => {
  const einsumEquation = new EinsumEquation(context.inputs, attributes.equation);
  const outputShape = einsumEquation.outputDims;
  const inputShapes = context.inputs.map((input, _) => input.dims);
  context.compute(createEinsumProgramInfo(inputShapes, context.inputs[0].dataType, einsumEquation, outputShape));
};

export const parseEinsumAttributes = (attributes: Record<string, unknown>): EinsumAttributes => {
  const equation = (attributes.equation as string).replace(/\s+/g, '');
  return createAttributeWithCacheKey({ equation });
};
