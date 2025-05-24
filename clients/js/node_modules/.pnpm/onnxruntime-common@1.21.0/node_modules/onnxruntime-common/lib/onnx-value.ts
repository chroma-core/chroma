// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Tensor } from './tensor.js';

export type NonTensorType = never;

/**
 * Type OnnxValue Represents both tensors and non-tensors value for model's inputs/outputs.
 *
 * NOTE: currently not support non-tensor
 */
export type OnnxValue = Tensor | NonTensorType;

/**
 * Type OnnxValueDataLocation represents the location of the data of an OnnxValue.
 */
export type OnnxValueDataLocation = Tensor.DataLocation;
