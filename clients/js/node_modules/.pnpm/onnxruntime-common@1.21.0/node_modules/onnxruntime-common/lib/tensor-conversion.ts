// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { OptionsFormat, OptionsNormalizationParameters, OptionsTensorLayout } from './tensor-factory.js';

export interface TensorToDataUrlOptions extends OptionsTensorLayout, OptionsFormat, OptionsNormalizationParameters {}

export interface TensorToImageDataOptions extends OptionsTensorLayout, OptionsFormat, OptionsNormalizationParameters {}

export interface ConversionUtils {
  /**
   * creates a DataURL instance from tensor
   *
   * @param options - An optional object representing options for creating a DataURL instance from the tensor.
   *
   * The following default settings will be applied:
   * - `format`: `'RGB'`
   * - `tensorLayout`: `'NCHW'`
   * @returns a DataURL string representing the image converted from tensor data
   */
  toDataURL(options?: TensorToDataUrlOptions): string;

  /**
   * creates an ImageData instance from tensor
   *
   * @param options - An optional object representing options for creating an ImageData instance from the tensor.
   *
   * The following default settings will be applied:
   * - `format`: `'RGB'`
   * - `tensorLayout`: `'NCHW'`
   * @returns an ImageData instance representing the image converted from tensor data
   */
  toImageData(options?: TensorToImageDataOptions): ImageData;
}
