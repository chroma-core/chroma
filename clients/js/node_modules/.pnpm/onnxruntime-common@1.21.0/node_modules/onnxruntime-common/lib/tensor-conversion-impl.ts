// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { TensorToDataUrlOptions, TensorToImageDataOptions } from './tensor-conversion.js';
import { Tensor } from './tensor.js';

/**
 * implementation of Tensor.toDataURL()
 */
export const tensorToDataURL = (tensor: Tensor, options?: TensorToDataUrlOptions): string => {
  const canvas = typeof document !== 'undefined' ? document.createElement('canvas') : new OffscreenCanvas(1, 1);
  canvas.width = tensor.dims[3];
  canvas.height = tensor.dims[2];
  const pixels2DContext = canvas.getContext('2d') as
    | CanvasRenderingContext2D
    | OffscreenCanvasRenderingContext2D
    | null;

  if (pixels2DContext != null) {
    // Default values for height and width & format
    let width: number;
    let height: number;
    if (options?.tensorLayout !== undefined && options.tensorLayout === 'NHWC') {
      width = tensor.dims[2];
      height = tensor.dims[3];
    } else {
      // Default layout is NCWH
      width = tensor.dims[3];
      height = tensor.dims[2];
    }

    const inputformat = options?.format !== undefined ? options.format : 'RGB';

    const norm = options?.norm;
    let normMean: [number, number, number, number];
    let normBias: [number, number, number, number];
    if (norm === undefined || norm.mean === undefined) {
      normMean = [255, 255, 255, 255];
    } else {
      if (typeof norm.mean === 'number') {
        normMean = [norm.mean, norm.mean, norm.mean, norm.mean];
      } else {
        normMean = [norm.mean[0], norm.mean[1], norm.mean[2], 0];
        if (norm.mean[3] !== undefined) {
          normMean[3] = norm.mean[3];
        }
      }
    }
    if (norm === undefined || norm.bias === undefined) {
      normBias = [0, 0, 0, 0];
    } else {
      if (typeof norm.bias === 'number') {
        normBias = [norm.bias, norm.bias, norm.bias, norm.bias];
      } else {
        normBias = [norm.bias[0], norm.bias[1], norm.bias[2], 0];
        if (norm.bias[3] !== undefined) {
          normBias[3] = norm.bias[3];
        }
      }
    }

    const stride = height * width;
    // Default pointer assignments
    let rTensorPointer = 0,
      gTensorPointer = stride,
      bTensorPointer = stride * 2,
      aTensorPointer = -1;

    // Updating the pointer assignments based on the input image format
    if (inputformat === 'RGBA') {
      rTensorPointer = 0;
      gTensorPointer = stride;
      bTensorPointer = stride * 2;
      aTensorPointer = stride * 3;
    } else if (inputformat === 'RGB') {
      rTensorPointer = 0;
      gTensorPointer = stride;
      bTensorPointer = stride * 2;
    } else if (inputformat === 'RBG') {
      rTensorPointer = 0;
      bTensorPointer = stride;
      gTensorPointer = stride * 2;
    }

    for (let i = 0; i < height; i++) {
      for (let j = 0; j < width; j++) {
        const R = ((tensor.data[rTensorPointer++] as number) - normBias[0]) * normMean[0]; // R value
        const G = ((tensor.data[gTensorPointer++] as number) - normBias[1]) * normMean[1]; // G value
        const B = ((tensor.data[bTensorPointer++] as number) - normBias[2]) * normMean[2]; // B value
        const A = aTensorPointer === -1 ? 255 : ((tensor.data[aTensorPointer++] as number) - normBias[3]) * normMean[3]; // A value
        // eslint-disable-next-line @typescript-eslint/restrict-plus-operands
        pixels2DContext.fillStyle = 'rgba(' + R + ',' + G + ',' + B + ',' + A + ')';
        pixels2DContext.fillRect(j, i, 1, 1);
      }
    }
    if ('toDataURL' in canvas) {
      return canvas.toDataURL();
    } else {
      throw new Error('toDataURL is not supported');
    }
  } else {
    throw new Error('Can not access image data');
  }
};

/**
 * implementation of Tensor.toImageData()
 */
export const tensorToImageData = (tensor: Tensor, options?: TensorToImageDataOptions): ImageData => {
  const pixels2DContext =
    typeof document !== 'undefined'
      ? document.createElement('canvas').getContext('2d')
      : (new OffscreenCanvas(1, 1).getContext('2d') as OffscreenCanvasRenderingContext2D);
  let image: ImageData;
  if (pixels2DContext != null) {
    // Default values for height and width & format
    let width: number;
    let height: number;
    let channels: number;
    if (options?.tensorLayout !== undefined && options.tensorLayout === 'NHWC') {
      width = tensor.dims[2];
      height = tensor.dims[1];
      channels = tensor.dims[3];
    } else {
      // Default layout is NCWH
      width = tensor.dims[3];
      height = tensor.dims[2];
      channels = tensor.dims[1];
    }
    const inputformat = options !== undefined ? (options.format !== undefined ? options.format : 'RGB') : 'RGB';

    const norm = options?.norm;
    let normMean: [number, number, number, number];
    let normBias: [number, number, number, number];
    if (norm === undefined || norm.mean === undefined) {
      normMean = [255, 255, 255, 255];
    } else {
      if (typeof norm.mean === 'number') {
        normMean = [norm.mean, norm.mean, norm.mean, norm.mean];
      } else {
        normMean = [norm.mean[0], norm.mean[1], norm.mean[2], 255];
        if (norm.mean[3] !== undefined) {
          normMean[3] = norm.mean[3];
        }
      }
    }
    if (norm === undefined || norm.bias === undefined) {
      normBias = [0, 0, 0, 0];
    } else {
      if (typeof norm.bias === 'number') {
        normBias = [norm.bias, norm.bias, norm.bias, norm.bias];
      } else {
        normBias = [norm.bias[0], norm.bias[1], norm.bias[2], 0];
        if (norm.bias[3] !== undefined) {
          normBias[3] = norm.bias[3];
        }
      }
    }

    const stride = height * width;
    if (options !== undefined) {
      if (
        (options.format !== undefined && channels === 4 && options.format !== 'RGBA') ||
        (channels === 3 && options.format !== 'RGB' && options.format !== 'BGR')
      ) {
        throw new Error("Tensor format doesn't match input tensor dims");
      }
    }

    // Default pointer assignments
    const step = 4;
    let rImagePointer = 0,
      gImagePointer = 1,
      bImagePointer = 2,
      aImagePointer = 3;
    let rTensorPointer = 0,
      gTensorPointer = stride,
      bTensorPointer = stride * 2,
      aTensorPointer = -1;

    // Updating the pointer assignments based on the input image format
    if (inputformat === 'RGBA') {
      rTensorPointer = 0;
      gTensorPointer = stride;
      bTensorPointer = stride * 2;
      aTensorPointer = stride * 3;
    } else if (inputformat === 'RGB') {
      rTensorPointer = 0;
      gTensorPointer = stride;
      bTensorPointer = stride * 2;
    } else if (inputformat === 'RBG') {
      rTensorPointer = 0;
      bTensorPointer = stride;
      gTensorPointer = stride * 2;
    }

    image = pixels2DContext.createImageData(width, height);

    for (
      let i = 0;
      i < height * width;
      rImagePointer += step, gImagePointer += step, bImagePointer += step, aImagePointer += step, i++
    ) {
      image.data[rImagePointer] = ((tensor.data[rTensorPointer++] as number) - normBias[0]) * normMean[0]; // R value
      image.data[gImagePointer] = ((tensor.data[gTensorPointer++] as number) - normBias[1]) * normMean[1]; // G value
      image.data[bImagePointer] = ((tensor.data[bTensorPointer++] as number) - normBias[2]) * normMean[2]; // B value
      image.data[aImagePointer] =
        aTensorPointer === -1 ? 255 : ((tensor.data[aTensorPointer++] as number) - normBias[3]) * normMean[3]; // A value
    }
  } else {
    throw new Error('Can not access image data');
  }
  return image;
};
