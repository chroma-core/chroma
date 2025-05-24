import { 
    ImageProcessor,
} from "../../base/image_processors_utils.js";
import { calculateDimensions } from "../../utils/core.js";

import {
    interpolate_4d,
    Tensor,
} from "../../utils/tensor.js";


/**
 * @typedef {object} SamImageProcessorResult
 * @property {Tensor} pixel_values
 * @property {import("../../base/image_processors_utils.js").HeightWidth[]} original_sizes
 * @property {import("../../base/image_processors_utils.js").HeightWidth[]} reshaped_input_sizes
 * @property {Tensor} [input_points]
 * @property {Tensor} [input_labels]
 * @property {Tensor} [input_boxes]
 */

export class SamImageProcessor extends ImageProcessor {

    /**
     * 
     * @param {any} input_points 
     * @param {import("../../base/image_processors_utils.js").HeightWidth[]} original_sizes 
     * @param {import("../../base/image_processors_utils.js").HeightWidth[]} reshaped_input_sizes 
     * @returns {Tensor}
     */
    reshape_input_points(input_points, original_sizes, reshaped_input_sizes, is_bounding_box = false) {

        // Make deep copy to avoid altering user's input
        input_points = structuredClone(input_points);
        let shape = calculateDimensions(input_points);

        // TODO: add support for 2D input_points
        if (shape.length === 3) {
            // Correct user's input
            if (!is_bounding_box) {
                shape = [1, ...shape];
            }
            input_points = [input_points];
        } else if (shape.length !== 4) {
            throw Error("The input_points must be a 4D tensor of shape `batch_size`, `point_batch_size`, `nb_points_per_image`, `2`.")
        }

        // Reshape input points
        for (let i = 0; i < input_points.length; ++i) { // batch_size
            let originalImageSize = original_sizes[i];
            let reshapedImageSize = reshaped_input_sizes[i];

            let resizeFactors = [
                reshapedImageSize[0] / originalImageSize[0],
                reshapedImageSize[1] / originalImageSize[1]
            ]

            for (let j = 0; j < input_points[i].length; ++j) { // point_batch_size
                for (let k = 0; k < input_points[i][j].length; ++k) { // nb_points_per_image
                    for (let w = 0; w < input_points[i][j][k].length; ++w) { // 2 or 4
                        input_points[i][j][k][w] *= resizeFactors[w % 2];
                    }
                }
            }
        }

        return new Tensor(
            'float32',
            Float32Array.from(input_points.flat(Infinity)),
            shape
        )

    }

    /**
     * 
     * @param {any} input_labels 
     * @param {Tensor} input_points 
     * @returns {Tensor}
     */
    add_input_labels(input_labels, input_points) {
        let shape = calculateDimensions(input_labels);
        if (shape.length === 2) {
            // Correct user's input
            shape = [1, ...shape];
            input_labels = [input_labels];
        } else if (shape.length !== 3) {
            throw Error("The input_points must be a 4D tensor of shape `batch_size`, `point_batch_size`, `nb_points_per_image`, `2`.")
        }

        if (shape.some((x, i) => x !== input_points.dims[i])) {
            throw Error(`The first ${shape.length} dimensions of 'input_points' and 'input_labels' must be the same.`)
        }
        return new Tensor(
            'int64',
            input_labels.flat(Infinity).map(BigInt),
            shape,
        )
    }
    /**
     * @param {any[]} images The URL(s) of the image(s) to extract features from.
     * @param {Object} [options] Additional options for the processor.
     * @param {any} [options.input_points=null] A 3D or 4D array, representing the input points provided by the user.
     * - 3D: `[point_batch_size, nb_points_per_image, 2]`. In this case, `batch_size` is assumed to be 1.
     * - 4D: `[batch_size, point_batch_size, nb_points_per_image, 2]`.
     * @param {any} [options.input_labels=null] A 2D or 3D array, representing the input labels for the points, used by the prompt encoder to encode the prompt.
     * - 2D: `[point_batch_size, nb_points_per_image]`. In this case, `batch_size` is assumed to be 1.
     * - 3D: `[batch_size, point_batch_size, nb_points_per_image]`.
     * @param {number[][][]} [options.input_boxes=null] A 3D array of shape `(batch_size, num_boxes, 4)`, representing the input boxes provided by the user.
     * This is used by the prompt encoder to encode the prompt. Generally yields to much better generated masks.
     * The processor will generate a tensor, with each dimension corresponding respectively to the image batch size,
     * the number of boxes per image and the coordinates of the top left and botton right point of the box.
     * In the order (`x1`, `y1`, `x2`, `y2`):
     * - `x1`: the x coordinate of the top left point of the input box
     * - `y1`: the y coordinate of the top left point of the input box
     * - `x2`: the x coordinate of the bottom right point of the input box
     * - `y2`: the y coordinate of the bottom right point of the input box
     * @returns {Promise<SamImageProcessorResult>}
     */
    async _call(images, {
        input_points = null,
        input_labels = null,
        input_boxes = null
    } = {}) {
        // TODO allow user to use preprocessed images
        /** @type {SamImageProcessorResult} */
        const processed = await super._call(images);

        if (input_points) {
            processed.input_points = this.reshape_input_points(
                input_points, processed.original_sizes, processed.reshaped_input_sizes
            );
        }

        if (input_labels) {
            if (!processed.input_points) {
                throw Error("`input_points` must be provided if `input_labels` are provided.")
            }
            processed.input_labels = this.add_input_labels(input_labels, processed.input_points);
        }

        if (input_boxes) {
            processed.input_boxes = this.reshape_input_points(
                input_boxes, processed.original_sizes, processed.reshaped_input_sizes, true,
            );
        }

        return processed;
    }

    /**
     * Remove padding and upscale masks to the original image size.
     * @param {Tensor} masks Batched masks from the mask_decoder in (batch_size, num_channels, height, width) format.
     * @param {[number, number][]} original_sizes The original sizes of each image before it was resized to the model's expected input shape, in (height, width) format.
     * @param {[number, number][]} reshaped_input_sizes The size of each image as it is fed to the model, in (height, width) format. Used to remove padding.
     * @param {Object} options Optional parameters for post-processing.
     * @param {number} [options.mask_threshold] The threshold to use for binarizing the masks.
     * @param {boolean} [options.binarize] Whether to binarize the masks.
     * @param {Object} [options.pad_size] The target size the images were padded to before being passed to the model. If `null`, the target size is assumed to be the processor's `pad_size`.
     * @param {number} [options.pad_size.height] The height the images were padded to.
     * @param {number} [options.pad_size.width] The width the images were padded to.
     * @returns {Promise<Tensor[]>} Batched masks in batch_size, num_channels, height, width) format, where (height, width) is given by original_size.
     */
    async post_process_masks(masks, original_sizes, reshaped_input_sizes, {
        mask_threshold = 0.0,
        binarize = true,
        pad_size = null,
    } = {}) {
        // masks: [1, 1, 3, 256, 256]

        const output_masks = [];

        pad_size = pad_size ?? this.pad_size;

        /** @type {[number, number]} */
        const target_image_size = [pad_size.height, pad_size.width];

        for (let i = 0; i < original_sizes.length; ++i) {
            const original_size = original_sizes[i];
            const reshaped_input_size = reshaped_input_sizes[i];

            // Upscale mask to padded size
            let interpolated_mask = (await interpolate_4d(
                masks[i],
                { mode: 'bilinear', size: target_image_size }
            ));

            // Crop mask
            interpolated_mask = interpolated_mask.slice(null, null, [0, reshaped_input_size[0]], [0, reshaped_input_size[1]]);

            // Downscale mask
            interpolated_mask = (await interpolate_4d(
                interpolated_mask,
                { mode: 'bilinear', size: original_size }
            ));

            if (binarize) {
                const data = interpolated_mask.data;
                const binarizedMaskData = new Uint8Array(data.length);
                for (let i = 0; i < data.length; ++i) {
                    if (data[i] > mask_threshold) {
                        binarizedMaskData[i] = 1;
                    }
                }
                interpolated_mask = new Tensor(
                    'bool',
                    binarizedMaskData,
                    interpolated_mask.dims
                )
            }

            output_masks.push(interpolated_mask);
        }

        return output_masks;
    }

    /**
     * Generates a list of crop boxes of different sizes. Each layer has (2**i)**2 boxes for the ith layer.
     * @param {import("../../utils/image.js").RawImage} image Input original image
     * @param {number} target_size Target size of the resized image
     * @param {Object} options Options for generating crop boxes 
     * @param {number} [options.crop_n_layers] If >0, mask prediction will be run again on crops of the image.
     * Sets the number of layers to run, where each layer has 2**i_layer number of image crops.
     * @param {number} [options.overlap_ratio] Sets the degree to which crops overlap. In the first crop layer,
     * crops will overlap by this fraction of the image length. Later layers with more crops scale down this overlap.
     * @param {number} [options.points_per_crop] Number of points to sample from each crop.
     * @param {number} [options.crop_n_points_downscale_factor] The number of points-per-side sampled in layer n is
     * scaled down by crop_n_points_downscale_factor**n.
     * @returns {Object} An object containing the crop boxes, number of points per crop, cropped images, and input labels.
     */
    generate_crop_boxes(image, target_size, {
        crop_n_layers = 0,
        overlap_ratio = 512 / 1500,
        points_per_crop = 32,
        crop_n_points_downscale_factor = 1,
    } = {}) {
        // TODO: Implement
        // return { crop_boxes, points_per_crop, cropped_images, input_labels }
    }
}

