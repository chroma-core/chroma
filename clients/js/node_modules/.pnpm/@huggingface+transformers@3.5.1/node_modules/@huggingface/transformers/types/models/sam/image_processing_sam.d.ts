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
    reshape_input_points(input_points: any, original_sizes: import("../../base/image_processors_utils.js").HeightWidth[], reshaped_input_sizes: import("../../base/image_processors_utils.js").HeightWidth[], is_bounding_box?: boolean): Tensor;
    /**
     *
     * @param {any} input_labels
     * @param {Tensor} input_points
     * @returns {Tensor}
     */
    add_input_labels(input_labels: any, input_points: Tensor): Tensor;
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
    _call(images: any[], { input_points, input_labels, input_boxes }?: {
        input_points?: any;
        input_labels?: any;
        input_boxes?: number[][][];
    }): Promise<SamImageProcessorResult>;
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
    post_process_masks(masks: Tensor, original_sizes: [number, number][], reshaped_input_sizes: [number, number][], { mask_threshold, binarize, pad_size, }?: {
        mask_threshold?: number;
        binarize?: boolean;
        pad_size?: {
            height?: number;
            width?: number;
        };
    }): Promise<Tensor[]>;
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
    generate_crop_boxes(image: import("../../utils/image.js").RawImage, target_size: number, { crop_n_layers, overlap_ratio, points_per_crop, crop_n_points_downscale_factor, }?: {
        crop_n_layers?: number;
        overlap_ratio?: number;
        points_per_crop?: number;
        crop_n_points_downscale_factor?: number;
    }): any;
}
export type SamImageProcessorResult = {
    pixel_values: Tensor;
    original_sizes: import("../../base/image_processors_utils.js").HeightWidth[];
    reshaped_input_sizes: import("../../base/image_processors_utils.js").HeightWidth[];
    input_points?: Tensor;
    input_labels?: Tensor;
    input_boxes?: Tensor;
};
import { ImageProcessor } from "../../base/image_processors_utils.js";
import { Tensor } from "../../utils/tensor.js";
//# sourceMappingURL=image_processing_sam.d.ts.map