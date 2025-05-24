/**
 * Converts bounding boxes from center format to corners format.
 *
 * @param {number[]} arr The coordinate for the center of the box and its width, height dimensions (center_x, center_y, width, height)
 * @returns {number[]} The coodinates for the top-left and bottom-right corners of the box (top_left_x, top_left_y, bottom_right_x, bottom_right_y)
 */
export function center_to_corners_format([centerX, centerY, width, height]: number[]): number[];
/**
 * Post-processes the outputs of the model (for object detection).
 * @param {Object} outputs The outputs of the model that must be post-processed
 * @param {Tensor} outputs.logits The logits
 * @param {Tensor} outputs.pred_boxes The predicted boxes.
 * @param {number} [threshold=0.5] The threshold to use for the scores.
 * @param {[number, number][]} [target_sizes=null] The sizes of the original images.
 * @param {boolean} [is_zero_shot=false] Whether zero-shot object detection was performed.
 * @return {Object[]} An array of objects containing the post-processed outputs.
 */
export function post_process_object_detection(outputs: {
    logits: Tensor;
    pred_boxes: Tensor;
}, threshold?: number, target_sizes?: [number, number][], is_zero_shot?: boolean): any[];
/**
 * Post-processes the outputs of the model (for semantic segmentation).
 * @param {*} outputs Raw outputs of the model.
 * @param {[number, number][]} [target_sizes=null] List of tuples corresponding to the requested final size
 * (height, width) of each prediction. If unset, predictions will not be resized.
 * @returns {{segmentation: Tensor; labels: number[]}[]} The semantic segmentation maps.
 */
export function post_process_semantic_segmentation(outputs: any, target_sizes?: [number, number][]): {
    segmentation: Tensor;
    labels: number[];
}[];
/**
 * Post-process the model output to generate the final panoptic segmentation.
 * @param {*} outputs The model output to post process
 * @param {number} [threshold=0.5] The probability score threshold to keep predicted instance masks.
 * @param {number} [mask_threshold=0.5] Threshold to use when turning the predicted masks into binary values.
 * @param {number} [overlap_mask_area_threshold=0.8] The overlap mask area threshold to merge or discard small disconnected parts within each binary instance mask.
 * @param {Set<number>} [label_ids_to_fuse=null] The labels in this state will have all their instances be fused together.
 * @param {[number, number][]} [target_sizes=null] The target sizes to resize the masks to.
 * @returns {Array<{ segmentation: Tensor, segments_info: Array<{id: number, label_id: number, score: number}>}>}
 */
export function post_process_panoptic_segmentation(outputs: any, threshold?: number, mask_threshold?: number, overlap_mask_area_threshold?: number, label_ids_to_fuse?: Set<number>, target_sizes?: [number, number][]): Array<{
    segmentation: Tensor;
    segments_info: Array<{
        id: number;
        label_id: number;
        score: number;
    }>;
}>;
/**
 * Post-processes the outputs of the model (for instance segmentation).
 * @param {*} outputs Raw outputs of the model.
 * @param {number} [threshold=0.5] The probability score threshold to keep predicted instance masks.
 * @param {[number, number][]} [target_sizes=null] List of tuples corresponding to the requested final size
 * (height, width) of each prediction. If unset, predictions will not be resized.
 * @returns {Array<{ segmentation: Tensor, segments_info: Array<{id: number, label_id: number, score: number}>}>}
 */
export function post_process_instance_segmentation(outputs: any, threshold?: number, target_sizes?: [number, number][]): Array<{
    segmentation: Tensor;
    segments_info: Array<{
        id: number;
        label_id: number;
        score: number;
    }>;
}>;
declare const ImageProcessor_base: new () => {
    (...args: any[]): any;
    _call(...args: any[]): any;
};
/**
 * @typedef {Object} ImageProcessorConfig A configuration object used to create an image processor.
 * @property {function} [progress_callback=null] If specified, this function will be called during model construction, to provide the user with progress updates.
 * @property {number[]} [image_mean] The mean values for image normalization.
 * @property {number[]} [image_std] The standard deviation values for image normalization.
 * @property {boolean} [do_rescale] Whether to rescale the image pixel values to the [0,1] range.
 * @property {number} [rescale_factor] The factor to use for rescaling the image pixel values.
 * @property {boolean} [do_normalize] Whether to normalize the image pixel values.
 * @property {boolean} [do_resize] Whether to resize the image.
 * @property {number} [resample] What method to use for resampling.
 * @property {number|Object} [size] The size to resize the image to.
 * @property {number|Object} [image_size] The size to resize the image to (same as `size`).
 * @property {boolean} [do_flip_channel_order=false] Whether to flip the color channels from RGB to BGR.
 * Can be overridden by the `do_flip_channel_order` parameter in the `preprocess` method.
 * @property {boolean} [do_center_crop] Whether to center crop the image to the specified `crop_size`.
 * Can be overridden by `do_center_crop` in the `preprocess` method.
 * @property {boolean} [do_thumbnail] Whether to resize the image using thumbnail method.
 * @property {boolean} [keep_aspect_ratio] If `true`, the image is resized to the largest possible size such that the aspect ratio is preserved.
 * Can be overidden by `keep_aspect_ratio` in `preprocess`.
 * @property {number} [ensure_multiple_of] If `do_resize` is `true`, the image is resized to a size that is a multiple of this value.
 * Can be overidden by `ensure_multiple_of` in `preprocess`.
 *
 * @property {number[]} [mean] The mean values for image normalization (same as `image_mean`).
 * @property {number[]} [std] The standard deviation values for image normalization (same as `image_std`).
 */
export class ImageProcessor extends ImageProcessor_base {
    /**
     * Instantiate one of the processor classes of the library from a pretrained model.
     *
     * The processor class to instantiate is selected based on the `image_processor_type` (or `feature_extractor_type`; legacy)
     * property of the config object (either passed as an argument or loaded from `pretrained_model_name_or_path` if possible)
     *
     * @param {string} pretrained_model_name_or_path The name or path of the pretrained model. Can be either:
     * - A string, the *model id* of a pretrained processor hosted inside a model repo on huggingface.co.
     *   Valid model ids can be located at the root-level, like `bert-base-uncased`, or namespaced under a
     *   user or organization name, like `dbmdz/bert-base-german-cased`.
     * - A path to a *directory* containing processor files, e.g., `./my_model_directory/`.
     * @param {import('../utils/hub.js').PretrainedOptions} options Additional options for loading the processor.
     *
     * @returns {Promise<ImageProcessor>} A new instance of the Processor class.
     */
    static from_pretrained(pretrained_model_name_or_path: string, options: import("../utils/hub.js").PretrainedOptions): Promise<ImageProcessor>;
    /**
     * Constructs a new `ImageProcessor`.
     * @param {ImageProcessorConfig} config The configuration object.
     */
    constructor(config: ImageProcessorConfig);
    image_mean: number[];
    image_std: number[];
    resample: number;
    do_rescale: boolean;
    rescale_factor: number;
    do_normalize: boolean;
    do_thumbnail: boolean;
    size: any;
    do_resize: boolean;
    size_divisibility: any;
    do_center_crop: boolean;
    crop_size: any;
    do_convert_rgb: any;
    do_crop_margin: any;
    pad_size: any;
    do_pad: any;
    min_pixels: any;
    max_pixels: any;
    do_flip_channel_order: boolean;
    config: ImageProcessorConfig;
    /**
     * Resize the image to make a thumbnail. The image is resized so that no dimension is larger than any
     * corresponding dimension of the specified size.
     * @param {RawImage} image The image to be resized.
     * @param {{height:number, width:number}} size The size `{"height": h, "width": w}` to resize the image to.
     * @param {string | 0 | 1 | 2 | 3 | 4 | 5} [resample=2] The resampling filter to use.
     * @returns {Promise<RawImage>} The resized image.
     */
    thumbnail(image: RawImage, size: {
        height: number;
        width: number;
    }, resample?: string | 0 | 1 | 2 | 3 | 4 | 5): Promise<RawImage>;
    /**
     * Crops the margin of the image. Gray pixels are considered margin (i.e., pixels with a value below the threshold).
     * @param {RawImage} image The image to be cropped.
     * @param {number} gray_threshold Value below which pixels are considered to be gray.
     * @returns {Promise<RawImage>} The cropped image.
     */
    crop_margin(image: RawImage, gray_threshold?: number): Promise<RawImage>;
    /**
     * Pad the image by a certain amount.
     * @param {Float32Array} pixelData The pixel data to pad.
     * @param {number[]} imgDims The dimensions of the image (height, width, channels).
     * @param {{width:number; height:number}|number|'square'} padSize The dimensions of the padded image.
     * @param {Object} options The options for padding.
     * @param {'constant'|'symmetric'} [options.mode='constant'] The type of padding to add.
     * @param {boolean} [options.center=false] Whether to center the image.
     * @param {number|number[]} [options.constant_values=0] The constant value to use for padding.
     * @returns {[Float32Array, number[]]} The padded pixel data and image dimensions.
     */
    pad_image(pixelData: Float32Array, imgDims: number[], padSize: {
        width: number;
        height: number;
    } | number | "square", { mode, center, constant_values, }?: {
        mode?: "constant" | "symmetric";
        center?: boolean;
        constant_values?: number | number[];
    }): [Float32Array, number[]];
    /**
     * Rescale the image' pixel values by `this.rescale_factor`.
     * @param {Float32Array} pixelData The pixel data to rescale.
     * @returns {void}
     */
    rescale(pixelData: Float32Array): void;
    /**
     * Find the target (width, height) dimension of the output image after
     * resizing given the input image and the desired size.
     * @param {RawImage} image The image to resize.
     * @param {any} size The size to use for resizing the image.
     * @returns {[number, number]} The target (width, height) dimension of the output image after resizing.
     */
    get_resize_output_image_size(image: RawImage, size: any): [number, number];
    /**
     * Resizes the image.
     * @param {RawImage} image The image to resize.
     * @returns {Promise<RawImage>} The resized image.
     */
    resize(image: RawImage): Promise<RawImage>;
    /**
     * @typedef {object} PreprocessedImage
     * @property {HeightWidth} original_size The original size of the image.
     * @property {HeightWidth} reshaped_input_size The reshaped input size of the image.
     * @property {Tensor} pixel_values The pixel values of the preprocessed image.
     */
    /**
     * Preprocesses the given image.
     *
     * @param {RawImage} image The image to preprocess.
     * @param {Object} overrides The overrides for the preprocessing options.
     * @returns {Promise<PreprocessedImage>} The preprocessed image.
     */
    preprocess(image: RawImage, { do_normalize, do_pad, do_convert_rgb, do_convert_grayscale, do_flip_channel_order, }?: any): Promise<{
        /**
         * The original size of the image.
         */
        original_size: HeightWidth;
        /**
         * The reshaped input size of the image.
         */
        reshaped_input_size: HeightWidth;
        /**
         * The pixel values of the preprocessed image.
         */
        pixel_values: Tensor;
    }>;
    /**
     * Calls the feature extraction process on an array of images,
     * preprocesses each image, and concatenates the resulting
     * features into a single Tensor.
     * @param {RawImage[]} images The image(s) to extract features from.
     * @param {...any} args Additional arguments.
     * @returns {Promise<ImageProcessorResult>} An object containing the concatenated pixel values (and other metadata) of the preprocessed images.
     */
    _call(images: RawImage[], ...args: any[]): Promise<ImageProcessorResult>;
}
/**
 * Named tuple to indicate the order we are using is (height x width),
 * even though the Graphics' industry standard is (width x height).
 */
export type HeightWidth = [height: number, width: number];
export type ImageProcessorResult = {
    /**
     * The pixel values of the batched preprocessed images.
     */
    pixel_values: Tensor;
    /**
     * Array of two-dimensional tuples like [[480, 640]].
     */
    original_sizes: HeightWidth[];
    /**
     * Array of two-dimensional tuples like [[1000, 1330]].
     */
    reshaped_input_sizes: HeightWidth[];
};
/**
 * A configuration object used to create an image processor.
 */
export type ImageProcessorConfig = {
    /**
     * If specified, this function will be called during model construction, to provide the user with progress updates.
     */
    progress_callback?: Function;
    /**
     * The mean values for image normalization.
     */
    image_mean?: number[];
    /**
     * The standard deviation values for image normalization.
     */
    image_std?: number[];
    /**
     * Whether to rescale the image pixel values to the [0,1] range.
     */
    do_rescale?: boolean;
    /**
     * The factor to use for rescaling the image pixel values.
     */
    rescale_factor?: number;
    /**
     * Whether to normalize the image pixel values.
     */
    do_normalize?: boolean;
    /**
     * Whether to resize the image.
     */
    do_resize?: boolean;
    /**
     * What method to use for resampling.
     */
    resample?: number;
    /**
     * The size to resize the image to.
     */
    size?: number | any;
    /**
     * The size to resize the image to (same as `size`).
     */
    image_size?: number | any;
    /**
     * Whether to flip the color channels from RGB to BGR.
     * Can be overridden by the `do_flip_channel_order` parameter in the `preprocess` method.
     */
    do_flip_channel_order?: boolean;
    /**
     * Whether to center crop the image to the specified `crop_size`.
     * Can be overridden by `do_center_crop` in the `preprocess` method.
     */
    do_center_crop?: boolean;
    /**
     * Whether to resize the image using thumbnail method.
     */
    do_thumbnail?: boolean;
    /**
     * If `true`, the image is resized to the largest possible size such that the aspect ratio is preserved.
     * Can be overidden by `keep_aspect_ratio` in `preprocess`.
     */
    keep_aspect_ratio?: boolean;
    /**
     * If `do_resize` is `true`, the image is resized to a size that is a multiple of this value.
     * Can be overidden by `ensure_multiple_of` in `preprocess`.
     */
    ensure_multiple_of?: number;
    /**
     * The mean values for image normalization (same as `image_mean`).
     */
    mean?: number[];
    /**
     * The standard deviation values for image normalization (same as `image_std`).
     */
    std?: number[];
};
import { Tensor } from "../utils/tensor.js";
import { RawImage } from "../utils/image.js";
export {};
//# sourceMappingURL=image_processors_utils.d.ts.map