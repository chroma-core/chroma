import { Callable } from "../utils/generic.js";
import { Tensor, interpolate, stack } from "../utils/tensor.js";
import { bankers_round, max, min, softmax } from "../utils/maths.js";
import { RawImage } from "../utils/image.js";
import { calculateReflectOffset } from "../utils/core.js";
import { getModelJSON } from "../utils/hub.js";
import { IMAGE_PROCESSOR_NAME } from '../utils/constants.js';

/**
 * Named tuple to indicate the order we are using is (height x width),
 * even though the Graphics' industry standard is (width x height).
 * @typedef {[height: number, width: number]} HeightWidth
 */


/**
 * @typedef {object} ImageProcessorResult
 * @property {Tensor} pixel_values The pixel values of the batched preprocessed images.
 * @property {HeightWidth[]} original_sizes Array of two-dimensional tuples like [[480, 640]].
 * @property {HeightWidth[]} reshaped_input_sizes Array of two-dimensional tuples like [[1000, 1330]].
 */



/**
 * Helper function to constrain a value to be a multiple of a number.
 * @param {number} val The value to constrain.
 * @param {number} multiple The number to constrain to.
 * @param {number} [minVal=0] The minimum value to constrain to.
 * @param {number} [maxVal=null] The maximum value to constrain to.
 * @returns {number} The constrained value.
 * @private
 */
function constraint_to_multiple_of(val, multiple, minVal = 0, maxVal = null) {
    const a = val / multiple;
    let x = bankers_round(a) * multiple;

    if (maxVal !== null && x > maxVal) {
        x = Math.floor(a) * multiple;
    }

    if (x < minVal) {
        x = Math.ceil(a) * multiple;
    }

    return x;
}

/**
 * Rounds the height and width down to the closest multiple of size_divisibility
 * @param {[number, number]} size The size of the image
 * @param {number} divisor The divisor to use.
 * @returns {[number, number]} The rounded size.
 */
function enforce_size_divisibility([width, height], divisor) {
    return [
        Math.max(Math.floor(width / divisor), 1) * divisor,
        Math.max(Math.floor(height / divisor), 1) * divisor
    ];
}


// Helper functions

/**
 * Converts bounding boxes from center format to corners format.
 * 
 * @param {number[]} arr The coordinate for the center of the box and its width, height dimensions (center_x, center_y, width, height)
 * @returns {number[]} The coodinates for the top-left and bottom-right corners of the box (top_left_x, top_left_y, bottom_right_x, bottom_right_y)
 */
export function center_to_corners_format([centerX, centerY, width, height]) {
    return [
        centerX - width / 2,
        centerY - height / 2,
        centerX + width / 2,
        centerY + height / 2
    ];
}

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
export function post_process_object_detection(outputs, threshold = 0.5, target_sizes = null, is_zero_shot = false) {
    const out_logits = outputs.logits;
    const out_bbox = outputs.pred_boxes;
    const [batch_size, num_boxes, num_classes] = out_logits.dims;

    if (target_sizes !== null && target_sizes.length !== batch_size) {
        throw Error("Make sure that you pass in as many target sizes as the batch dimension of the logits")
    }
    let toReturn = [];
    for (let i = 0; i < batch_size; ++i) {
        let target_size = target_sizes !== null ? target_sizes[i] : null;
        let info = {
            boxes: [],
            classes: [],
            scores: []
        }
        let logits = out_logits[i];
        let bbox = out_bbox[i];

        for (let j = 0; j < num_boxes; ++j) {
            let logit = logits[j];

            let indices = [];
            let probs;
            if (is_zero_shot) {
                // Get indices of classes with high enough probability
                probs = logit.sigmoid().data;
                for (let k = 0; k < probs.length; ++k) {
                    if (probs[k] > threshold) {
                        indices.push(k);
                    }
                }

            } else {
                // Get most probable class
                let maxIndex = max(logit.data)[1];

                if (maxIndex === num_classes - 1) {
                    // This is the background class, skip it
                    continue;
                }
                // Compute softmax over classes
                probs = softmax(logit.data);

                if (probs[maxIndex] < threshold) {
                    continue;
                }
                indices.push(maxIndex);
            }

            for (const index of indices) {

                // Some class has a high enough probability
                /** @type {number[]} */
                let box = bbox[j].data;

                // convert to [x0, y0, x1, y1] format
                box = center_to_corners_format(box)
                if (target_size !== null) {
                    box = box.map((x, i) => x * target_size[(i + 1) % 2])
                }

                info.boxes.push(box);
                info.classes.push(index);
                info.scores.push(probs[index]);
            }
        }
        toReturn.push(info);
    }
    return toReturn;
}


/**
 * Post-processes the outputs of the model (for semantic segmentation).
 * @param {*} outputs Raw outputs of the model.
 * @param {[number, number][]} [target_sizes=null] List of tuples corresponding to the requested final size
 * (height, width) of each prediction. If unset, predictions will not be resized.
 * @returns {{segmentation: Tensor; labels: number[]}[]} The semantic segmentation maps.
 */
export function post_process_semantic_segmentation(outputs, target_sizes = null) {

    const logits = outputs.logits;
    const batch_size = logits.dims[0];

    if (target_sizes !== null && target_sizes.length !== batch_size) {
        throw Error("Make sure that you pass in as many target sizes as the batch dimension of the logits")
    }

    const toReturn = [];
    for (let i = 0; i < batch_size; ++i) {
        const target_size = target_sizes !== null ? target_sizes[i] : null;

        let data = logits[i];

        // 1. If target_size is not null, we need to resize the masks to the target size
        if (target_size !== null) {
            // resize the masks to the target size
            data = interpolate(data, target_size, 'bilinear', false);
        }
        const [height, width] = target_size ?? data.dims.slice(-2);

        const segmentation = new Tensor(
            'int32',
            new Int32Array(height * width),
            [height, width]
        );

        // Buffer to store current largest value
        const buffer = data[0].data;
        const segmentation_data = segmentation.data;
        for (let j = 1; j < data.dims[0]; ++j) {
            const row = data[j].data;
            for (let k = 0; k < row.length; ++k) {
                if (row[k] > buffer[k]) {
                    buffer[k] = row[k];
                    segmentation_data[k] = j;
                }
            }
        }

        // Store which objects have labels
        // This is much more efficient that creating a set of the final values
        const hasLabel = new Array(data.dims[0]);
        for (let j = 0; j < segmentation_data.length; ++j) {
            const index = segmentation_data[j];
            hasLabel[index] = index;
        }
        /** @type {number[]} The unique list of labels that were detected */
        const labels = hasLabel.filter(x => x !== undefined);

        toReturn.push({ segmentation, labels });
    }
    return toReturn;
}


/**
 * Binarize the given masks using `object_mask_threshold`, it returns the associated values of `masks`, `scores` and `labels`.
 * @param {Tensor} class_logits The class logits.
 * @param {Tensor} mask_logits The mask logits.
 * @param {number} object_mask_threshold A number between 0 and 1 used to binarize the masks.
 * @param {number} num_labels The number of labels.
 * @returns {[Tensor[], number[], number[]]} The binarized masks, the scores, and the labels.
 * @private
 */
function remove_low_and_no_objects(class_logits, mask_logits, object_mask_threshold, num_labels) {

    const mask_probs_item = [];
    const pred_scores_item = [];
    const pred_labels_item = [];

    for (let j = 0; j < class_logits.dims[0]; ++j) {
        const cls = class_logits[j];
        const mask = mask_logits[j];

        const pred_label = max(cls.data)[1];
        if (pred_label === num_labels) {
            // Is the background, so we ignore it
            continue;
        }

        const scores = softmax(cls.data);
        const pred_score = scores[pred_label];
        if (pred_score > object_mask_threshold) {
            mask_probs_item.push(mask);
            pred_scores_item.push(pred_score);
            pred_labels_item.push(pred_label);
        }
    }

    return [mask_probs_item, pred_scores_item, pred_labels_item];
}

/**
 * Checks whether the segment is valid or not.
 * @param {Int32Array} mask_labels Labels for each pixel in the mask.
 * @param {Tensor[]} mask_probs Probabilities for each pixel in the masks.
 * @param {number} k The class id of the segment.
 * @param {number} mask_threshold The mask threshold.
 * @param {number} overlap_mask_area_threshold The overlap mask area threshold.
 * @returns {[boolean, number[]]} Whether the segment is valid or not, and the indices of the valid labels.
 * @private
 */
function check_segment_validity(
    mask_labels,
    mask_probs,
    k,
    mask_threshold = 0.5,
    overlap_mask_area_threshold = 0.8
) {
    // mask_k is a 1D array of indices, indicating where the mask is equal to k
    const mask_k = [];
    let mask_k_area = 0;
    let original_area = 0;

    const mask_probs_k_data = mask_probs[k].data;

    // Compute the area of all the stuff in query k
    for (let i = 0; i < mask_labels.length; ++i) {
        if (mask_labels[i] === k) {
            mask_k.push(i);
            ++mask_k_area;
        }

        if (mask_probs_k_data[i] >= mask_threshold) {
            ++original_area;
        }
    }
    let mask_exists = mask_k_area > 0 && original_area > 0;

    // Eliminate disconnected tiny segments
    if (mask_exists) {
        // Perform additional check
        let area_ratio = mask_k_area / original_area;
        mask_exists = area_ratio > overlap_mask_area_threshold;
    }

    return [mask_exists, mask_k]
}

/**
 * Computes the segments.
 * @param {Tensor[]} mask_probs The mask probabilities.
 * @param {number[]} pred_scores The predicted scores.
 * @param {number[]} pred_labels The predicted labels.
 * @param {number} mask_threshold The mask threshold.
 * @param {number} overlap_mask_area_threshold The overlap mask area threshold.
 * @param {Set<number>} label_ids_to_fuse The label ids to fuse.
 * @param {number[]} target_size The target size of the image.
 * @returns {[Tensor, Array<{id: number, label_id: number, score: number}>]} The computed segments.
 * @private
 */
function compute_segments(
    mask_probs,
    pred_scores,
    pred_labels,
    mask_threshold,
    overlap_mask_area_threshold,
    label_ids_to_fuse = null,
    target_size = null,
) {
    const [height, width] = target_size ?? mask_probs[0].dims;

    const segmentation = new Tensor(
        'int32',
        new Int32Array(height * width),
        [height, width]
    );
    const segments = [];

    // 1. If target_size is not null, we need to resize the masks to the target size
    if (target_size !== null) {
        // resize the masks to the target size
        for (let i = 0; i < mask_probs.length; ++i) {
            mask_probs[i] = interpolate(mask_probs[i], target_size, 'bilinear', false);
        }
    }

    // 2. Weigh each mask by its prediction score
    // NOTE: `mask_probs` is updated in-place
    // 
    // Temporary storage for the best label/scores for each pixel ([height, width]):
    const mask_labels = new Int32Array(mask_probs[0].data.length);
    const bestScores = new Float32Array(mask_probs[0].data.length);

    for (let i = 0; i < mask_probs.length; ++i) {
        let score = pred_scores[i];

        const mask_probs_i_data = mask_probs[i].data;

        for (let j = 0; j < mask_probs_i_data.length; ++j) {
            mask_probs_i_data[j] *= score
            if (mask_probs_i_data[j] > bestScores[j]) {
                mask_labels[j] = i;
                bestScores[j] = mask_probs_i_data[j];
            }
        }
    }

    let current_segment_id = 0;

    // let stuff_memory_list = {}
    const segmentation_data = segmentation.data;
    for (let k = 0; k < pred_labels.length; ++k) {
        const pred_class = pred_labels[k];

        // TODO add `should_fuse`
        // let should_fuse = pred_class in label_ids_to_fuse

        // Check if mask exists and large enough to be a segment
        const [mask_exists, mask_k] = check_segment_validity(
            mask_labels,
            mask_probs,
            k,
            mask_threshold,
            overlap_mask_area_threshold
        )

        if (!mask_exists) {
            // Nothing to see here
            continue;
        }

        // TODO
        // if (pred_class in stuff_memory_list) {
        //     current_segment_id = stuff_memory_list[pred_class]
        // } else {
        //     current_segment_id += 1;
        // }
        ++current_segment_id;


        // Add current object segment to final segmentation map
        for (const index of mask_k) {
            segmentation_data[index] = current_segment_id;
        }

        segments.push({
            id: current_segment_id,
            label_id: pred_class,
            // was_fused: should_fuse, TODO
            score: pred_scores[k],
        })

        // TODO
        // if(should_fuse){
        //     stuff_memory_list[pred_class] = current_segment_id
        // }
    }

    return [segmentation, segments];
}

/**
 * Rescales the image so that the following conditions are met:
 *
 * 1. Both dimensions (height and width) are divisible by 'factor'.
 * 2. The total number of pixels is within the range ['min_pixels', 'max_pixels'].
 * 3. The aspect ratio of the image is maintained as closely as possible.
 * 
 * @param {number} height The height of the image.
 * @param {number} width The width of the image.
 * @param {number} [factor=28] The factor to use for resizing.
 * @param {number} [min_pixels=56*56] The minimum number of pixels.
 * @param {number} [max_pixels=14*14*4*1280] The maximum number of pixels.
 * @returns {[number, number]} The new height and width of the image.
 * @throws {Error} If the height or width is smaller than the factor.
 */
function smart_resize(height, width, factor = 28, min_pixels = 56 * 56, max_pixels = 14 * 14 * 4 * 1280) {

    if (height < factor || width < factor) {
        throw new Error(`height:${height} or width:${width} must be larger than factor:${factor}`);
    } else if (Math.max(height, width) / Math.min(height, width) > 200) {
        throw new Error(
            `absolute aspect ratio must be smaller than 200, got ${Math.max(height, width) / Math.min(height, width)}`
        );
    }

    let h_bar = Math.round(height / factor) * factor;
    let w_bar = Math.round(width / factor) * factor;

    if (h_bar * w_bar > max_pixels) {
        const beta = Math.sqrt((height * width) / max_pixels);
        h_bar = Math.floor((height / beta) / factor) * factor;
        w_bar = Math.floor((width / beta) / factor) * factor;
    } else if (h_bar * w_bar < min_pixels) {
        const beta = Math.sqrt(min_pixels / (height * width));
        h_bar = Math.ceil((height * beta) / factor) * factor;
        w_bar = Math.ceil((width * beta) / factor) * factor;
    }

    return [h_bar, w_bar];
}


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
export function post_process_panoptic_segmentation(
    outputs,
    threshold = 0.5,
    mask_threshold = 0.5,
    overlap_mask_area_threshold = 0.8,
    label_ids_to_fuse = null,
    target_sizes = null,
) {
    if (label_ids_to_fuse === null) {
        console.warn("`label_ids_to_fuse` unset. No instance will be fused.")
        label_ids_to_fuse = new Set();
    }

    const class_queries_logits = outputs.class_queries_logits ?? outputs.logits; // [batch_size, num_queries, num_classes+1]
    const masks_queries_logits = outputs.masks_queries_logits ?? outputs.pred_masks; // [batch_size, num_queries, height, width]

    const mask_probs = masks_queries_logits.sigmoid()  // [batch_size, num_queries, height, width]

    let [batch_size, num_queries, num_labels] = class_queries_logits.dims;
    num_labels -= 1; // Remove last class (background)

    if (target_sizes !== null && target_sizes.length !== batch_size) {
        throw Error("Make sure that you pass in as many target sizes as the batch dimension of the logits")
    }

    let toReturn = [];
    for (let i = 0; i < batch_size; ++i) {
        let target_size = target_sizes !== null ? target_sizes[i] : null;

        let class_logits = class_queries_logits[i];
        let mask_logits = mask_probs[i];

        let [mask_probs_item, pred_scores_item, pred_labels_item] = remove_low_and_no_objects(class_logits, mask_logits, threshold, num_labels);

        if (pred_labels_item.length === 0) {
            // No mask found
            let [height, width] = target_size ?? mask_logits.dims.slice(-2);

            let segmentation = new Tensor(
                'int32',
                new Int32Array(height * width).fill(-1),
                [height, width]
            )
            toReturn.push({
                segmentation: segmentation,
                segments_info: []
            });
            continue;
        }


        // Get segmentation map and segment information of batch item
        let [segmentation, segments] = compute_segments(
            mask_probs_item,
            pred_scores_item,
            pred_labels_item,
            mask_threshold,
            overlap_mask_area_threshold,
            label_ids_to_fuse,
            target_size,
        )

        toReturn.push({
            segmentation: segmentation,
            segments_info: segments
        })
    }

    return toReturn;
}


/**
 * Post-processes the outputs of the model (for instance segmentation).
 * @param {*} outputs Raw outputs of the model.
 * @param {number} [threshold=0.5] The probability score threshold to keep predicted instance masks.
 * @param {[number, number][]} [target_sizes=null] List of tuples corresponding to the requested final size
 * (height, width) of each prediction. If unset, predictions will not be resized.
 * @returns {Array<{ segmentation: Tensor, segments_info: Array<{id: number, label_id: number, score: number}>}>}
 */
export function post_process_instance_segmentation(outputs, threshold = 0.5, target_sizes = null) {
    throw new Error('`post_process_instance_segmentation` is not yet implemented.');
}


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

export class ImageProcessor extends Callable {

    /**
     * Constructs a new `ImageProcessor`.
     * @param {ImageProcessorConfig} config The configuration object.
     */
    constructor(config) {
        super();

        this.image_mean = config.image_mean ?? config.mean;
        this.image_std = config.image_std ?? config.std;

        this.resample = config.resample ?? 2; // 2 => bilinear
        this.do_rescale = config.do_rescale ?? true;
        this.rescale_factor = config.rescale_factor ?? (1 / 255);
        this.do_normalize = config.do_normalize;

        this.do_thumbnail = config.do_thumbnail;
        this.size = config.size ?? config.image_size;
        this.do_resize = config.do_resize ?? (this.size !== undefined);
        // @ts-expect-error TS2339
        this.size_divisibility = config.size_divisibility ?? config.size_divisor;

        this.do_center_crop = config.do_center_crop;
        // @ts-expect-error TS2339
        this.crop_size = config.crop_size;
        // @ts-expect-error TS2339
        this.do_convert_rgb = config.do_convert_rgb ?? true;
        // @ts-expect-error TS2339
        this.do_crop_margin = config.do_crop_margin;

        // @ts-expect-error TS2339
        this.pad_size = config.pad_size;
        // @ts-expect-error TS2339
        this.do_pad = config.do_pad;
        // @ts-expect-error TS2339
        this.min_pixels = config.min_pixels;
        // @ts-expect-error TS2339
        this.max_pixels = config.max_pixels;

        if (this.do_pad && !this.pad_size && this.size && this.size.width !== undefined && this.size.height !== undefined) {
            // Should pad, but no pad size specified
            // We infer the pad size from the resize size
            this.pad_size = this.size
        }

        this.do_flip_channel_order = config.do_flip_channel_order ?? false;

        this.config = config;
    }

    /**
     * Resize the image to make a thumbnail. The image is resized so that no dimension is larger than any
     * corresponding dimension of the specified size.
     * @param {RawImage} image The image to be resized.
     * @param {{height:number, width:number}} size The size `{"height": h, "width": w}` to resize the image to.
     * @param {string | 0 | 1 | 2 | 3 | 4 | 5} [resample=2] The resampling filter to use.
     * @returns {Promise<RawImage>} The resized image.
     */
    async thumbnail(image, size, resample = 2) {
        const input_height = image.height;
        const input_width = image.width;

        const output_height = size.height;
        const output_width = size.width;

        // We always resize to the smallest of either the input or output size.
        let height = Math.min(input_height, output_height)
        let width = Math.min(input_width, output_width)

        if (height === input_height && width === input_width) {
            return image;
        }
        if (input_height > input_width) {
            width = Math.floor(input_width * height / input_height);
        } else if (input_width > input_height) {
            height = Math.floor(input_height * width / input_width);
        }
        return await image.resize(width, height, { resample });
    }


    /**
     * Crops the margin of the image. Gray pixels are considered margin (i.e., pixels with a value below the threshold).
     * @param {RawImage} image The image to be cropped.
     * @param {number} gray_threshold Value below which pixels are considered to be gray.
     * @returns {Promise<RawImage>} The cropped image.
     */
    async crop_margin(image, gray_threshold = 200) {

        const gray_image = image.clone().grayscale();

        const minValue = min(gray_image.data)[0];
        const maxValue = max(gray_image.data)[0];
        const diff = maxValue - minValue;

        if (diff === 0) {
            return image;
        }

        const threshold = gray_threshold / 255;

        let x_min = gray_image.width, y_min = gray_image.height, x_max = 0, y_max = 0;
        const gray_image_data = gray_image.data;
        for (let j = 0; j < gray_image.height; ++j) {
            const row = j * gray_image.width;
            for (let i = 0; i < gray_image.width; ++i) {
                if ((gray_image_data[row + i] - minValue) / diff < threshold) {
                    // We have a non-zero pixel, so we update the min/max values accordingly
                    x_min = Math.min(x_min, i);
                    y_min = Math.min(y_min, j);
                    x_max = Math.max(x_max, i);
                    y_max = Math.max(y_max, j);
                }
            }
        }

        image = await image.crop([x_min, y_min, x_max, y_max]);
        return image;
    }

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
    pad_image(pixelData, imgDims, padSize, {
        mode = 'constant',
        center = false,
        constant_values = 0,
    } = {}) {
        const [imageHeight, imageWidth, imageChannels] = imgDims;

        let paddedImageWidth, paddedImageHeight;
        if (typeof padSize === 'number') {
            paddedImageWidth = padSize;
            paddedImageHeight = padSize;
        } else if (padSize === 'square') {
            paddedImageWidth = paddedImageHeight = Math.max(imageHeight, imageWidth);
        } else {
            paddedImageWidth = padSize.width;
            paddedImageHeight = padSize.height;
        }

        // Only add padding if there is a difference in size
        if (paddedImageWidth !== imageWidth || paddedImageHeight !== imageHeight) {
            const paddedPixelData = new Float32Array(paddedImageWidth * paddedImageHeight * imageChannels);
            if (Array.isArray(constant_values)) {
                // Fill with constant values, cycling through the array
                for (let i = 0; i < paddedPixelData.length; ++i) {
                    paddedPixelData[i] = constant_values[i % imageChannels];
                }
            } else if (constant_values !== 0) {
                paddedPixelData.fill(constant_values);
            }

            const [left, top] = center
                ? [Math.floor((paddedImageWidth - imageWidth) / 2), Math.floor((paddedImageHeight - imageHeight) / 2)]
                : [0, 0];

            // Copy the original image into the padded image
            for (let i = 0; i < imageHeight; ++i) {
                const a = (i + top) * paddedImageWidth;
                const b = i * imageWidth;
                for (let j = 0; j < imageWidth; ++j) {
                    const c = (a + j + left) * imageChannels;
                    const d = (b + j) * imageChannels;
                    for (let k = 0; k < imageChannels; ++k) {
                        paddedPixelData[c + k] = pixelData[d + k];
                    }
                }
            }

            if (mode === 'symmetric') {
                if (center) {
                    throw new Error('`center` padding is not supported when `mode` is set to `symmetric`.');
                    // TODO: Implement this
                }
                const h1 = imageHeight - 1;
                const w1 = imageWidth - 1;
                for (let i = 0; i < paddedImageHeight; ++i) {
                    const a = i * paddedImageWidth;
                    const b = calculateReflectOffset(i, h1) * imageWidth;

                    for (let j = 0; j < paddedImageWidth; ++j) {
                        if (i < imageHeight && j < imageWidth) continue; // Do not overwrite original image
                        const c = (a + j) * imageChannels;
                        const d = (b + calculateReflectOffset(j, w1)) * imageChannels;

                        // Copy channel-wise
                        for (let k = 0; k < imageChannels; ++k) {
                            paddedPixelData[c + k] = pixelData[d + k];
                        }
                    }
                }
            }


            // Update pixel data and image dimensions
            pixelData = paddedPixelData;
            imgDims = [paddedImageHeight, paddedImageWidth, imageChannels]
        }
        return [pixelData, imgDims];
    }

    /**
     * Rescale the image' pixel values by `this.rescale_factor`.
     * @param {Float32Array} pixelData The pixel data to rescale.
     * @returns {void}
     */
    rescale(pixelData) {
        for (let i = 0; i < pixelData.length; ++i) {
            pixelData[i] = this.rescale_factor * pixelData[i];
        }
    }

    /**
     * Find the target (width, height) dimension of the output image after
     * resizing given the input image and the desired size.
     * @param {RawImage} image The image to resize.
     * @param {any} size The size to use for resizing the image. 
     * @returns {[number, number]} The target (width, height) dimension of the output image after resizing.
     */
    get_resize_output_image_size(image, size) {
        // `size` comes in many forms, so we need to handle them all here:
        // 1. `size` is an integer, in which case we resize the image to be a square 

        const [srcWidth, srcHeight] = image.size;

        let shortest_edge;
        let longest_edge;

        if (this.do_thumbnail) {
            // NOTE: custom logic for `Donut` models
            const { height, width } = size;
            shortest_edge = Math.min(height, width)
        }
        // Support both formats for backwards compatibility
        else if (Number.isInteger(size)) {
            shortest_edge = size;
            // @ts-expect-error TS2339
            longest_edge = this.config.max_size ?? shortest_edge;

        } else if (size !== undefined) {
            // Extract known properties from `size`
            shortest_edge = size.shortest_edge;
            longest_edge = size.longest_edge;
        }

        // If `longest_edge` and `shortest_edge` are set, maintain aspect ratio and resize to `shortest_edge`
        // while keeping the largest dimension <= `longest_edge`
        if (shortest_edge !== undefined || longest_edge !== undefined) {
            // http://opensourcehacker.com/2011/12/01/calculate-aspect-ratio-conserving-resize-for-images-in-javascript/
            // Try resize so that shortest edge is `shortest_edge` (target)
            const shortResizeFactor = shortest_edge === undefined
                ? 1 // If `shortest_edge` is not set, don't upscale
                : Math.max(shortest_edge / srcWidth, shortest_edge / srcHeight);

            const newWidth = srcWidth * shortResizeFactor;
            const newHeight = srcHeight * shortResizeFactor;

            // The new width and height might be greater than `longest_edge`, so
            // we downscale again to ensure the largest dimension is `longest_edge` 
            const longResizeFactor = longest_edge === undefined
                ? 1 // If `longest_edge` is not set, don't downscale
                : Math.min(longest_edge / newWidth, longest_edge / newHeight);

            // To avoid certain floating point precision issues, we round to 2 decimal places
            let finalWidth = Math.floor(Number((newWidth * longResizeFactor).toFixed(2)));
            let finalHeight = Math.floor(Number((newHeight * longResizeFactor).toFixed(2)));

            if (this.size_divisibility !== undefined) {
                [finalWidth, finalHeight] = enforce_size_divisibility([finalWidth, finalHeight], this.size_divisibility)
            }
            return [finalWidth, finalHeight];

        } else if (size !== undefined && size.width !== undefined && size.height !== undefined) {
            // If `width` and `height` are set, resize to those dimensions

            let newWidth = size.width;
            let newHeight = size.height;

            // Custom for DPT models
            if (this.config.keep_aspect_ratio && this.config.ensure_multiple_of) {

                // determine new height and width
                let scale_height = newHeight / srcHeight;
                let scale_width = newWidth / srcWidth;

                // scale as little as possible
                if (Math.abs(1 - scale_width) < Math.abs(1 - scale_height)) {
                    // fit width
                    scale_height = scale_width;
                } else {
                    // fit height
                    scale_width = scale_height;
                }

                newHeight = constraint_to_multiple_of(scale_height * srcHeight, this.config.ensure_multiple_of);
                newWidth = constraint_to_multiple_of(scale_width * srcWidth, this.config.ensure_multiple_of);
            }

            return [newWidth, newHeight];

        } else if (this.size_divisibility !== undefined) {
            return enforce_size_divisibility([srcWidth, srcHeight], this.size_divisibility);
        } else if (this.min_pixels !== undefined && this.max_pixels !== undefined) {
            // Custom resize logic for Qwen2-VL models
            // @ts-expect-error TS2339
            const factor = this.config.patch_size * this.config.merge_size;
            return smart_resize(srcHeight, srcWidth, factor, this.min_pixels, this.max_pixels);
        } else {
            throw new Error(`Could not resize image due to unsupported \`this.size\` option in config: ${JSON.stringify(size)}`);
        }
    }

    /**
     * Resizes the image.
     * @param {RawImage} image The image to resize.
     * @returns {Promise<RawImage>} The resized image.
     */
    async resize(image) {
        const [newWidth, newHeight] = this.get_resize_output_image_size(image, this.size);
        return await image.resize(newWidth, newHeight, {
            // @ts-expect-error TS2322
            resample: this.resample,
        });
    }

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
    async preprocess(image, {
        do_normalize = null,
        do_pad = null,
        do_convert_rgb = null,
        do_convert_grayscale = null,
        do_flip_channel_order = null,
    } = {}) {
        if (this.do_crop_margin) {
            // NOTE: Specific to nougat processors. This is done before resizing,
            // and can be interpreted as a pre-preprocessing step.
            image = await this.crop_margin(image);
        }

        const [srcWidth, srcHeight] = image.size; // original image size

        // Convert image to RGB if specified in config.
        if (do_convert_rgb ?? this.do_convert_rgb) {
            image = image.rgb();
        } else if (do_convert_grayscale) {
            image = image.grayscale();
        }

        // TODO:
        // For efficiency reasons, it might be best to merge the resize and center crop operations into one.

        // Resize all images
        if (this.do_resize) {
            image = await this.resize(image);
        }

        // Resize the image using thumbnail method.
        if (this.do_thumbnail) {
            // @ts-expect-error TS2345
            image = await this.thumbnail(image, this.size, this.resample);
        }

        if (this.do_center_crop) {

            let crop_width;
            let crop_height;
            if (Number.isInteger(this.crop_size)) {
                crop_width = this.crop_size;
                crop_height = this.crop_size;
            } else {
                crop_width = this.crop_size.width;
                crop_height = this.crop_size.height;
            }

            image = await image.center_crop(crop_width, crop_height);
        }

        /** @type {HeightWidth} */
        const reshaped_input_size = [image.height, image.width];

        // NOTE: All pixel-level manipulation (i.e., modifying `pixelData`)
        // occurs with data in the hwc format (height, width, channels), 
        // to emulate the behavior of the original Python code (w/ numpy).
        /** @type {Float32Array} */
        let pixelData = Float32Array.from(image.data);
        let imgDims = [image.height, image.width, image.channels];

        if (this.do_rescale) {
            this.rescale(pixelData);
        }

        if (do_normalize ?? this.do_normalize) {
            let image_mean = this.image_mean;
            if (!Array.isArray(this.image_mean)) {
                image_mean = new Array(image.channels).fill(image_mean);
            }

            let image_std = this.image_std;
            if (!Array.isArray(this.image_std)) {
                image_std = new Array(image.channels).fill(image_mean);
            }

            if (image_mean.length !== image.channels || image_std.length !== image.channels) {
                throw new Error(`When set to arrays, the length of \`image_mean\` (${image_mean.length}) and \`image_std\` (${image_std.length}) must match the number of channels in the image (${image.channels}).`);
            }

            for (let i = 0; i < pixelData.length; i += image.channels) {
                for (let j = 0; j < image.channels; ++j) {
                    pixelData[i + j] = (pixelData[i + j] - image_mean[j]) / image_std[j];
                }
            }
        }

        // do padding after rescaling/normalizing
        if (do_pad ?? this.do_pad) {
            if (this.pad_size) {
                const padded = this.pad_image(pixelData, [image.height, image.width, image.channels], this.pad_size);
                [pixelData, imgDims] = padded; // Update pixel data and image dimensions
            } else if (this.size_divisibility) {
                const [paddedWidth, paddedHeight] = enforce_size_divisibility([imgDims[1], imgDims[0]], this.size_divisibility);
                [pixelData, imgDims] = this.pad_image(pixelData, imgDims, { width: paddedWidth, height: paddedHeight });
            }
        }

        if (do_flip_channel_order ?? this.do_flip_channel_order) {
            if (imgDims[2] !== 3) {
                throw new Error('Flipping channel order is only supported for RGB images.');
            }
            // Convert RGB to BGR
            for (let i = 0; i < pixelData.length; i += 3) {
                const temp = pixelData[i];
                pixelData[i] = pixelData[i + 2];
                pixelData[i + 2] = temp;
            }
        }

        const pixel_values = new Tensor('float32', pixelData, imgDims)
            .permute(2, 0, 1); // convert to channel dimension format (hwc -> chw)

        return {
            original_size: [srcHeight, srcWidth],
            reshaped_input_size: reshaped_input_size,
            pixel_values,
        }
    }

    /**
     * Calls the feature extraction process on an array of images,
     * preprocesses each image, and concatenates the resulting
     * features into a single Tensor.
     * @param {RawImage[]} images The image(s) to extract features from.
     * @param {...any} args Additional arguments.
     * @returns {Promise<ImageProcessorResult>} An object containing the concatenated pixel values (and other metadata) of the preprocessed images.
     */
    async _call(images, ...args) {
        if (!Array.isArray(images)) {
            images = [images];
        }
        /** @type {PreprocessedImage[]} */
        const imageData = await Promise.all(images.map(x => this.preprocess(x)));

        // Stack pixel values
        const pixel_values = stack(imageData.map(x => x.pixel_values), 0);

        return {
            pixel_values,

            // Original sizes of images
            original_sizes: imageData.map(x => x.original_size),

            // Reshaped sizes of images, before padding or cropping
            reshaped_input_sizes: imageData.map(x => x.reshaped_input_size),
        }
    }


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
    static async from_pretrained(pretrained_model_name_or_path, options) {
        const preprocessorConfig = await getModelJSON(pretrained_model_name_or_path, IMAGE_PROCESSOR_NAME, true, options);
        return new this(preprocessorConfig);
    }
}
