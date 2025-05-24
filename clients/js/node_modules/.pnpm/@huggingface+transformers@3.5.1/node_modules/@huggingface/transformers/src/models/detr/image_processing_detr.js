import { 
    ImageProcessor,
    post_process_object_detection,
    post_process_panoptic_segmentation,
    post_process_instance_segmentation,
} from "../../base/image_processors_utils.js";

import { full } from '../../utils/tensor.js';


/**
 * @typedef {object} DetrFeatureExtractorResultProps
 * @property {import('../../utils/tensor.js').Tensor} pixel_mask
 * @typedef {import('../../base/image_processors_utils.js').ImageProcessorResult & DetrFeatureExtractorResultProps} DetrFeatureExtractorResult
 */

export class DetrImageProcessor extends ImageProcessor {
    /**
     * Calls the feature extraction process on an array of images, preprocesses
     * each image, and concatenates the resulting features into a single Tensor.
     * @param {import('../../utils/image.js').RawImage[]} images The image(s) to extract features from.
     * @returns {Promise<DetrFeatureExtractorResult>} An object containing the concatenated pixel values of the preprocessed images.
     */
    async _call(images) {
        const result = await super._call(images);

        // TODO support differently-sized images, for now assume all images are the same size.
        // TODO support different mask sizes (not just 64x64)
        // Currently, just fill pixel mask with 1s
        const maskSize = [result.pixel_values.dims[0], 64, 64];
        const pixel_mask = full(maskSize, 1n);

        return { ...result, pixel_mask };
    }

    /** @type {typeof post_process_object_detection} */
    post_process_object_detection(...args) {
        return post_process_object_detection(...args);
    }

    /** @type {typeof post_process_panoptic_segmentation} */
    post_process_panoptic_segmentation(...args) {
        return post_process_panoptic_segmentation(...args);
    }

    /** @type {typeof post_process_instance_segmentation} */
    post_process_instance_segmentation(...args) {
        return post_process_instance_segmentation(...args);
    }
}

export class DetrFeatureExtractor extends DetrImageProcessor { } // NOTE: extends DetrImageProcessor  
