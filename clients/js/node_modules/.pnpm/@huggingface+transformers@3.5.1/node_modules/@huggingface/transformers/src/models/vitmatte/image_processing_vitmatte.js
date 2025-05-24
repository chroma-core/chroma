import { 
    ImageProcessor,
} from "../../base/image_processors_utils.js";

import {
    stack,
    cat,
} from "../../utils/tensor.js";

export class VitMatteImageProcessor extends ImageProcessor {
    /**
     * Calls the feature extraction process on an array of images, preprocesses
     * each image, and concatenates the resulting features into a single Tensor.
     * @param {import("../../utils/image.js").RawImage[]} images The image(s) to extract features from.
     * @param {import("../../utils/image.js").RawImage[]} trimaps The trimaps(s) to extract features from.
     * @returns {Promise<import("../../base/image_processors_utils.js").ImageProcessorResult>} An object containing the concatenated pixel values of the preprocessed images.
     */
    async _call(images, trimaps) {
        if (!Array.isArray(images)) {
            images = [images];
        }
        if (!Array.isArray(trimaps)) {
            trimaps = [trimaps];
        }

        const imageData = await Promise.all(images.map(x => this.preprocess(x)));
        const trimapData = await Promise.all(trimaps.map(x => this.preprocess(x, {
            do_normalize: false,
            do_convert_rgb: false,
            do_convert_grayscale: true,
        })));


        // Stack pixel values
        const pixel_values = stack(imageData.map(
            // Concatenate images and trimaps
            (x, i) => cat([x.pixel_values, trimapData[i].pixel_values], 0)
        ), 0);

        return {
            pixel_values,

            // Original sizes of images
            original_sizes: imageData.map(x => x.original_size),

            // Reshaped sizes of images, before padding or cropping
            reshaped_input_sizes: imageData.map(x => x.reshaped_input_size),
        }
    }
}
