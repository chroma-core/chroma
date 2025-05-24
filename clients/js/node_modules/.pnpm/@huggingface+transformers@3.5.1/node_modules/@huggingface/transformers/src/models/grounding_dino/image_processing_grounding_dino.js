
import { 
    ImageProcessor,
} from "../../base/image_processors_utils.js";
import { ones } from '../../utils/tensor.js';


/**
 * @typedef {object} GroundingDinoFeatureExtractorResultProps
 * @property {import('../../utils/tensor.js').Tensor} pixel_mask
 * @typedef {import('../../base/image_processors_utils.js').ImageProcessorResult & GroundingDinoFeatureExtractorResultProps} GroundingDinoFeatureExtractorResult
 */

export class GroundingDinoImageProcessor extends ImageProcessor {
    /**
     * Calls the feature extraction process on an array of images, preprocesses
     * each image, and concatenates the resulting features into a single Tensor.
     * @param {import('../../utils/image.js').RawImage[]} images The image(s) to extract features from.
     * @returns {Promise<GroundingDinoFeatureExtractorResult>} An object containing the concatenated pixel values of the preprocessed images.
     */
    async _call(images) {
        const result = await super._call(images);

        const dims = result.pixel_values.dims;
        const pixel_mask = ones([dims[0], dims[2], dims[3]]);

        return { ...result, pixel_mask };
    }
}
