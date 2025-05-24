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
    _call(images: import("../../utils/image.js").RawImage[]): Promise<GroundingDinoFeatureExtractorResult>;
}
export type GroundingDinoFeatureExtractorResultProps = {
    pixel_mask: import("../../utils/tensor.js").Tensor;
};
export type GroundingDinoFeatureExtractorResult = import("../../base/image_processors_utils.js").ImageProcessorResult & GroundingDinoFeatureExtractorResultProps;
import { ImageProcessor } from "../../base/image_processors_utils.js";
//# sourceMappingURL=image_processing_grounding_dino.d.ts.map