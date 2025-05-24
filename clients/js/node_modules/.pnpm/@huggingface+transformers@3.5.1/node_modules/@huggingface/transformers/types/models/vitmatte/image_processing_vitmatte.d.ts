export class VitMatteImageProcessor extends ImageProcessor {
    /**
     * Calls the feature extraction process on an array of images, preprocesses
     * each image, and concatenates the resulting features into a single Tensor.
     * @param {import("../../utils/image.js").RawImage[]} images The image(s) to extract features from.
     * @param {import("../../utils/image.js").RawImage[]} trimaps The trimaps(s) to extract features from.
     * @returns {Promise<import("../../base/image_processors_utils.js").ImageProcessorResult>} An object containing the concatenated pixel values of the preprocessed images.
     */
    _call(images: import("../../utils/image.js").RawImage[], trimaps: import("../../utils/image.js").RawImage[]): Promise<import("../../base/image_processors_utils.js").ImageProcessorResult>;
}
import { ImageProcessor } from "../../base/image_processors_utils.js";
//# sourceMappingURL=image_processing_vitmatte.d.ts.map