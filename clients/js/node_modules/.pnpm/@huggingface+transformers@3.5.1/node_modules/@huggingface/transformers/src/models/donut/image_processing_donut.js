import { 
    ImageProcessor,
} from "../../base/image_processors_utils.js";

export class DonutImageProcessor extends ImageProcessor {
    pad_image(pixelData, imgDims, padSize, options = {}) {
        const [imageHeight, imageWidth, imageChannels] = imgDims;

        let image_mean = this.image_mean;
        if (!Array.isArray(this.image_mean)) {
            image_mean = new Array(imageChannels).fill(image_mean);
        }

        let image_std = this.image_std;
        if (!Array.isArray(image_std)) {
            image_std = new Array(imageChannels).fill(image_mean);
        }

        const constant_values = image_mean.map((x, i) => - x / image_std[i]);

        return super.pad_image(pixelData, imgDims, padSize, {
            center: true,

            // Since normalization is done after padding, we need to use certain constant values to ensure the same behaviour is observed.
            // For more information, see https://github.com/huggingface/transformers/blob/main/src/transformers/models/donut/image_processing_donut.py#L433-L451
            constant_values,
            ...options,
        });
    }
}
export class DonutFeatureExtractor extends DonutImageProcessor { }
