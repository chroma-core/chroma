import { 
    ImageProcessor,
} from "../../base/image_processors_utils.js";

export class Swin2SRImageProcessor extends ImageProcessor {
    pad_image(pixelData, imgDims, padSize, options = {}) {
        // NOTE: In this case, `padSize` represents the size of the sliding window for the local attention.
        // In other words, the image is padded so that its width and height are multiples of `padSize`.
        const [imageHeight, imageWidth, imageChannels] = imgDims;

        return super.pad_image(pixelData, imgDims, {
            // NOTE: For Swin2SR models, the original python implementation adds padding even when the image's width/height is already
            // a multiple of `pad_size`. However, this is most likely a bug (PR: https://github.com/mv-lab/swin2sr/pull/19).
            // For this reason, we only add padding when the image's width/height is not a multiple of `pad_size`.
            width: imageWidth + (padSize - imageWidth % padSize) % padSize,
            height: imageHeight + (padSize - imageHeight % padSize) % padSize,
        }, {
            mode: 'symmetric',
            center: false,
            constant_values: -1,
            ...options,
        })
    }
}