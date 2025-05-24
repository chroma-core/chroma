
import {
    ImageProcessor,
} from "../../base/image_processors_utils.js";

export class VLMImageProcessor extends ImageProcessor {
    constructor(config) {
        super({
            do_pad: true,
            pad_size: {
                width: config.image_size,
                height: config.image_size,
            },
            ...config,
        });
        // @ts-expect-error TS2339
        this.constant_values = this.config.background_color.map(x => x * this.rescale_factor)
    }

    pad_image(pixelData, imgDims, padSize, options) {
        return super.pad_image(pixelData, imgDims, padSize, {
            constant_values: this.constant_values,
            center: true,
            ...options,
        });
    }
}
