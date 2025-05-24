import {
    ImageProcessor,
} from "../../base/image_processors_utils.js";

export class JinaCLIPImageProcessor extends ImageProcessor {
    constructor(config) {
        // JinaCLIPImageProcessor uses a custom preprocessor_config.json, so we configure it here
        const { resize_mode, fill_color, interpolation, size, ...other } = config;

        const new_size = resize_mode === 'squash'
            ? { width: size, height: size }
            : resize_mode === 'shortest'
                ? { shortest_edge: size }
                : { longest_edge: size };

        const resample = interpolation === 'bicubic' ? 3 : 2;
        super({
            ...other,
            size: new_size,
            resample,
            do_center_crop: true,
            crop_size: size,
            do_normalize: true,
        });
    }
}
