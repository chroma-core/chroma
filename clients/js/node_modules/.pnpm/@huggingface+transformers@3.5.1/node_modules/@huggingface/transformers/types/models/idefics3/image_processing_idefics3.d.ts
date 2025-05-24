export class Idefics3ImageProcessor extends ImageProcessor {
    constructor(config: any);
    do_image_splitting: any;
    max_image_size: any;
    /**
     * @typedef {import('../../utils/image.js').RawImage} RawImage
     * @typedef {import('../../utils/tensor.js').Tensor} Tensor
     */
    /**
     * Calculate size to resize images to, to be multiples of `vision_encoder_max_size` while preserving the aspect ratio.
     * @param {Tensor} pixel_values Tensor of the image to resize.
     * @param {number} vision_encoder_max_size Maximum size of the output image. If the image is larger than this size,
     * it will be split into patches of this size, and the original image will be concatenated with the patches, resized to max_size.
     */
    get_resize_for_vision_encoder(pixel_values: import("../../utils/tensor.js").Tensor, vision_encoder_max_size: number): {
        height: number;
        width: number;
    };
    /** @param {RawImage|RawImage[]|RawImage[][]} images */
    _call(images: import("../../utils/image.js").RawImage | import("../../utils/image.js").RawImage[] | import("../../utils/image.js").RawImage[][], { do_image_splitting, return_row_col_info, }?: {
        do_image_splitting?: any;
        return_row_col_info?: boolean;
    }): Promise<{
        rows?: any[][];
        cols?: any[][];
        pixel_values: import("../../utils/tensor.js").Tensor;
        pixel_attention_mask: import("../../utils/tensor.js").Tensor;
        original_sizes: import("../../base/image_processors_utils.js").HeightWidth[];
        reshaped_input_sizes: import("../../base/image_processors_utils.js").HeightWidth[];
    }>;
    split_image(pixel_values: any, { longest_edge }: {
        longest_edge: any;
    }): Promise<{
        frames: any[];
        num_splits_h: number;
        num_splits_w: number;
    }>;
}
import { ImageProcessor } from "../../base/image_processors_utils.js";
//# sourceMappingURL=image_processing_idefics3.d.ts.map