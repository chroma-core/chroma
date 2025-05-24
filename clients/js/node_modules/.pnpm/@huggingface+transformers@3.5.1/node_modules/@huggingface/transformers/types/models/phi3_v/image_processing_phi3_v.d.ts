export class Phi3VImageProcessor extends ImageProcessor {
    constructor(config: any);
    _num_crops: any;
    calc_num_image_tokens_from_image_size(width: any, height: any): number;
    _call(images: any, { num_crops, }?: {
        num_crops?: any;
    }): Promise<{
        pixel_values: Tensor;
        original_sizes: any[];
        reshaped_input_sizes: any[];
        image_sizes: Tensor;
        num_img_tokens: number[];
    }>;
}
import { ImageProcessor } from "../../base/image_processors_utils.js";
import { Tensor } from "../../utils/tensor.js";
//# sourceMappingURL=image_processing_phi3_v.d.ts.map