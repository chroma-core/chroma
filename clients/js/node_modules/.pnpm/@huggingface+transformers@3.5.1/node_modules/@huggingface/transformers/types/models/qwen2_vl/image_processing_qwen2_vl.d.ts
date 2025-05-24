export class Qwen2VLImageProcessor extends ImageProcessor {
    _call(images: any, ...args: any[]): Promise<{
        pixel_values: Tensor;
        image_grid_thw: Tensor;
        original_sizes: import("../../base/image_processors_utils.js").HeightWidth[];
        reshaped_input_sizes: import("../../base/image_processors_utils.js").HeightWidth[];
    }>;
}
import { ImageProcessor } from "../../base/image_processors_utils.js";
import { Tensor } from "../../utils/tensor.js";
//# sourceMappingURL=image_processing_qwen2_vl.d.ts.map