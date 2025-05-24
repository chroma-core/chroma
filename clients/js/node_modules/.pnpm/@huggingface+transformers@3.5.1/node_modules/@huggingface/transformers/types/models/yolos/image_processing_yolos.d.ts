export class YolosImageProcessor extends ImageProcessor {
    post_process_object_detection(outputs: {
        logits: import("../../transformers.js").Tensor;
        pred_boxes: import("../../transformers.js").Tensor;
    }, threshold?: number, target_sizes?: [number, number][], is_zero_shot?: boolean): any[];
}
export class YolosFeatureExtractor extends YolosImageProcessor {
}
import { ImageProcessor } from "../../base/image_processors_utils.js";
//# sourceMappingURL=image_processing_yolos.d.ts.map