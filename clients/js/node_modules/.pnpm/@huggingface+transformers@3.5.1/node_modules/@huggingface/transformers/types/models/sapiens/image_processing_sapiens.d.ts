export class SapiensImageProcessor extends ImageProcessor {
    post_process_semantic_segmentation(outputs: any, target_sizes?: [number, number][]): {
        segmentation: import("../../transformers.js").Tensor;
        labels: number[];
    }[];
}
export class SapiensFeatureExtractor extends SapiensImageProcessor {
}
import { ImageProcessor } from "../../base/image_processors_utils.js";
//# sourceMappingURL=image_processing_sapiens.d.ts.map