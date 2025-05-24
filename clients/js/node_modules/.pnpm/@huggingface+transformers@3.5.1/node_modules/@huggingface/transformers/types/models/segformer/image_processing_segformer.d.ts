export class SegformerImageProcessor extends ImageProcessor {
    post_process_semantic_segmentation(outputs: any, target_sizes?: [number, number][]): {
        segmentation: import("../../transformers.js").Tensor;
        labels: number[];
    }[];
}
export class SegformerFeatureExtractor extends SegformerImageProcessor {
}
import { ImageProcessor } from "../../base/image_processors_utils.js";
//# sourceMappingURL=image_processing_segformer.d.ts.map