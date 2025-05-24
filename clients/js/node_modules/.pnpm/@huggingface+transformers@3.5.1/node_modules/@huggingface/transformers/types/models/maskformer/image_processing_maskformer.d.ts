export class MaskFormerImageProcessor extends ImageProcessor {
    post_process_panoptic_segmentation(outputs: any, threshold?: number, mask_threshold?: number, overlap_mask_area_threshold?: number, label_ids_to_fuse?: Set<number>, target_sizes?: [number, number][]): Array<{
        segmentation: import("../../transformers.js").Tensor;
        segments_info: Array<{
            id: number;
            label_id: number;
            score: number;
        }>;
    }>;
    post_process_instance_segmentation(outputs: any, threshold?: number, target_sizes?: [number, number][]): Array<{
        segmentation: import("../../transformers.js").Tensor;
        segments_info: Array<{
            id: number;
            label_id: number;
            score: number;
        }>;
    }>;
}
export class MaskFormerFeatureExtractor extends MaskFormerImageProcessor {
}
import { ImageProcessor } from "../../base/image_processors_utils.js";
//# sourceMappingURL=image_processing_maskformer.d.ts.map