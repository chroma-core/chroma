/**
 * @typedef {object} DetrFeatureExtractorResultProps
 * @property {import('../../utils/tensor.js').Tensor} pixel_mask
 * @typedef {import('../../base/image_processors_utils.js').ImageProcessorResult & DetrFeatureExtractorResultProps} DetrFeatureExtractorResult
 */
export class DetrImageProcessor extends ImageProcessor {
    /**
     * Calls the feature extraction process on an array of images, preprocesses
     * each image, and concatenates the resulting features into a single Tensor.
     * @param {import('../../utils/image.js').RawImage[]} images The image(s) to extract features from.
     * @returns {Promise<DetrFeatureExtractorResult>} An object containing the concatenated pixel values of the preprocessed images.
     */
    _call(images: import("../../utils/image.js").RawImage[]): Promise<DetrFeatureExtractorResult>;
    post_process_object_detection(outputs: {
        logits: import("../../utils/tensor.js").Tensor;
        pred_boxes: import("../../utils/tensor.js").Tensor;
    }, threshold?: number, target_sizes?: [number, number][], is_zero_shot?: boolean): any[];
    post_process_panoptic_segmentation(outputs: any, threshold?: number, mask_threshold?: number, overlap_mask_area_threshold?: number, label_ids_to_fuse?: Set<number>, target_sizes?: [number, number][]): Array<{
        segmentation: import("../../utils/tensor.js").Tensor;
        segments_info: Array<{
            id: number;
            label_id: number;
            score: number;
        }>;
    }>;
    post_process_instance_segmentation(outputs: any, threshold?: number, target_sizes?: [number, number][]): Array<{
        segmentation: import("../../utils/tensor.js").Tensor;
        segments_info: Array<{
            id: number;
            label_id: number;
            score: number;
        }>;
    }>;
}
export class DetrFeatureExtractor extends DetrImageProcessor {
}
export type DetrFeatureExtractorResultProps = {
    pixel_mask: import("../../utils/tensor.js").Tensor;
};
export type DetrFeatureExtractorResult = import("../../base/image_processors_utils.js").ImageProcessorResult & DetrFeatureExtractorResultProps;
import { ImageProcessor } from "../../base/image_processors_utils.js";
//# sourceMappingURL=image_processing_detr.d.ts.map