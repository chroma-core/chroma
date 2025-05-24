export class VitPoseImageProcessor extends ImageProcessor {
    /**
     * Transform the heatmaps into keypoint predictions and transform them back to the image.
     * NOTE: This is a naive implementation and does not include advanced post-processing techniques,
     * so the results may not be as accurate as the original implementation.
     * @param {import('../../utils/tensor.js').Tensor} outputs The model outputs.
     * @param {[number, number, number, number][][]} boxes List or array of bounding boxes for each image.
     * Each box should be a list of 4 floats representing the bounding box coordinates in COCO format (top_left_x, top_left_y, width, height).
     * @returns {{
     *   bbox: [number, number, number, number],
     *   scores: number[],
     *   labels: number[],
     *   keypoints: [number, number][]
     * }[][]} List of keypoints predictions for each image.
     */
    post_process_pose_estimation(outputs: import("../../utils/tensor.js").Tensor, boxes: [number, number, number, number][][], { threshold, }?: {
        threshold?: any;
    }): {
        bbox: [number, number, number, number];
        scores: number[];
        labels: number[];
        keypoints: [number, number][];
    }[][];
}
import { ImageProcessor } from "../../base/image_processors_utils.js";
//# sourceMappingURL=image_processing_vitpose.d.ts.map