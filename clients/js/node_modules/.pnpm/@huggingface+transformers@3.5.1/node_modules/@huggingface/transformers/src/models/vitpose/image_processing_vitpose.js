import {
    ImageProcessor,
} from "../../base/image_processors_utils.js";

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
    post_process_pose_estimation(outputs, boxes, {
        threshold = null,
        // TODO:
        // kernel_size = 11,
        // target_sizes = null,
    } = {}) {
        // NOTE: boxes are 3D (batch_size, num_boxes, 4)
        const heatmaps = outputs.tolist();
        const [batch_size, num_classes, height, width] = outputs.dims;

        const results = [];
        for (let b = 0; b < batch_size; ++b) {
            const heatmap = heatmaps[b];
            const bboxes = boxes[b];

            const batch_results = [];
            for (let n = 0; n < bboxes.length; ++n) {
                const bbox = bboxes[n];

                const keypoints = [];
                const scores = [];
                const labels = [];

                const xScale = bbox.at(-2) / width;
                const yScale = bbox.at(-1) / height;
                for (let c = 0; c < heatmap.length; ++c) {
                    let [xWeightedSum, yWeightedSum] = [0, 0];
                    let sum = 0;
                    let score = -Infinity;
                    const row = heatmap[c];
                    for (let y = 0; y < row.length; ++y) {
                        const col = row[y];
                        for (let x = 0; x < col.length; ++x) {
                            const value = col[x];
                            sum += value;

                            score = Math.max(score, value);

                            // Get weighted sum of positions
                            // TODO: Determine best offsets
                            xWeightedSum += (x + 0.5) * value;
                            yWeightedSum += (y) * value;
                        }
                    }

                    // Ignore low scores, if threshold is set
                    if (threshold != null && score < threshold) continue;

                    /** @type {[number, number]} */
                    const keypoint = [
                        xScale * xWeightedSum / sum,
                        yScale * yWeightedSum / sum,
                    ]
                    keypoints.push(keypoint);
                    labels.push(c);
                    scores.push(score);
                }
                batch_results.push({
                    bbox,
                    scores,
                    labels,
                    keypoints,
                });
            }
            results.push(batch_results);
        }
        return results;
    }
}
