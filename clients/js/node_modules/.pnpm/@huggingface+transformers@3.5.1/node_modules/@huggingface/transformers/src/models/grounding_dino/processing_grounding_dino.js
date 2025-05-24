import { Processor } from "../../base/processing_utils.js";
import { AutoImageProcessor } from "../auto/image_processing_auto.js";
import { AutoTokenizer } from "../../tokenizers.js";
import { center_to_corners_format } from "../../base/image_processors_utils.js";

/**
 * Get token ids of phrases from posmaps and input_ids.
 * @param {import('../../utils/tensor.js').Tensor} posmaps A boolean tensor of unbatched text-thresholded logits related to the detected bounding boxes of shape `(hidden_size, )`.
 * @param {import('../../utils/tensor.js').Tensor} input_ids A tensor of token ids of shape `(sequence_length, )`.
 */
function get_phrases_from_posmap(posmaps, input_ids) {

    const left_idx = 0;
    const right_idx = posmaps.dims.at(-1) - 1;

    const posmaps_list = posmaps.tolist();
    posmaps_list.fill(false, 0, left_idx + 1);
    posmaps_list.fill(false, right_idx);

    const input_ids_list = input_ids.tolist();
    return posmaps_list
        .map((val, idx) => val ? idx : null)
        .filter(idx => idx !== null)
        .map(i => input_ids_list[i]);
}

export class GroundingDinoProcessor extends Processor {
    static tokenizer_class = AutoTokenizer
    static image_processor_class = AutoImageProcessor

    /**
     * @typedef {import('../../utils/image.js').RawImage} RawImage
     */
    /**
     * 
     * @param {RawImage|RawImage[]|RawImage[][]} images  
     * @param {string|string[]} text 
     * @returns {Promise<any>}
     */
    async _call(images, text, options = {}) {

        const image_inputs = images ? await this.image_processor(images, options) : {};
        const text_inputs = text ? this.tokenizer(text, options) : {};

        return {
            ...text_inputs,
            ...image_inputs,
        }
    }
    post_process_grounded_object_detection(outputs, input_ids, {
        box_threshold = 0.25,
        text_threshold = 0.25,
        target_sizes = null
    } = {}) {
        const { logits, pred_boxes } = outputs;
        const batch_size = logits.dims[0];

        if (target_sizes !== null && target_sizes.length !== batch_size) {
            throw Error("Make sure that you pass in as many target sizes as the batch dimension of the logits")
        }
        const num_queries = logits.dims.at(1);

        const probs = logits.sigmoid(); // (batch_size, num_queries, 256)
        const scores = probs.max(-1).tolist(); // (batch_size, num_queries)

        // Convert to [x0, y0, x1, y1] format
        const boxes = pred_boxes.tolist() // (batch_size, num_queries, 4)
            .map(batch => batch.map(box => center_to_corners_format(box)));

        const results = [];
        for (let i = 0; i < batch_size; ++i) {
            const target_size = target_sizes !== null ? target_sizes[i] : null;

            // Convert from relative [0, 1] to absolute [0, height] coordinates
            if (target_size !== null) {
                boxes[i] = boxes[i].map(box => box.map((x, j) => x * target_size[(j + 1) % 2]));
            }

            const batch_scores = scores[i];
            const final_scores = [];
            const final_phrases = [];
            const final_boxes = [];
            for (let j = 0; j < num_queries; ++j) {
                const score = batch_scores[j];
                if (score <= box_threshold) {
                    continue;
                }
                const box = boxes[i][j];
                const prob = probs[i][j];

                final_scores.push(score);
                final_boxes.push(box);

                const phrases = get_phrases_from_posmap(prob.gt(text_threshold), input_ids[i]);
                final_phrases.push(phrases);
            }
            results.push({ scores: final_scores, boxes: final_boxes, labels: this.batch_decode(final_phrases) });
        }
        return results;
    }
}
