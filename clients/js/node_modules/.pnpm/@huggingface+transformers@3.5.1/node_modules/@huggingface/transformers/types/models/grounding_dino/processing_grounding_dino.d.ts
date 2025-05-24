export class GroundingDinoProcessor extends Processor {
    static tokenizer_class: typeof AutoTokenizer;
    static image_processor_class: typeof AutoImageProcessor;
    /**
     * @typedef {import('../../utils/image.js').RawImage} RawImage
     */
    /**
     *
     * @param {RawImage|RawImage[]|RawImage[][]} images
     * @param {string|string[]} text
     * @returns {Promise<any>}
     */
    _call(images: import("../../utils/image.js").RawImage | import("../../utils/image.js").RawImage[] | import("../../utils/image.js").RawImage[][], text: string | string[], options?: {}): Promise<any>;
    post_process_grounded_object_detection(outputs: any, input_ids: any, { box_threshold, text_threshold, target_sizes }?: {
        box_threshold?: number;
        text_threshold?: number;
        target_sizes?: any;
    }): {
        scores: any[];
        boxes: any[];
        labels: string[];
    }[];
}
import { Processor } from "../../base/processing_utils.js";
import { AutoTokenizer } from "../../tokenizers.js";
import { AutoImageProcessor } from "../auto/image_processing_auto.js";
//# sourceMappingURL=processing_grounding_dino.d.ts.map