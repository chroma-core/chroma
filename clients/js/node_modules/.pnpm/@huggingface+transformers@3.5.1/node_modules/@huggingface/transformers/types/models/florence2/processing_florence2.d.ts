export class Florence2Processor extends Processor {
    static tokenizer_class: typeof AutoTokenizer;
    static image_processor_class: typeof AutoImageProcessor;
    constructor(config: any, components: any);
    /** @type {Map<string, string>} */
    tasks_answer_post_processing_type: Map<string, string>;
    /** @type {Map<string, string>} */
    task_prompts_without_inputs: Map<string, string>;
    /** @type {Map<string, string>} */
    task_prompts_with_input: Map<string, string>;
    regexes: {
        quad_boxes: RegExp;
        bboxes: RegExp;
    };
    size_per_bin: number;
    /**
     * Helper function to construct prompts from input texts
     * @param {string|string[]} text
     * @returns {string[]}
     */
    construct_prompts(text: string | string[]): string[];
    /**
     * Post-process the output of the model to each of the task outputs.
     * @param {string} text The text to post-process.
     * @param {string} task The task to post-process the text for.
     * @param {[number, number]} image_size The size of the image. height x width.
     */
    post_process_generation(text: string, task: string, image_size: [number, number]): {
        [task]: string | {
            [x: string]: any[];
            labels: any[];
        };
    };
    _call(images: any, text?: any, kwargs?: {}): Promise<any>;
}
import { Processor } from "../../base/processing_utils.js";
import { AutoTokenizer } from "../../tokenizers.js";
import { AutoImageProcessor } from "../auto/image_processing_auto.js";
//# sourceMappingURL=processing_florence2.d.ts.map