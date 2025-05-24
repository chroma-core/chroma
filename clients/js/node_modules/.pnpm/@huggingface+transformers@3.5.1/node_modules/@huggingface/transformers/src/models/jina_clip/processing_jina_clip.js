
import { Processor } from "../../base/processing_utils.js";
import { AutoImageProcessor } from "../auto/image_processing_auto.js";
import { AutoTokenizer } from "../../tokenizers.js";

export class JinaCLIPProcessor extends Processor {
    static tokenizer_class = AutoTokenizer
    static image_processor_class = AutoImageProcessor

    async _call(text=null, images=null, kwargs = {}) {

        if (!text && !images){
            throw new Error('Either text or images must be provided');
        }

        const text_inputs = text ? this.tokenizer(text, kwargs) : {};
        const image_inputs = images ? await this.image_processor(images, kwargs) : {};

        return {
            ...text_inputs,
            ...image_inputs,
        }
    }
}
