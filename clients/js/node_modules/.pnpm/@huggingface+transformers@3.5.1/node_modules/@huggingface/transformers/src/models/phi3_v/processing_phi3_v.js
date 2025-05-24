import { Processor } from "../../base/processing_utils.js";
import { AutoImageProcessor } from "../auto/image_processing_auto.js";
import { AutoTokenizer } from "../../tokenizers.js";
import { RawImage } from "../../utils/image.js";

const IMAGE_TOKEN = "<|image|>";
const IMAGE_TOKEN_PATTERN = /<\|image_\d+\|>/g;

export class Phi3VProcessor extends Processor {
    static image_processor_class = AutoImageProcessor
    static tokenizer_class = AutoTokenizer

    /**
     * 
     * @param {string|string[]} text 
     * @param {RawImage|RawImage[]} images 
     * @param  { { padding?: boolean, truncation?: boolean, num_crops?: number } | undefined } options
     * @returns {Promise<any>}
     */
    async _call(text, images = null, {
        padding = true,
        truncation = true,
        num_crops = null,
    } = {}) {

        if (!Array.isArray(text)) {
            text = [text];
        }

        let text_inputs, image_inputs;
        if (images) {
            image_inputs = await this.image_processor(images, { num_crops });
            const { num_img_tokens } = image_inputs;

            // The original implementation adds a bos_token before the image tokens
            // TODO: Check if this affects performance, since it looks like a bug in the original implementation
            const prompt_chunks = text.map((t, i) => t.split(IMAGE_TOKEN_PATTERN).join(IMAGE_TOKEN.repeat(num_img_tokens[i])));

            text_inputs = this.tokenizer(prompt_chunks, { padding, truncation });

            // The model expects image tokens to be negative, so we negate the image token ids
            const image_token_id = this.tokenizer.model.convert_tokens_to_ids([IMAGE_TOKEN])[0];
            text_inputs.input_ids.map_(id => (id == image_token_id) ? -id : id);
        } else {
            text_inputs = this.tokenizer(text);
        }

        return {
            ...text_inputs,
            ...image_inputs,
        }
    }
}
