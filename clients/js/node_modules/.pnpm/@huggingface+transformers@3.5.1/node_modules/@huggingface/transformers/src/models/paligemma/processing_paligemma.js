import { Processor } from "../../base/processing_utils.js";
import { AutoImageProcessor } from "../auto/image_processing_auto.js";
import { AutoTokenizer } from "../../tokenizers.js";

const IMAGE_TOKEN = "<image>";

function build_string_from_input(
    prompt,
    bos_token,
    image_seq_len,
    image_token,
    num_images,
) {
    return `${image_token.repeat(image_seq_len * num_images)}${bos_token}${prompt}\n`
}

export class PaliGemmaProcessor extends Processor {
    static tokenizer_class = AutoTokenizer
    static image_processor_class = AutoImageProcessor
    static uses_processor_config = false;

    /**
     * @typedef {import('../../utils/image.js').RawImage} RawImage
     */

    // `images` is required, `text` is optional
    async _call(/** @type {RawImage|RawImage[]} */ images, text = null, kwargs = {}) {
        if (!text) {
            console.warn(
                "You are using PaliGemma without a text prefix. It will perform as a picture-captioning model."
            )
            text = ""
        }

        if (!Array.isArray(images)) {
            images = [images]
        }

        if (!Array.isArray(text)) {
            text = [text]
        }

        const bos_token = this.tokenizer.bos_token;
        // @ts-expect-error TS2339
        const image_seq_length = this.image_processor.config.image_seq_length;
        let input_strings;
        if (text.some((t) => t.includes(IMAGE_TOKEN))) {
            input_strings = text.map(
                sample => {
                    const expanded_sample = sample.replaceAll(IMAGE_TOKEN, IMAGE_TOKEN.repeat(image_seq_length));
                    const bos_rfind_index = expanded_sample.lastIndexOf(IMAGE_TOKEN);
                    const bos_index = bos_rfind_index === -1 ? 0 : bos_rfind_index + IMAGE_TOKEN.length;
                    return expanded_sample.slice(0, bos_index) + bos_token + expanded_sample.slice(bos_index) + "\n";
                }
            )
        } else {
            console.warn(
                "You are passing both `text` and `images` to `PaliGemmaProcessor`. The processor expects special " +
                "image tokens in the text, as many tokens as there are images per each text. It is recommended to " +
                "add `<image>` tokens in the very beginning of your text. For this call, we will infer how many images " +
                "each text has and add special tokens."
            )

            input_strings = text.map(
                sample => build_string_from_input(
                    sample,
                    bos_token,
                    image_seq_length,
                    IMAGE_TOKEN,
                    images.length,
                )
            )
        }

        const text_inputs = this.tokenizer(input_strings, kwargs);
        const image_inputs = await this.image_processor(images, kwargs);

        return {
            ...image_inputs,
            ...text_inputs,
        }
    }
}
