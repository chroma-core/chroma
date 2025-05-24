
import { Processor } from "../../base/processing_utils.js";
import { AutoImageProcessor } from "../auto/image_processing_auto.js";
import { AutoTokenizer } from "../../tokenizers.js";
import { RawImage } from "../../utils/image.js";
import { count } from "../../utils/core.js";

/**
 * Prompt with expanded image tokens for when the image is split into patches.
 * @private
 */
function _prompt_split_image(image_seq_len, image_rows, image_cols, fake_token_around_image, image_token, global_img_token) {
    let text_split_images = "";
    for (let n_h = 0; n_h < image_rows; ++n_h) {
        for (let n_w = 0; n_w < image_cols; ++n_w) {
            text_split_images += (
                fake_token_around_image +
                `<row_${n_h + 1}_col_${n_w + 1}>` +
                image_token.repeat(image_seq_len)
            );
        }
        text_split_images += "\n";
    }

    text_split_images += (
        `\n${fake_token_around_image}` +
        `${global_img_token}` +
        image_token.repeat(image_seq_len) +
        `${fake_token_around_image}`
    );
    return text_split_images;
}

/**
 * Prompt with expanded image tokens for a single image.
 * @private
 */
function _prompt_single_image(image_seq_len, fake_token_around_image, image_token, global_img_token) {
    return (
        `${fake_token_around_image}` +
        `${global_img_token}` +
        image_token.repeat(image_seq_len) +
        `${fake_token_around_image}`
    );
}

function get_image_prompt_string(image_rows, image_cols, image_seq_len, fake_token_around_image, image_token, global_img_token) {
    if (image_rows === 0 && image_cols === 0) {
        return _prompt_single_image(
            image_seq_len,
            fake_token_around_image,
            image_token,
            global_img_token
        );
    }
    return _prompt_split_image(
        image_seq_len, image_rows, image_cols, fake_token_around_image, image_token, global_img_token
    );
}


export class Idefics3Processor extends Processor {
    static image_processor_class = AutoImageProcessor
    static tokenizer_class = AutoTokenizer
    static uses_processor_config = true;

    fake_image_token = "<fake_token_around_image>";
    image_token = "<image>";
    global_img_token = "<global-img>";

    /**
     * 
     * @param {string|string[]} text 
     * @param {RawImage|RawImage[]|RawImage[][]} images  
     * @returns {Promise<any>}
     */
    async _call(text, images = null, options = {}) {
        options.return_row_col_info ??= true;

        let image_inputs;

        if (images) {
            image_inputs = await this.image_processor(images, options);
        }

        // NOTE: We assume text is present
        if (!Array.isArray(text)) {
            text = [text];
        }

        const image_rows = image_inputs.rows ?? [new Array(text.length).fill(0)];
        const image_cols = image_inputs.cols ?? [new Array(text.length).fill(0)];

        const image_seq_len = this.config.image_seq_len;
        const n_images_in_text = []
        const prompt_strings = [];
        for (let i = 0; i < text.length; ++i) {
            const sample = text[i];
            const sample_rows = image_rows[i];
            const sample_cols = image_cols[i];

            n_images_in_text.push(count(sample, this.image_token));

            // Replace the image token with fake tokens around the expanded image token sequence of length `image_seq_len`
            const image_prompt_strings = sample_rows.map(
                (n_rows, j) => get_image_prompt_string(
                    n_rows,
                    sample_cols[j],
                    image_seq_len,
                    this.fake_image_token,
                    this.image_token,
                    this.global_img_token,
                )
            );

            const split_sample = sample.split(this.image_token);
            if (split_sample.length === 0) {
                throw new Error("The image token should be present in the text.");
            }

            // Place in the image prompt strings where the image tokens are
            let new_sample = split_sample[0];
            for (let j = 0; j < image_prompt_strings.length; ++j) {
                new_sample += image_prompt_strings[j] + split_sample[j + 1];
            }
            prompt_strings.push(new_sample);
        }

        const text_inputs = this.tokenizer(prompt_strings);

        return {
            ...text_inputs,
            ...image_inputs,
        }
    }
}
