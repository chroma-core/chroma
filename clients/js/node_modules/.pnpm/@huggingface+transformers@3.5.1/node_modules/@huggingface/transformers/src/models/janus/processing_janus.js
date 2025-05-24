
import { Processor } from "../../base/processing_utils.js";
import { AutoImageProcessor } from "../auto/image_processing_auto.js";
import { AutoTokenizer } from "../../tokenizers.js";
import { mergeArrays } from "../../utils/core.js";
import { Tensor } from "../../utils/tensor.js";
import { RawImage } from "../../utils/image.js";

export class VLChatProcessor extends Processor {
    static image_processor_class = AutoImageProcessor
    static tokenizer_class = AutoTokenizer
    static uses_processor_config = true;

    constructor(config, components) {
        super(config, components);

        this.image_tag = this.config.image_tag;
        this.image_start_tag = this.config.image_start_tag;
        this.image_end_tag = this.config.image_end_tag;
        this.num_image_tokens = this.config.num_image_tokens;
    }

    /**
     * @typedef {Object} MultimodalMessageProperties Additional properties for multimodal messages.
     * @property {(RawImage | string | URL)[]} [images] The images in the message.
     * @typedef {(import('../../tokenizers.js').Message & MultimodalMessageProperties)[]} MultimodalConversation The conversation possibly containing multimodal inputs.
     */

    /**
     * @typedef {Object} VLCChatProcessorResult The processed input.
     * @property {Tensor} input_ids The input IDs.
     * @property {Tensor} attention_mask The attention mask.
     * @property {Tensor} images_seq_mask The image sequence mask.
     * @property {Tensor} images_emb_mask The image embedding mask.
     */

    /**
     * @param {MultimodalConversation} conversation The chat messages to process.
     * @param {Object} options Additional options for processing.
     * @param {RawImage|RawImage[]} [options.images] The images to process, if not set in the conversation.
     * @param {string} [options.chat_template="default"] The chat template to use.
     * @returns {Promise<VLCChatProcessorResult | VLCChatProcessorResult & import('../../base/image_processors_utils.js').ImageProcessorResult>} The processed input.
     */
    async _call(conversation, {
        images = null,
        chat_template = "default",
    }={}) {
        if (!images) {
            images = await Promise.all(
                conversation
                    .filter((msg) => msg.images)
                    .flatMap((msg) => msg.images)
                    .map((img) => RawImage.read(img))
            );
        } else if (!Array.isArray(images)) {
            images = [images];
        }

        const tokenizer = this.tokenizer;
        const result = tokenizer.apply_chat_template(conversation, {
            tokenize: false,
            add_generation_prompt: true,
            chat_template,
        });

        const encode = (text) => tokenizer.encode(text, { add_special_tokens: false });
        const parts = (/** @type {string} */(result))
            .split(this.image_tag);
        const num_images = parts.length - 1;
        if (images.length !== num_images) {
            throw new Error(`Number of images provided (${images.length}) does not match number of "${this.image_tag}" image tags (${num_images})`);
        }

        const [
            image_placeholder_tag_id,
            image_start_tag_id,
            image_end_tag_id,
        ] = tokenizer.model.convert_tokens_to_ids([
            this.image_tag,
            this.image_start_tag,
            this.image_end_tag,
        ]);

        let input_ids = encode(parts[0]);
        let images_seq_mask = new Array(input_ids.length).fill(false);
        for (let i = 1; i < parts.length; ++i) {
            const placeholder_image_tokens = new Array(this.num_image_tokens).fill(image_placeholder_tag_id);
            const tokens = encode(parts[i]);
            input_ids = mergeArrays(
                input_ids,
                [image_start_tag_id], placeholder_image_tokens, [image_end_tag_id],
                tokens,
            );
            const image_mask = new Array(this.num_image_tokens).fill(true);
            images_seq_mask = mergeArrays(
                images_seq_mask,
                [false], image_mask, [false],
                new Array(tokens.length).fill(false),
            );
        }

        const dims = [1, input_ids.length];
        const final = {
            input_ids: new Tensor('int64', input_ids, dims),
            attention_mask: new Tensor('int64', new Array(input_ids.length).fill(1), dims),
            images_seq_mask: new Tensor('bool', images_seq_mask, dims),
            images_emb_mask: new Tensor(
                'bool',
                new Array(num_images * this.num_image_tokens).fill(true),
                [1, num_images, this.num_image_tokens],
            ),
        }

        if (images && images.length > 0) {
            const image_inputs = await this.image_processor(images);
            // Set the batch_size dimension to 1
            image_inputs.pixel_values.unsqueeze_(0);
            return { ...final, ...image_inputs };
        }

        return final;
    }
}
