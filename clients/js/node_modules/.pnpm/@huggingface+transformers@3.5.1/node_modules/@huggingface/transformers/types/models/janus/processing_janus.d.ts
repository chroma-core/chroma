export class VLChatProcessor extends Processor {
    static image_processor_class: typeof AutoImageProcessor;
    static tokenizer_class: typeof AutoTokenizer;
    constructor(config: any, components: any);
    image_tag: any;
    image_start_tag: any;
    image_end_tag: any;
    num_image_tokens: any;
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
    _call(conversation: (import("../../tokenizers.js").Message & {
        /**
         * The images in the message.
         */
        images?: (RawImage | string | URL)[];
    })[], { images, chat_template, }?: {
        images?: RawImage | RawImage[];
        chat_template?: string;
    }): Promise<{
        /**
         * The input IDs.
         */
        input_ids: Tensor;
        /**
         * The attention mask.
         */
        attention_mask: Tensor;
        /**
         * The image sequence mask.
         */
        images_seq_mask: Tensor;
        /**
         * The image embedding mask.
         */
        images_emb_mask: Tensor;
    } | ({
        /**
         * The input IDs.
         */
        input_ids: Tensor;
        /**
         * The attention mask.
         */
        attention_mask: Tensor;
        /**
         * The image sequence mask.
         */
        images_seq_mask: Tensor;
        /**
         * The image embedding mask.
         */
        images_emb_mask: Tensor;
    } & import("../../base/image_processors_utils.js").ImageProcessorResult)>;
}
import { Processor } from "../../base/processing_utils.js";
import { RawImage } from "../../utils/image.js";
import { Tensor } from "../../utils/tensor.js";
import { AutoImageProcessor } from "../auto/image_processing_auto.js";
import { AutoTokenizer } from "../../tokenizers.js";
//# sourceMappingURL=processing_janus.d.ts.map