import { AutoFeatureExtractor } from "../auto/feature_extraction_auto.js"
import { AutoTokenizer } from "../../tokenizers.js"
import { Processor } from "../../base/processing_utils.js"

/**
 * Represents a UltravoxProcessor that extracts features from an audio input.
 */
export class UltravoxProcessor extends Processor {
    static tokenizer_class = AutoTokenizer
    static feature_extractor_class = AutoFeatureExtractor
    static uses_processor_config = true;

    /**
     * @param {string} text The text input to process.
     * @param {Float32Array} audio The audio input to process.
     */
    async _call(text, audio = null, kwargs = {}) {
        // TODO: Support batched inputs
        if (Array.isArray(text)) {
            throw new Error("Batched inputs are not supported yet.");
        }

        let audio_inputs = {};
        if (audio) {
            const audio_len = audio.length;
            const { input_features } = await this.feature_extractor(audio, {
                ...kwargs,
                max_length: audio_len,
            });
            const nb_encoder_frames = Math.round(audio_len / this.config.encoder_ds_factor + 1e-4);

            // NOTE: The python version appears to have an off-by-one error.
            const audio_embed_frames = 1 + Math.ceil(nb_encoder_frames / this.config.stack_factor);
            audio_inputs["audio_token_len"] = [audio_embed_frames];
            audio_inputs["audio_values"] = input_features;

            const image_token = this.config.audio_placeholder;
            if (!text.includes(image_token)) {
                throw new Error(`The input text does not contain the image token ${image_token}.`);
            }
            text = text.replaceAll(image_token, image_token.repeat(audio_embed_frames));
        }

        const text_inputs = this.tokenizer(text, {
            add_special_tokens: false,
            ...kwargs,
        });

        return {
            ...text_inputs,
            ...audio_inputs,
        }
    }
}
