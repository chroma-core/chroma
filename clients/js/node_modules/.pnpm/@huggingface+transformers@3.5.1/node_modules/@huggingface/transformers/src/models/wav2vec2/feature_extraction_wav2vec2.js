import { FeatureExtractor, validate_audio_inputs } from "../../base/feature_extraction_utils.js";
import { Tensor } from "../../utils/tensor.js";

export class Wav2Vec2FeatureExtractor extends FeatureExtractor {

    /**
     * @param {Float32Array} input_values 
     * @returns {Float32Array} 
     */
    _zero_mean_unit_var_norm(input_values) {
        // TODO support batch?
        const sum = input_values.reduce((a, b) => a + b, 0);
        const mean = sum / input_values.length;
        const variance = input_values.reduce((a, b) => a + (b - mean) ** 2, 0) / input_values.length;
        return input_values.map(x => (x - mean) / Math.sqrt(variance + 1e-7));
    }

    /**
     * Asynchronously extracts features from a given audio using the provided configuration.
     * @param {Float32Array|Float64Array} audio The audio data as a Float32Array/Float64Array.
     * @returns {Promise<{ input_values: Tensor; attention_mask: Tensor }>} A Promise resolving to an object containing the extracted input features and attention mask as Tensors.
     */
    async _call(audio) {
        validate_audio_inputs(audio, 'Wav2Vec2FeatureExtractor');

        if (audio instanceof Float64Array) {
            audio = new Float32Array(audio);
        }

        let input_values = audio;

        // zero-mean and unit-variance normalization
        if (this.config.do_normalize) {
            input_values = this._zero_mean_unit_var_norm(input_values);
        }

        // TODO: allow user to pass in attention mask
        const shape = [1, input_values.length];
        return {
            input_values: new Tensor('float32', input_values, shape),
            attention_mask: new Tensor('int64', new BigInt64Array(input_values.length).fill(1n), shape)
        };
    }
}
