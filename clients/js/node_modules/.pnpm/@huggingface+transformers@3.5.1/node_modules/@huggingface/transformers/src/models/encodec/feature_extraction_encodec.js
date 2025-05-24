import { FeatureExtractor, validate_audio_inputs } from '../../base/feature_extraction_utils.js';
import { Tensor } from '../../utils/tensor.js';


export class EncodecFeatureExtractor extends FeatureExtractor {
    /**
     * Asynchronously extracts input values from a given audio using the provided configuration.
     * @param {Float32Array|Float64Array} audio The audio data as a Float32Array/Float64Array.
     * @returns {Promise<{ input_values: Tensor; }>} The extracted input values.
     */
    async _call(audio) {
        validate_audio_inputs(audio, 'EncodecFeatureExtractor');

        if (audio instanceof Float64Array) {
            audio = new Float32Array(audio);
        }

        const num_channels = this.config.feature_size;
        if (audio.length % num_channels !== 0) {
            throw new Error(`The length of the audio data must be a multiple of the number of channels (${num_channels}).`);
        }

        const shape = [
            1,                              /* batch_size */
            num_channels,                   /* num_channels */
            audio.length / num_channels,    /* num_samples */
        ];
        return {
            input_values: new Tensor('float32', audio, shape),
        };
    }
}
