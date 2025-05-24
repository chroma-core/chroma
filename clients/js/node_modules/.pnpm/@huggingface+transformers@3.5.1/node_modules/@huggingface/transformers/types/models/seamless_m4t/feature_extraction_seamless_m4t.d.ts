export class SeamlessM4TFeatureExtractor extends FeatureExtractor {
    constructor(config: any);
    mel_filters: number[][];
    window: Float64Array<ArrayBufferLike>;
    /**
     * Computes the log-Mel spectrogram of the provided audio waveform.
     * @param {Float32Array|Float64Array} waveform The audio waveform to process.
     * @param {number} max_length The maximum number of frames to return.
     * @returns {Promise<Tensor>} An object containing the log-Mel spectrogram data as a Float32Array and its dimensions as an array of numbers.
     */
    _extract_fbank_features(waveform: Float32Array | Float64Array, max_length: number): Promise<Tensor>;
    /**
     * Asynchronously extracts features from a given audio using the provided configuration.
     * @param {Float32Array|Float64Array} audio The audio data as a Float32Array/Float64Array.
     * @param {Object} options Optional parameters for feature extraction.
     * @param {boolean} [options.padding=true] Whether to pad the sequence to a multiple of `pad_to_multiple_of`.
     * @param {number} [options.pad_to_multiple_of=2] The number to pad the sequence to a multiple of.
     * @param {boolean} [options.do_normalize_per_mel_bins=true] Whether or not to zero-mean unit-variance normalize the input per mel-channel.
     * @param {boolean} [options.return_attention_mask=true] Whether to return the attention mask.
     * @returns {Promise<{ input_features: Tensor, attention_mask?: Tensor }>} A Promise resolving to an object containing the extracted input features and attention masks as Tensors.
     */
    _call(audio: Float32Array | Float64Array, { padding, pad_to_multiple_of, do_normalize_per_mel_bins, return_attention_mask, }?: {
        padding?: boolean;
        pad_to_multiple_of?: number;
        do_normalize_per_mel_bins?: boolean;
        return_attention_mask?: boolean;
    }): Promise<{
        input_features: Tensor;
        attention_mask?: Tensor;
    }>;
}
import { FeatureExtractor } from '../../base/feature_extraction_utils.js';
import { Tensor } from '../../utils/tensor.js';
//# sourceMappingURL=feature_extraction_seamless_m4t.d.ts.map