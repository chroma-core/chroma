export class WeSpeakerFeatureExtractor extends FeatureExtractor {
    constructor(config: any);
    mel_filters: number[][];
    window: Float64Array<ArrayBufferLike>;
    min_num_frames: any;
    /**
     * Computes the log-Mel spectrogram of the provided audio waveform.
     * @param {Float32Array|Float64Array} waveform The audio waveform to process.
     * @returns {Promise<Tensor>} An object containing the log-Mel spectrogram data as a Float32Array and its dimensions as an array of numbers.
     */
    _extract_fbank_features(waveform: Float32Array | Float64Array): Promise<Tensor>;
    /**
     * Asynchronously extracts features from a given audio using the provided configuration.
     * @param {Float32Array|Float64Array} audio The audio data as a Float32Array/Float64Array.
     * @returns {Promise<{ input_features: Tensor }>} A Promise resolving to an object containing the extracted input features as a Tensor.
     */
    _call(audio: Float32Array | Float64Array): Promise<{
        input_features: Tensor;
    }>;
}
import { FeatureExtractor } from '../../base/feature_extraction_utils.js';
import { Tensor } from '../../utils/tensor.js';
//# sourceMappingURL=feature_extraction_wespeaker.d.ts.map