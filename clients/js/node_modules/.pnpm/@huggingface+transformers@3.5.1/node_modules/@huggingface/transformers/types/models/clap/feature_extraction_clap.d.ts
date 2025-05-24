export class ClapFeatureExtractor extends FeatureExtractor {
    constructor(config: any);
    mel_filters: number[][];
    mel_filters_slaney: number[][];
    window: Float64Array<ArrayBufferLike>;
    /**
     * Extracts the mel spectrogram and prepares it for the mode based on the `truncation` and `padding` arguments.
     *
     * Four different path are possible:
     *   - `truncation="fusion"` and the length of the waveform is greater than the max length: the mel spectrogram
     *     will be computed on the entire audio. 3 random crops and a dowsampled version of the full mel spectrogram
     *     are then stacked together. They will later be used for `feature_fusion`.
     *   - `truncation="rand_trunc"` and the length of the waveform is smaller than the max length: the audio is
     *     padded based on `padding`.
     *   - `truncation="fusion"` and the length of the waveform is smaller than the max length: the audio is padded
     *     based on `padding`, and is repeated `4` times.
     *   - `truncation="rand_trunc"` and the length of the waveform is greater than the max length: the mel
     *     spectrogram will be computed on a random crop of the waveform.
     *
     * @param {Float32Array|Float64Array} waveform The input waveform.
     * @param {number} max_length The maximum length of the waveform.
     * @param {string} truncation The truncation strategy to use.
     * @param {string} padding The padding strategy to use.
     * @returns {Promise<Tensor>} An object containing the mel spectrogram data as a Float32Array, its dimensions as an array of numbers, and a boolean indicating whether the waveform was longer than the max length.
     * @private
     */
    private _get_input_mel;
    /**
     * Compute the log-mel spectrogram of the provided `waveform` using the Hann window.
     * In CLAP, two different filter banks are used depending on the truncation pattern:
     *  - `self.mel_filters`: they correspond to the default parameters of `torchaudio` which can be obtained from
     *    calling `torchaudio.transforms.MelSpectrogram().mel_scale.fb`. These filters are used when `truncation`
     *    is set to `"fusion"`.
     *  - `self.mel_filteres_slaney` : they correspond to the default parameters of `librosa` which used
     *    `librosa.filters.mel` when computing the mel spectrogram. These filters were only used in the original
     *    implementation when the truncation mode is not `"fusion"`.
     *
     * @param {Float32Array|Float64Array} waveform The audio waveform to process.
     * @param {number[][]} mel_filters The mel filters to use.
     * @param {number} [max_length=null] The maximum number of frames to return.
     * @returns {Promise<Tensor>} An object containing the log-Mel spectrogram data as a Float32Array and its dimensions as an array of numbers.
     */
    _extract_fbank_features(waveform: Float32Array | Float64Array, mel_filters: number[][], max_length?: number): Promise<Tensor>;
    /**
     * Asynchronously extracts features from a given audio using the provided configuration.
     * @param {Float32Array|Float64Array} audio The audio data as a Float32Array/Float64Array.
     * @returns {Promise<{ input_features: Tensor }>} A Promise resolving to an object containing the extracted input features as a Tensor.
     */
    _call(audio: Float32Array | Float64Array, { max_length, }?: {
        max_length?: any;
    }): Promise<{
        input_features: Tensor;
    }>;
}
import { FeatureExtractor } from '../../base/feature_extraction_utils.js';
import { Tensor } from '../../utils/tensor.js';
//# sourceMappingURL=feature_extraction_clap.d.ts.map