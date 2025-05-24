import { FeatureExtractor, validate_audio_inputs } from '../../base/feature_extraction_utils.js';
import { Tensor } from '../../utils/tensor.js';
import { mel_filter_bank, spectrogram, window_function } from '../../utils/audio.js';


export class WeSpeakerFeatureExtractor extends FeatureExtractor {

    constructor(config) {
        super(config);

        const sampling_rate = this.config.sampling_rate;
        const mel_filters = mel_filter_bank(
            257, // num_frequency_bins
            this.config.num_mel_bins, // num_mel_filters
            20, // min_frequency
            Math.floor(sampling_rate / 2), // max_frequency
            sampling_rate, // sampling_rate
            null, // norm
            "kaldi", // mel_scale
            true, // triangularize_in_mel_space
        );
        this.mel_filters = mel_filters;

        this.window = window_function(400, 'hamming', {
            periodic: false,
        })
        this.min_num_frames = this.config.min_num_frames;
    }

    /**
     * Computes the log-Mel spectrogram of the provided audio waveform.
     * @param {Float32Array|Float64Array} waveform The audio waveform to process.
     * @returns {Promise<Tensor>} An object containing the log-Mel spectrogram data as a Float32Array and its dimensions as an array of numbers.
     */
    async _extract_fbank_features(waveform) {
        // Kaldi compliance: 16-bit signed integers
        // 32768 == 2 ** 15
        waveform = waveform.map((/** @type {number} */ x) => x * 32768)

        return spectrogram(
            waveform,
            this.window, // window
            400, // frame_length
            160, // hop_length
            {
                fft_length: 512,
                power: 2.0,
                center: false,
                preemphasis: 0.97,
                mel_filters: this.mel_filters,
                log_mel: 'log',
                mel_floor: 1.192092955078125e-07,
                remove_dc_offset: true,

                // Custom
                transpose: true,
                min_num_frames: this.min_num_frames,
            }
        )
    }


    /**
     * Asynchronously extracts features from a given audio using the provided configuration.
     * @param {Float32Array|Float64Array} audio The audio data as a Float32Array/Float64Array.
     * @returns {Promise<{ input_features: Tensor }>} A Promise resolving to an object containing the extracted input features as a Tensor.
     */
    async _call(audio) {
        validate_audio_inputs(audio, 'WeSpeakerFeatureExtractor');

        const features = (await this._extract_fbank_features(audio)).unsqueeze_(0);

        if (this.config.fbank_centering_span === null) {
            // center features with global average
            const meanData = /** @type {Float32Array} */ (features.mean(1).data);
            const featuresData = /** @type {Float32Array} */(features.data);
            const [batch_size, num_frames, feature_size] = features.dims;

            for (let i = 0; i < batch_size; ++i) {
                const offset1 = i * num_frames * feature_size;
                const offset2 = i * feature_size;
                for (let j = 0; j < num_frames; ++j) {
                    const offset3 = offset1 + j * feature_size;
                    for (let k = 0; k < feature_size; ++k) {
                        featuresData[offset3 + k] -= meanData[offset2 + k];
                    }
                }
            }
        }

        return {
            input_features: features
        };
    }
}
