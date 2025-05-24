import { FeatureExtractor, validate_audio_inputs } from '../../base/feature_extraction_utils.js';
import { Tensor } from '../../utils/tensor.js';
import { mel_filter_bank, spectrogram, window_function } from '../../utils/audio.js';


export class ASTFeatureExtractor extends FeatureExtractor {

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

        this.window = window_function(400, 'hann', {
            periodic: false,
        })

        this.mean = this.config.mean;
        this.std = this.config.std;
    }

    /**
     * Computes the log-Mel spectrogram of the provided audio waveform.
     * @param {Float32Array|Float64Array} waveform The audio waveform to process.
     * @param {number} max_length The maximum number of frames to return.
     * @returns {Promise<Tensor>} An object containing the log-Mel spectrogram data as a Float32Array and its dimensions as an array of numbers.
     */
    async _extract_fbank_features(waveform, max_length) {
        // NOTE: We don't pad/truncate since that is passed in as `max_num_frames`
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
                max_num_frames: max_length,
                transpose: true,
            }
        )
    }


    /**
     * Asynchronously extracts features from a given audio using the provided configuration.
     * @param {Float32Array|Float64Array} audio The audio data as a Float32Array/Float64Array.
     * @returns {Promise<{ input_values: Tensor }>} A Promise resolving to an object containing the extracted input features as a Tensor.
     */
    async _call(audio) {
        validate_audio_inputs(audio, 'ASTFeatureExtractor');

        const features = await this._extract_fbank_features(audio, this.config.max_length);
        if (this.config.do_normalize) {
            // Normalize the input audio spectrogram to have mean=0, std=0.5
            const denom = this.std * 2;
            const features_data = features.data;
            for (let i = 0; i < features_data.length; ++i) {
                features_data[i] = (features_data[i] - this.mean) / denom;
            }
        }

        return {
            input_values: features.unsqueeze_(0)
        };
    }
}
