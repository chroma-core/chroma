import { FeatureExtractor, validate_audio_inputs } from '../../base/feature_extraction_utils.js';
import { Tensor } from '../../utils/tensor.js';
import { mel_filter_bank, spectrogram, window_function } from '../../utils/audio.js';
import { max } from '../../utils/maths.js';

export class WhisperFeatureExtractor extends FeatureExtractor {

    constructor(config) {
        super(config);

        // Prefer given `mel_filters` from preprocessor_config.json, or calculate them if they don't exist.
        this.config.mel_filters ??= mel_filter_bank(
            Math.floor(1 + this.config.n_fft / 2), // num_frequency_bins
            this.config.feature_size, // num_mel_filters
            0.0, // min_frequency
            8000.0, // max_frequency
            this.config.sampling_rate, // sampling_rate
            "slaney", // norm
            "slaney", // mel_scale
        );

        this.window = window_function(this.config.n_fft, 'hann');
    }

    /**
     * Computes the log-Mel spectrogram of the provided audio waveform.
     * @param {Float32Array|Float64Array} waveform The audio waveform to process.
     * @returns {Promise<Tensor>} An object containing the log-Mel spectrogram data as a Float32Array and its dimensions as an array of numbers.
     */
    async _extract_fbank_features(waveform) {
        const features = await spectrogram(
            waveform,
            this.window, // window
            this.config.n_fft, // frame_length
            this.config.hop_length, // hop_length
            {
                power: 2.0,
                mel_filters: this.config.mel_filters,
                log_mel: 'log10',

                // Custom
                max_num_frames: Math.min(
                    Math.floor(waveform.length / this.config.hop_length),
                    this.config.nb_max_frames, // 3000
                )
            }
        )

        const data = features.data;
        const maxValue = max(/** @type {Float32Array} */(data))[0];

        for (let i = 0; i < data.length; ++i) {
            data[i] = (Math.max(data[i], maxValue - 8.0) + 4.0) / 4.0;
        }

        return features;
    }

    /**
     * Asynchronously extracts features from a given audio using the provided configuration.
     * @param {Float32Array|Float64Array} audio The audio data as a Float32Array/Float64Array.
     * @returns {Promise<{ input_features: Tensor }>} A Promise resolving to an object containing the extracted input features as a Tensor.
     */
    async _call(audio, {
        max_length = null,
    } = {}) {
        validate_audio_inputs(audio, 'WhisperFeatureExtractor');

        let waveform;
        const length = max_length ?? this.config.n_samples;
        if (audio.length > length) {
            if (audio.length > this.config.n_samples) {
                console.warn(
                    "Attempting to extract features for audio longer than 30 seconds. " +
                    "If using a pipeline to extract transcript from a long audio clip, " +
                    "remember to specify `chunk_length_s` and/or `stride_length_s`."
                );
            }
            waveform = audio.slice(0, length);
        } else {
            // pad with zeros
            waveform = new Float32Array(length);
            waveform.set(audio);
        }

        const features = await this._extract_fbank_features(waveform);

        return {
            input_features: features.unsqueeze_(0)
        };
    }
}
