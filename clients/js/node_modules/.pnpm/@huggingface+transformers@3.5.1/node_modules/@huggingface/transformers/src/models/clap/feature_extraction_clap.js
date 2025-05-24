import { FeatureExtractor, validate_audio_inputs } from '../../base/feature_extraction_utils.js';
import { Tensor } from '../../utils/tensor.js';
import { mel_filter_bank, spectrogram, window_function } from '../../utils/audio.js';


export class ClapFeatureExtractor extends FeatureExtractor {

    constructor(config) {
        super(config);

        this.mel_filters = mel_filter_bank(
            this.config.nb_frequency_bins, // num_frequency_bins
            this.config.feature_size, // num_mel_filters
            this.config.frequency_min, // min_frequency
            this.config.frequency_max, // max_frequency
            this.config.sampling_rate, // sampling_rate
            null, // norm
            "htk", // mel_scale
        );

        this.mel_filters_slaney = mel_filter_bank(
            this.config.nb_frequency_bins, // num_frequency_bins
            this.config.feature_size, // num_mel_filters
            this.config.frequency_min, // min_frequency
            this.config.frequency_max, // max_frequency
            this.config.sampling_rate, // sampling_rate
            "slaney", // norm
            "slaney", // mel_scale
        );

        this.window = window_function(this.config.fft_window_size, 'hann')

    }


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
    async _get_input_mel(waveform, max_length, truncation, padding) {

        /** @type {Tensor} */
        let input_mel;
        let longer = false;
        const diff = waveform.length - max_length;
        if (diff > 0) {
            if (truncation === 'rand_trunc') {
                longer = true;
                const idx = Math.floor(Math.random() * (diff + 1));
                waveform = waveform.subarray(idx, idx + max_length);

                input_mel = await this._extract_fbank_features(waveform, this.mel_filters_slaney, this.config.nb_max_samples);
            } else {
                // TODO implement fusion strategy
                throw new Error(`Truncation strategy "${truncation}" not implemented`)
            }
        } else {
            if (diff < 0) {
                let padded = new Float64Array(max_length); // already padded with zeros
                padded.set(waveform);

                if (padding === 'repeat') {
                    for (let i = waveform.length; i < max_length; i += waveform.length) {
                        padded.set(waveform.subarray(0, Math.min(waveform.length, max_length - i)), i);
                    }
                } else if (padding === 'repeatpad') {
                    for (let i = waveform.length; i < -diff; i += waveform.length) {
                        padded.set(waveform, i);
                    }
                }
                waveform = padded;
            }

            if (truncation === 'fusion') {
                throw new Error(`Truncation strategy "${truncation}" not implemented`)
            }

            input_mel = await this._extract_fbank_features(waveform, this.mel_filters_slaney, this.config.nb_max_samples);
        }

        return input_mel.unsqueeze_(0);
    }

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
    async _extract_fbank_features(waveform, mel_filters, max_length = null) {
        // NOTE: We don't pad/truncate since that is passed in as `max_num_frames`
        return spectrogram(
            waveform,
            this.window, // window
            this.config.fft_window_size, // frame_length
            this.config.hop_length, // hop_length
            {
                power: 2.0,
                mel_filters,
                log_mel: 'dB',

                // Custom
                max_num_frames: max_length,
                do_pad: false,
                transpose: true,
            }
        )
    }


    /**
     * Asynchronously extracts features from a given audio using the provided configuration.
     * @param {Float32Array|Float64Array} audio The audio data as a Float32Array/Float64Array.
     * @returns {Promise<{ input_features: Tensor }>} A Promise resolving to an object containing the extracted input features as a Tensor.
     */
    async _call(audio, {
        max_length = null,
    } = {}) {
        validate_audio_inputs(audio, 'ClapFeatureExtractor');

        // convert to mel spectrogram, truncate and pad if needed.
        const padded_inputs = await this._get_input_mel(
            audio,
            max_length ?? this.config.nb_max_samples,
            this.config.truncation,
            this.config.padding,
        );

        return {
            input_features: padded_inputs.unsqueeze_(0),
        }
    }
}
