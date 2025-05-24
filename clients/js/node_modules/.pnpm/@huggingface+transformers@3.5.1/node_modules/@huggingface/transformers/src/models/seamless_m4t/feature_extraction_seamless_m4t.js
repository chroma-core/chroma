import { FeatureExtractor, validate_audio_inputs } from '../../base/feature_extraction_utils.js';
import { Tensor } from '../../utils/tensor.js';
import { mel_filter_bank, spectrogram, window_function } from '../../utils/audio.js';

export class SeamlessM4TFeatureExtractor extends FeatureExtractor {

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

        this.window = window_function(400, 'povey', {
            periodic: false,
        })
    }

    /**
     * Computes the log-Mel spectrogram of the provided audio waveform.
     * @param {Float32Array|Float64Array} waveform The audio waveform to process.
     * @param {number} max_length The maximum number of frames to return.
     * @returns {Promise<Tensor>} An object containing the log-Mel spectrogram data as a Float32Array and its dimensions as an array of numbers.
     */
    async _extract_fbank_features(waveform, max_length) {
        // NOTE: We don't pad/truncate since that is passed in as `max_num_frames`

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
                max_num_frames: max_length,
                transpose: true,
            }
        )
    }

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
    async _call(audio, {
        padding = true,
        pad_to_multiple_of = 2,
        do_normalize_per_mel_bins = true,
        return_attention_mask = true,
    } = {}) {
        validate_audio_inputs(audio, 'SeamlessM4TFeatureExtractor');

        let features = await this._extract_fbank_features(audio, this.config.max_length);

        if (do_normalize_per_mel_bins) {
            const [num_features, feature_size] = features.dims;
            const data = features.data;
            for (let i = 0; i < feature_size; ++i) {
                let sum = 0;
                for (let j = 0; j < num_features; ++j) {
                    sum += data[j * feature_size + i];
                }

                const mean = sum / num_features;

                let variance = 0;
                for (let j = 0; j < num_features; ++j) {
                    variance += (data[j * feature_size + i] - mean) ** 2;
                }
                variance /= num_features - 1; // NOTE: We use ddof=1

                const std = Math.sqrt(variance + 1e-7);
                for (let j = 0; j < num_features; ++j) {
                    const index = j * feature_size + i;
                    data[index] = (data[index] - mean) / std;
                }
            }
        }

        let padded_attention_mask;
        if (padding) {
            const [num_frames, num_channels] = features.dims;
            const data = /** @type {Float32Array} */(features.data);

            const pad_size = num_frames % pad_to_multiple_of;
            if (pad_size > 0) {
                const padded_data = new Float32Array(num_channels * (num_frames + pad_size));
                padded_data.set(data)
                padded_data.fill(this.config.padding_value, data.length)

                const numPaddedFrames = num_frames + pad_size;
                features = new Tensor(
                    features.type,
                    padded_data,
                    [numPaddedFrames, num_channels],
                )

                if (return_attention_mask) {
                    padded_attention_mask = new Tensor(
                        'int64',
                        new BigInt64Array(numPaddedFrames),
                        [1, numPaddedFrames],
                    );
                    /** @type {BigInt64Array} */ (padded_attention_mask.data).fill(1n, 0, num_frames);
                }
            }
        }

        const [num_frames, num_channels] = features.dims;

        const stride = this.config.stride;
        const remainder = num_frames % stride;
        if (remainder !== 0) {
            throw new Error(`The number of frames (${num_frames}) must be a multiple of the stride (${stride}).`)
        }

        const input_features = features.view(
            1,
            Math.floor(num_frames / stride),
            num_channels * stride,
        );

        const result = { input_features }

        if (return_attention_mask) {
            const reshapedNumFrames = input_features.dims[1];

            const attention_mask_data = new BigInt64Array(reshapedNumFrames);

            if (padded_attention_mask) {
                const padded_attention_mask_data = padded_attention_mask.data;
                for (let i = 1, j = 0; i < num_frames; i += stride, ++j) {
                    attention_mask_data[j] = padded_attention_mask_data[i];
                }
            } else {
                attention_mask_data.fill(1n);
            }
            result.attention_mask = new Tensor(
                'int64',
                attention_mask_data,
                [1, reshapedNumFrames],
            );
        }

        return result;
    }
}
