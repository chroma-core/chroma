export class PyAnnoteFeatureExtractor extends FeatureExtractor {
    /**
     * Asynchronously extracts features from a given audio using the provided configuration.
     * @param {Float32Array|Float64Array} audio The audio data as a Float32Array/Float64Array.
     * @returns {Promise<{ input_values: Tensor; }>} The extracted input features.
     */
    _call(audio: Float32Array | Float64Array): Promise<{
        input_values: Tensor;
    }>;
    /**
     * NOTE: Can return fractional values. `Math.ceil` will ensure correct value.
     * @param {number} samples The number of frames in the audio.
     * @returns {number} The number of frames in the audio.
     */
    samples_to_frames(samples: number): number;
    /**
     * Post-processes the speaker diarization logits output by the model.
     * @param {import('../../utils/tensor.js').Tensor} logits The speaker diarization logits output by the model.
     * @param {number} num_samples Number of samples in the input audio.
     * @returns {Array<Array<{ id: number, start: number, end: number, confidence: number }>>} The post-processed speaker diarization results.
     */
    post_process_speaker_diarization(logits: import("../../utils/tensor.js").Tensor, num_samples: number): Array<Array<{
        id: number;
        start: number;
        end: number;
        confidence: number;
    }>>;
}
import { FeatureExtractor } from '../../base/feature_extraction_utils.js';
import { Tensor } from '../../utils/tensor.js';
//# sourceMappingURL=feature_extraction_pyannote.d.ts.map