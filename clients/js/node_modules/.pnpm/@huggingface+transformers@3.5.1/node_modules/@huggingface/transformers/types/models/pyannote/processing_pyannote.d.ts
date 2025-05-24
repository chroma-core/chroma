export class PyAnnoteProcessor extends Processor {
    static feature_extractor_class: typeof PyAnnoteFeatureExtractor;
    /**
     * Calls the feature_extractor function with the given audio input.
     * @param {any} audio The audio input to extract features from.
     * @returns {Promise<any>} A Promise that resolves with the extracted features.
     */
    _call(audio: any): Promise<any>;
    post_process_speaker_diarization(logits: import("../../transformers.js").Tensor, num_samples: number): Array<Array<{
        id: number;
        start: number;
        end: number;
        confidence: number;
    }>>;
    get sampling_rate(): any;
}
import { Processor } from '../../base/processing_utils.js';
import { PyAnnoteFeatureExtractor } from './feature_extraction_pyannote.js';
//# sourceMappingURL=processing_pyannote.d.ts.map