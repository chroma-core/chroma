import { Processor } from '../../base/processing_utils.js';
import { PyAnnoteFeatureExtractor } from './feature_extraction_pyannote.js';

export class PyAnnoteProcessor extends Processor {
    static feature_extractor_class = PyAnnoteFeatureExtractor

    /**
     * Calls the feature_extractor function with the given audio input.
     * @param {any} audio The audio input to extract features from.
     * @returns {Promise<any>} A Promise that resolves with the extracted features.
     */
    async _call(audio) {
        return await this.feature_extractor(audio)
    }

    /** @type {PyAnnoteFeatureExtractor['post_process_speaker_diarization']} */
    post_process_speaker_diarization(...args) {
        return /** @type {PyAnnoteFeatureExtractor} */(this.feature_extractor).post_process_speaker_diarization(...args);
    }

    get sampling_rate() {
        return this.feature_extractor.config.sampling_rate;
    }
}
