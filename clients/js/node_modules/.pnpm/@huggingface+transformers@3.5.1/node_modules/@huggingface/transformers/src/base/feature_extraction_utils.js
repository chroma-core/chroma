import { FEATURE_EXTRACTOR_NAME } from "../utils/constants.js";
import { Callable } from "../utils/generic.js";
import { getModelJSON } from "../utils/hub.js";

/**
 * Base class for feature extractors.
 */
export class FeatureExtractor extends Callable {
    /**
     * Constructs a new FeatureExtractor instance.
     *
     * @param {Object} config The configuration for the feature extractor.
     */
    constructor(config) {
        super();
        this.config = config
    }

    /**
     * Instantiate one of the feature extractor classes of the library from a pretrained model.
     * 
     * The feature extractor class to instantiate is selected based on the `feature_extractor_type` property of
     * the config object (either passed as an argument or loaded from `pretrained_model_name_or_path` if possible)
     * 
     * @param {string} pretrained_model_name_or_path The name or path of the pretrained model. Can be either:
     * - A string, the *model id* of a pretrained feature_extractor hosted inside a model repo on huggingface.co.
     *   Valid model ids can be located at the root-level, like `bert-base-uncased`, or namespaced under a
     *   user or organization name, like `dbmdz/bert-base-german-cased`.
     * - A path to a *directory* containing feature_extractor files, e.g., `./my_model_directory/`.
     * @param {import('../utils/hub.js').PretrainedOptions} options Additional options for loading the feature_extractor.
     * 
     * @returns {Promise<FeatureExtractor>} A new instance of the Feature Extractor class.
     */
    static async from_pretrained(pretrained_model_name_or_path, options) {
        const config = await getModelJSON(pretrained_model_name_or_path, FEATURE_EXTRACTOR_NAME, true, options);
        return new this(config);
    }
}


/**
 * Helper function to validate audio inputs.
 * @param {any} audio The audio data.
 * @param {string} feature_extractor The name of the feature extractor.
 * @private
 */
export function validate_audio_inputs(audio, feature_extractor) {
    if (!(audio instanceof Float32Array || audio instanceof Float64Array)) {
        throw new Error(
            `${feature_extractor} expects input to be a Float32Array or a Float64Array, but got ${audio?.constructor?.name ?? typeof audio} instead. ` +
            `If using the feature extractor directly, remember to use \`read_audio(url, sampling_rate)\` to obtain the raw audio data of the file/url.`
        )
    }
}
