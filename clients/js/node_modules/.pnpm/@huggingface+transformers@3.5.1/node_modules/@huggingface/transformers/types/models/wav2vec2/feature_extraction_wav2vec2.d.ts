export class Wav2Vec2FeatureExtractor extends FeatureExtractor {
    /**
     * @param {Float32Array} input_values
     * @returns {Float32Array}
     */
    _zero_mean_unit_var_norm(input_values: Float32Array): Float32Array;
    /**
     * Asynchronously extracts features from a given audio using the provided configuration.
     * @param {Float32Array|Float64Array} audio The audio data as a Float32Array/Float64Array.
     * @returns {Promise<{ input_values: Tensor; attention_mask: Tensor }>} A Promise resolving to an object containing the extracted input features and attention mask as Tensors.
     */
    _call(audio: Float32Array | Float64Array): Promise<{
        input_values: Tensor;
        attention_mask: Tensor;
    }>;
}
import { FeatureExtractor } from "../../base/feature_extraction_utils.js";
import { Tensor } from "../../utils/tensor.js";
//# sourceMappingURL=feature_extraction_wav2vec2.d.ts.map