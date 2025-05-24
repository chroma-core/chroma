export class EncodecFeatureExtractor extends FeatureExtractor {
    /**
     * Asynchronously extracts input values from a given audio using the provided configuration.
     * @param {Float32Array|Float64Array} audio The audio data as a Float32Array/Float64Array.
     * @returns {Promise<{ input_values: Tensor; }>} The extracted input values.
     */
    _call(audio: Float32Array | Float64Array): Promise<{
        input_values: Tensor;
    }>;
}
import { FeatureExtractor } from '../../base/feature_extraction_utils.js';
import { Tensor } from '../../utils/tensor.js';
//# sourceMappingURL=feature_extraction_encodec.d.ts.map