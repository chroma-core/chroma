

import { IMAGE_PROCESSOR_NAME } from '../../utils/constants.js';
import { getModelJSON } from '../../utils/hub.js';
import { Processor } from '../../base/processing_utils.js';

import * as AllProcessors from '../processors.js';
import * as AllImageProcessors from '../image_processors.js';
import * as AllFeatureExtractors from '../feature_extractors.js';

/**
 * Helper class which is used to instantiate pretrained processors with the `from_pretrained` function.
 * The chosen processor class is determined by the type specified in the processor config.
 * 
 * **Example:** Load a processor using `from_pretrained`.
 * ```javascript
 * let processor = await AutoProcessor.from_pretrained('openai/whisper-tiny.en');
 * ```
 * 
 * **Example:** Run an image through a processor.
 * ```javascript
 * let processor = await AutoProcessor.from_pretrained('Xenova/clip-vit-base-patch16');
 * let image = await RawImage.read('https://huggingface.co/datasets/Xenova/transformers.js-docs/resolve/main/football-match.jpg');
 * let image_inputs = await processor(image);
 * // {
 * //   "pixel_values": {
 * //     "dims": [ 1, 3, 224, 224 ],
 * //     "type": "float32",
 * //     "data": Float32Array [ -1.558687686920166, -1.558687686920166, -1.5440893173217773, ... ],
 * //     "size": 150528
 * //   },
 * //   "original_sizes": [
 * //     [ 533, 800 ]
 * //   ],
 * //   "reshaped_input_sizes": [
 * //     [ 224, 224 ]
 * //   ]
 * // }
 * ```
 */
export class AutoProcessor {

    /** @type {typeof Processor.from_pretrained} */
    static async from_pretrained(pretrained_model_name_or_path, options={}) {

        // TODO: first check for processor.json 
        const preprocessorConfig = await getModelJSON(pretrained_model_name_or_path, IMAGE_PROCESSOR_NAME, true, options);

        const { image_processor_type, feature_extractor_type, processor_class } = preprocessorConfig;
        if (processor_class && AllProcessors[processor_class]) {
            return AllProcessors[processor_class].from_pretrained(pretrained_model_name_or_path, options);
        }

        if (!image_processor_type && !feature_extractor_type) {
            throw new Error('No `image_processor_type` or `feature_extractor_type` found in the config.');
        }

        const components = {};
        if (image_processor_type) {
            const image_processor_class = AllImageProcessors[image_processor_type];
            if (!image_processor_class) {
                throw new Error(`Unknown image_processor_type: '${image_processor_type}'.`);
            }
            components.image_processor = new image_processor_class(preprocessorConfig);
        }

        if (feature_extractor_type) {
            const image_processor_class = AllImageProcessors[feature_extractor_type];
            if (image_processor_class) {
                // Handle legacy case where image processors were specified as feature extractors
                components.image_processor = new image_processor_class(preprocessorConfig);
            } else {
                const feature_extractor_class = AllFeatureExtractors[feature_extractor_type];
                if (!feature_extractor_class) {
                    throw new Error(`Unknown feature_extractor_type: '${feature_extractor_type}'.`);
                }
                components.feature_extractor = new feature_extractor_class(preprocessorConfig);
            }
        }

        const config = {};
        return new Processor(config, components);
    }
}
