
import { GITHUB_ISSUE_URL, IMAGE_PROCESSOR_NAME } from '../../utils/constants.js';
import { getModelJSON } from '../../utils/hub.js';
import { ImageProcessor } from '../../base/image_processors_utils.js';
import * as AllImageProcessors from '../image_processors.js';

export class AutoImageProcessor {

    /** @type {typeof ImageProcessor.from_pretrained} */
    static async from_pretrained(pretrained_model_name_or_path, options={}) {

        const preprocessorConfig = await getModelJSON(pretrained_model_name_or_path, IMAGE_PROCESSOR_NAME, true, options);

        // Determine image processor class
        const key = preprocessorConfig.image_processor_type ?? preprocessorConfig.feature_extractor_type;
        let image_processor_class = AllImageProcessors[key];

        if (!image_processor_class) {
            if (key !== undefined) {
                // Only log a warning if the class is not found and the key is set.
                console.warn(`Image processor type '${key}' not found, assuming base ImageProcessor. Please report this at ${GITHUB_ISSUE_URL}.`)
            }
            image_processor_class = ImageProcessor;
        }

        // Instantiate image processor
        return new image_processor_class(preprocessorConfig);
    }
}
