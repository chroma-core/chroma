import { 
    ImageProcessor,
    post_process_object_detection,
} from "../../base/image_processors_utils.js";

export class YolosImageProcessor extends ImageProcessor {
    /** @type {typeof post_process_object_detection} */
    post_process_object_detection(...args) {
        return post_process_object_detection(...args);
    }
}
export class YolosFeatureExtractor extends YolosImageProcessor { }
