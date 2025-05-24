import { 
    ImageProcessor,
    post_process_semantic_segmentation,
} from "../../base/image_processors_utils.js";


export class SapiensImageProcessor extends ImageProcessor {
    /** @type {typeof post_process_semantic_segmentation} */
    post_process_semantic_segmentation(...args) {
        return post_process_semantic_segmentation(...args);
    }
}
export class SapiensFeatureExtractor extends SapiensImageProcessor { }
