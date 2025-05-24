import { 
    ImageProcessor,
    post_process_panoptic_segmentation,
    post_process_instance_segmentation,
} from "../../base/image_processors_utils.js";

export class MaskFormerImageProcessor extends ImageProcessor {

    /** @type {typeof post_process_panoptic_segmentation} */
    post_process_panoptic_segmentation(...args) {
        return post_process_panoptic_segmentation(...args);
    }
    /** @type {typeof post_process_instance_segmentation} */
    post_process_instance_segmentation(...args) {
        return post_process_instance_segmentation(...args);
    }
}
export class MaskFormerFeatureExtractor extends MaskFormerImageProcessor { }
