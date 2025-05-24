import { Processor } from "../../base/processing_utils.js";
import { AutoImageProcessor } from "../auto/image_processing_auto.js";

export class SamProcessor extends Processor {
    static image_processor_class = AutoImageProcessor

    async _call(...args) {
        return await this.image_processor(...args);
    }

    post_process_masks(...args) {
        // @ts-ignore
        return this.image_processor.post_process_masks(...args);
    }

    reshape_input_points(...args) {
        // @ts-ignore
        return this.image_processor.reshape_input_points(...args);
    }
}