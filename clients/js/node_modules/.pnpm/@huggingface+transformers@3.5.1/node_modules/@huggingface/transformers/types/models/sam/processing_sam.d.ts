export class SamProcessor extends Processor {
    static image_processor_class: typeof AutoImageProcessor;
    _call(...args: any[]): Promise<any>;
    post_process_masks(...args: any[]): any;
    reshape_input_points(...args: any[]): any;
}
import { Processor } from "../../base/processing_utils.js";
import { AutoImageProcessor } from "../auto/image_processing_auto.js";
//# sourceMappingURL=processing_sam.d.ts.map