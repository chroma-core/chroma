export class ConvNextImageProcessor extends ImageProcessor {
    constructor(config: any);
    /**
     * Percentage of the image to crop. Only has an effect if this.size < 384.
     */
    crop_pct: any;
    resize(image: any): Promise<any>;
}
export class ConvNextFeatureExtractor extends ConvNextImageProcessor {
}
import { ImageProcessor } from "../../base/image_processors_utils.js";
//# sourceMappingURL=image_processing_convnext.d.ts.map