import { 
    ImageProcessor,
} from "../../base/image_processors_utils.js";

export class EfficientNetImageProcessor extends ImageProcessor {
    constructor(config) {
        super(config);
        // @ts-expect-error TS2339
        this.include_top = this.config.include_top ?? true;
        if (this.include_top) {
            this.image_std = this.image_std.map(x => x * x);
        }
    }
}
