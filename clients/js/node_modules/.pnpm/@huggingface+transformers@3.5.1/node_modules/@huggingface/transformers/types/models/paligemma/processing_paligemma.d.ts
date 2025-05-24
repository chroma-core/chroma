export class PaliGemmaProcessor extends Processor {
    static tokenizer_class: typeof AutoTokenizer;
    static image_processor_class: typeof AutoImageProcessor;
    /**
     * @typedef {import('../../utils/image.js').RawImage} RawImage
     */
    _call(images: import("../../utils/image.js").RawImage | import("../../utils/image.js").RawImage[], text?: any, kwargs?: {}): Promise<any>;
}
import { Processor } from "../../base/processing_utils.js";
import { AutoTokenizer } from "../../tokenizers.js";
import { AutoImageProcessor } from "../auto/image_processing_auto.js";
//# sourceMappingURL=processing_paligemma.d.ts.map