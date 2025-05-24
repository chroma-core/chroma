export class JinaCLIPProcessor extends Processor {
    static tokenizer_class: typeof AutoTokenizer;
    static image_processor_class: typeof AutoImageProcessor;
    _call(text?: any, images?: any, kwargs?: {}): Promise<any>;
}
import { Processor } from "../../base/processing_utils.js";
import { AutoTokenizer } from "../../tokenizers.js";
import { AutoImageProcessor } from "../auto/image_processing_auto.js";
//# sourceMappingURL=processing_jina_clip.d.ts.map