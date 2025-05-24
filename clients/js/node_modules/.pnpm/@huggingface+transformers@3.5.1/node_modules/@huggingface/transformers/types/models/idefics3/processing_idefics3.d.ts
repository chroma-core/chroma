export class Idefics3Processor extends Processor {
    static image_processor_class: typeof AutoImageProcessor;
    static tokenizer_class: typeof AutoTokenizer;
    fake_image_token: string;
    image_token: string;
    global_img_token: string;
    /**
     *
     * @param {string|string[]} text
     * @param {RawImage|RawImage[]|RawImage[][]} images
     * @returns {Promise<any>}
     */
    _call(text: string | string[], images?: RawImage | RawImage[] | RawImage[][], options?: {}): Promise<any>;
}
import { Processor } from "../../base/processing_utils.js";
import { RawImage } from "../../utils/image.js";
import { AutoImageProcessor } from "../auto/image_processing_auto.js";
import { AutoTokenizer } from "../../tokenizers.js";
//# sourceMappingURL=processing_idefics3.d.ts.map