

import {
    ImageProcessor,
} from "../../base/image_processors_utils.js";
import { cat, full, interpolate_4d, slice, stack } from "../../utils/tensor.js";

export class Idefics3ImageProcessor extends ImageProcessor {
    constructor(config) {
        super(config);

        this.do_image_splitting = config.do_image_splitting ?? true;
        this.max_image_size = config.max_image_size;
    }

    /**
     * @typedef {import('../../utils/image.js').RawImage} RawImage
     * @typedef {import('../../utils/tensor.js').Tensor} Tensor
     */

    /**
     * Calculate size to resize images to, to be multiples of `vision_encoder_max_size` while preserving the aspect ratio.
     * @param {Tensor} pixel_values Tensor of the image to resize.
     * @param {number} vision_encoder_max_size Maximum size of the output image. If the image is larger than this size,
     * it will be split into patches of this size, and the original image will be concatenated with the patches, resized to max_size.
     */
    get_resize_for_vision_encoder(pixel_values, vision_encoder_max_size) {
        let [height, width] = pixel_values.dims.slice(-2);

        const aspect_ratio = width / height;
        if (width >= height) {
            width = Math.ceil(width / vision_encoder_max_size) * vision_encoder_max_size;
            height = Math.floor(width / aspect_ratio);
            height = Math.ceil(height / vision_encoder_max_size) * vision_encoder_max_size;
        } else {
            height = Math.ceil(height / vision_encoder_max_size) * vision_encoder_max_size;
            width = Math.floor(height * aspect_ratio);
            width = Math.ceil(width / vision_encoder_max_size) * vision_encoder_max_size;
        }
        return { height, width };
    }

    /** @param {RawImage|RawImage[]|RawImage[][]} images */
    async _call(images, {
        do_image_splitting = null,
        return_row_col_info = false,
    } = {}) {

        /** @type {RawImage[][]} */
        let batched_2d_images;
        if (!Array.isArray(images)) {
            batched_2d_images = [[images]];
        } else {
            if (images.length === 0 || !images[0]) {
                throw new Error("No images provided.");
            }
            if (!Array.isArray(images[0])) {
                batched_2d_images = [/** @type {RawImage[]} */(images)];
            } else {
                batched_2d_images = /** @type {RawImage[][]} */(images);
            }
        }

        // List of tensors, each with shape [patches, channels, height, width]
        let all_pixel_values = [];
        let images_list_rows = [];
        let images_list_cols = [];

        const original_sizes = [];
        const reshaped_input_sizes = [];
        for (const image_batch of batched_2d_images) {

            let images_list = await Promise.all(image_batch.map(x => this.preprocess(x)));

            // Original sizes of images
            original_sizes.push(...images_list.map(x => x.original_size));

            // Reshaped sizes of images, before padding or cropping
            reshaped_input_sizes.push(...images_list.map(x => x.reshaped_input_size));

            // Convert images to 4D tensors for easier processing
            images_list.forEach(x => x.pixel_values.unsqueeze_(0));

            const { longest_edge } = this.max_image_size;

            /** @type {Tensor[]} */
            let images_tensor;
            if (do_image_splitting ?? this.do_image_splitting) {
                let image_rows = new Array(images_list.length);
                let image_cols = new Array(images_list.length);

                // We first resize both height and width of each image to the nearest max_image_size multiple, disregarding the aspect ratio
                images_tensor = await Promise.all(images_list.map(async (x, i) => {
                    const new_size = this.get_resize_for_vision_encoder(x.pixel_values, longest_edge);

                    const resized = await interpolate_4d(x.pixel_values, {
                        size: [new_size.height, new_size.width],
                    });

                    const { frames, num_splits_h, num_splits_w } = await this.split_image(resized, this.max_image_size);
                    image_rows[i] = num_splits_h;
                    image_cols[i] = num_splits_w;
                    return cat(frames, 0);
                }));

                images_list_rows.push(image_rows);
                images_list_cols.push(image_cols);

            } else {
                /** @type {[number, number]} */
                const size = [longest_edge, longest_edge];
                images_tensor = await Promise.all(
                    images_list.map(x => interpolate_4d(x.pixel_values, { size }))
                );

                images_list_rows.push(new Array(images_list.length).fill(0));
                images_list_cols.push(new Array(images_list.length).fill(0));
            }

            all_pixel_values.push(cat(images_tensor, 0));
        }

        const batch_size = all_pixel_values.length;
        const [n, c, h, w] = all_pixel_values[0].dims;

        // Stack pixel values
        let pixel_values;
        let pixel_attention_mask;
        if (batch_size === 1) {
            pixel_values = all_pixel_values[0].unsqueeze_(0);
            pixel_attention_mask = full([batch_size, n, h, w], true);
        } else {
            // Add padding (if necessary) to images with less patches than the maximum number of patches
            const max_num_patches = Math.max(...all_pixel_values.map(x => x.dims.at(0)));

            pixel_attention_mask = full([batch_size, max_num_patches, h, w], true);
            const pixel_attention_mask_data = pixel_attention_mask.data;
            const pixel_attention_mask_stride = max_num_patches * h * w;
            for (let i = 0; i < batch_size; ++i) {
                const num_patches = all_pixel_values[i].dims[0];
                if (num_patches < max_num_patches) {
                    all_pixel_values[i] = cat([
                        all_pixel_values[i],
                        full([max_num_patches - num_patches, c, h, w], 0),
                    ], 0);

                    const start_offset = i * pixel_attention_mask_stride + num_patches * h * w;
                    const end_offset = (i + 1) * pixel_attention_mask_stride;

                    // @ts-ignore
                    pixel_attention_mask_data.fill(false, start_offset, end_offset);
                }
            }
            pixel_values = stack(all_pixel_values, 0);
        }

        return {
            pixel_values,
            pixel_attention_mask,

            original_sizes,
            reshaped_input_sizes,
            ...(
                return_row_col_info
                    ? { rows: images_list_rows, cols: images_list_cols }
                    : {}
            ),
        }
    }

    async split_image(pixel_values, { longest_edge }) {
        const max_height = longest_edge;
        const max_width = longest_edge;

        const frames = [];

        const [height, width] = pixel_values.dims.slice(-2);

        let num_splits_h = 0, num_splits_w = 0;

        if (height > max_height || width > max_width) {
            // Calculate the number of splits
            num_splits_h = Math.ceil(height / max_height);
            num_splits_w = Math.ceil(width / max_width);

            // Calculate the optimal width and height for the sub-images
            const optimal_height = Math.ceil(height / num_splits_h);
            const optimal_width = Math.ceil(width / num_splits_w);

            // Iterate through each row and column
            for (let r = 0; r < num_splits_h; ++r) {
                for (let c = 0; c < num_splits_w; ++c) {
                    let start_x, start_y, end_x, end_y;
                    if (r === num_splits_h - 1) { // At bottom
                        start_y = height - optimal_height;
                        end_y = height;
                    } else {
                        start_y = r * optimal_height;
                        end_y = (r + 1) * optimal_height;
                    }
                    if (c === num_splits_w - 1) { // At right
                        start_x = width - optimal_width;
                        end_x = width;
                    } else {
                        start_x = c * optimal_width;
                        end_x = (c + 1) * optimal_width;
                    }

                    const starts = [start_y, start_x];
                    const ends = [end_y, end_x];

                    const patch = await slice(pixel_values, starts, ends, [2, 3]);
                    frames.push(patch);
                }
            }

            // Resize the global image to match max dimensions for memory efficiency
            const global_image_height = max_height;
            const global_image_width = max_width;

            if (height !== global_image_height || width !== global_image_width) {
                pixel_values = await interpolate_4d(pixel_values, {
                    size: [global_image_height, global_image_width],
                })
            }
        }

        frames.push(pixel_values);

        return { frames, num_splits_h, num_splits_w };
    }
}
