import {
    ImageProcessor,
} from "../../base/image_processors_utils.js";
import { cat, interpolate_4d, slice, stack, Tensor } from "../../utils/tensor.js";

const IMAGE_SIZE = 336;
const SLICE_AXES = [2, 3]; // axes to slice on
const { ceil, floor, sqrt } = Math;

export class Phi3VImageProcessor extends ImageProcessor {
    constructor(config) {
        super({
            ...config,
            do_normalize: true,
            do_pad: true,
            pad_size: 'custom',
            do_convert_rgb: true,
            do_resize: true, // Smart resizing "hd_transform"
        });

        this._num_crops = config.num_crops;
    }
    calc_num_image_tokens_from_image_size(width, height) {
        // @ts-expect-error
        const { num_img_tokens } = this.config;
        return floor(((floor((height / IMAGE_SIZE)) * floor((width / IMAGE_SIZE)) + 1) * num_img_tokens) + 1 + (floor(height / IMAGE_SIZE) + 1) * sqrt(num_img_tokens));
    }

    /** @type {ImageProcessor['get_resize_output_image_size']} */
    get_resize_output_image_size(image, size) {
        const hd_num = this._num_crops;
        const [width, height] = image.size

        let ratio = width / height;
        let scale = 1;

        // Calculate the scaling factor
        while (scale * Math.ceil(scale / ratio) <= hd_num) {
            scale += 1;
        }
        scale -= 1;

        // Compute the new dimensions
        const new_w = Math.floor(scale * 336);
        const new_h = Math.floor(new_w / ratio);

        return [new_w, new_h]
    }


    /** @type {ImageProcessor['pad_image']} */
    pad_image(pixelData, imgDims, padSize, options = {}) {
        // Phi3V uses a custom padding strategy:
        // - Pad to a multiple of 336
        // - Pad with white pixels
        const [imageHeight, imageWidth] = imgDims;
        const height = IMAGE_SIZE * ceil(imageHeight / IMAGE_SIZE);
        const width = IMAGE_SIZE * ceil(imageWidth / IMAGE_SIZE);

        // NOTE: Since padding is done after normalization, we need to fill with the normalized values
        const constant_values = [1, 1, 1].map((x, i) => (x - this.image_mean[i]) / this.image_std[i]);
        return super.pad_image(pixelData, imgDims, { width, height }, {
            center: true,
            constant_values,
            ...options,
        });
    }

    async _call(images, {
        num_crops = null,
    } = {}) {
        // @ts-expect-error
        this._num_crops = num_crops ??= this.config.num_crops;
        if (num_crops < 4 || sqrt(num_crops) % 1 !== 0) {
            throw new Error("num_crops must be a square number >= 4");
        }

        if (!Array.isArray(images)) {
            images = [images];
        }

        const num_images = images.length;
        const imageData = await Promise.all(images.map(x => this.preprocess(x)));

        const original_sizes = imageData.map(x => x.original_size);
        const reshaped_input_sizes = imageData.map(x => x.reshaped_input_size);

        // Process each image in batch
        const all_pixel_values = [];
        for (const { pixel_values } of imageData) {
            pixel_values.unsqueeze_(0); // Easier processing as 4D tensor

            const [height, width] = pixel_values.dims.slice(-2);

            // Global image (Tensor of shape [num_channels, height, width])
            const batch_pixel_values = await interpolate_4d(pixel_values, {
                size: [IMAGE_SIZE, IMAGE_SIZE],
                mode: 'bicubic',
            });

            if (num_crops > 0) {
                const patches = [];
                const sqrt_patches = sqrt(num_crops);
                const patch_width = floor(width / sqrt_patches);
                const patch_height = floor(height / sqrt_patches);
                for (let y = 0; y < sqrt_patches; ++y) {
                    for (let x = 0; x < sqrt_patches; ++x) {
                        let start_x, start_y, end_x, end_y;
                        if (y === sqrt_patches - 1) { // At bottom
                            start_y = height - patch_height;
                            end_y = height;
                        } else {
                            start_y = y * patch_height;
                            end_y = (y + 1) * patch_height;
                        }
                        if (x === sqrt_patches - 1) { // At right
                            start_x = width - patch_width;
                            end_x = width;
                        } else {
                            start_x = x * patch_width;
                            end_x = (x + 1) * patch_width;
                        }

                        const starts = [start_y, start_x];
                        const ends = [end_y, end_x];
                        const patch = await slice(pixel_values, starts, ends, SLICE_AXES);
                        patches.push(patch);
                    }
                }

                const resized_tensors = await interpolate_4d(cat(patches, 0), {
                    size: [IMAGE_SIZE, IMAGE_SIZE],
                    mode: 'bicubic',
                }); // [num_crops, 3, 336, 336]

                // Concatenate the global image with the patches
                all_pixel_values.push(cat([batch_pixel_values, resized_tensors], 0));
            } else {
                // Only use the global image
                // NOTE: Not currently supported in modelling code
                all_pixel_values.push(batch_pixel_values);
            }
        }

        // [num_images, 1 + num_crops, num_channels=3, height, width]
        const pixel_values = stack(all_pixel_values, 0);

        // Calculate padded image sizes
        const sizes = reshaped_input_sizes.map(x => x.map(y => IMAGE_SIZE * ceil(y / IMAGE_SIZE)));

        const image_sizes = new Tensor(
            'int64',
            sizes.flat(),
            [num_images, 2],
        );

        const num_img_tokens = sizes.map(
            ([height, width]) => this.calc_num_image_tokens_from_image_size(width, height),
        );

        return { pixel_values, original_sizes, reshaped_input_sizes, image_sizes, num_img_tokens };
    }
}
