export class RawImage {
    /**
     * Helper method for reading an image from a variety of input types.
     * @param {RawImage|string|URL|Blob|HTMLCanvasElement|OffscreenCanvas} input
     * @returns The image object.
     *
     * **Example:** Read image from a URL.
     * ```javascript
     * let image = await RawImage.read('https://huggingface.co/datasets/Xenova/transformers.js-docs/resolve/main/football-match.jpg');
     * // RawImage {
     * //   "data": Uint8ClampedArray [ 25, 25, 25, 19, 19, 19, ... ],
     * //   "width": 800,
     * //   "height": 533,
     * //   "channels": 3
     * // }
     * ```
     */
    static read(input: RawImage | string | URL | Blob | HTMLCanvasElement | OffscreenCanvas): Promise<RawImage>;
    /**
     * Read an image from a canvas.
     * @param {HTMLCanvasElement|OffscreenCanvas} canvas The canvas to read the image from.
     * @returns {RawImage} The image object.
     */
    static fromCanvas(canvas: HTMLCanvasElement | OffscreenCanvas): RawImage;
    /**
     * Read an image from a URL or file path.
     * @param {string|URL} url The URL or file path to read the image from.
     * @returns {Promise<RawImage>} The image object.
     */
    static fromURL(url: string | URL): Promise<RawImage>;
    /**
     * Helper method to create a new Image from a blob.
     * @param {Blob} blob The blob to read the image from.
     * @returns {Promise<RawImage>} The image object.
     */
    static fromBlob(blob: Blob): Promise<RawImage>;
    /**
     * Helper method to create a new Image from a tensor
     * @param {Tensor} tensor
     */
    static fromTensor(tensor: Tensor, channel_format?: string): RawImage;
    /**
     * Create a new `RawImage` object.
     * @param {Uint8ClampedArray|Uint8Array} data The pixel data.
     * @param {number} width The width of the image.
     * @param {number} height The height of the image.
     * @param {1|2|3|4} channels The number of channels.
     */
    constructor(data: Uint8ClampedArray | Uint8Array, width: number, height: number, channels: 1 | 2 | 3 | 4);
    data: Uint8Array<ArrayBufferLike> | Uint8ClampedArray<ArrayBufferLike>;
    width: number;
    height: number;
    channels: 2 | 1 | 3 | 4;
    /**
     * Returns the size of the image (width, height).
     * @returns {[number, number]} The size of the image (width, height).
     */
    get size(): [number, number];
    /**
     * Convert the image to grayscale format.
     * @returns {RawImage} `this` to support chaining.
     */
    grayscale(): RawImage;
    /**
     * Convert the image to RGB format.
     * @returns {RawImage} `this` to support chaining.
     */
    rgb(): RawImage;
    /**
     * Convert the image to RGBA format.
     * @returns {RawImage} `this` to support chaining.
     */
    rgba(): RawImage;
    /**
     * Apply an alpha mask to the image. Operates in place.
     * @param {RawImage} mask The mask to apply. It should have a single channel.
     * @returns {RawImage} The masked image.
     * @throws {Error} If the mask is not the same size as the image.
     * @throws {Error} If the image does not have 4 channels.
     * @throws {Error} If the mask is not a single channel.
     */
    putAlpha(mask: RawImage): RawImage;
    /**
     * Resize the image to the given dimensions. This method uses the canvas API to perform the resizing.
     * @param {number} width The width of the new image. `null` or `-1` will preserve the aspect ratio.
     * @param {number} height The height of the new image. `null` or `-1` will preserve the aspect ratio.
     * @param {Object} options Additional options for resizing.
     * @param {0|1|2|3|4|5|string} [options.resample] The resampling method to use.
     * @returns {Promise<RawImage>} `this` to support chaining.
     */
    resize(width: number, height: number, { resample, }?: {
        resample?: 0 | 1 | 2 | 3 | 4 | 5 | string;
    }): Promise<RawImage>;
    pad([left, right, top, bottom]: [any, any, any, any]): Promise<any>;
    crop([x_min, y_min, x_max, y_max]: [any, any, any, any]): Promise<any>;
    center_crop(crop_width: any, crop_height: any): Promise<any>;
    toBlob(type?: string, quality?: number): Promise<any>;
    toTensor(channel_format?: string): Tensor;
    toCanvas(): any;
    /**
     * Split this image into individual bands. This method returns an array of individual image bands from an image.
     * For example, splitting an "RGB" image creates three new images each containing a copy of one of the original bands (red, green, blue).
     *
     * Inspired by PIL's `Image.split()` [function](https://pillow.readthedocs.io/en/latest/reference/Image.html#PIL.Image.Image.split).
     * @returns {RawImage[]} An array containing bands.
     */
    split(): RawImage[];
    /**
     * Helper method to update the image data.
     * @param {Uint8ClampedArray} data The new image data.
     * @param {number} width The new width of the image.
     * @param {number} height The new height of the image.
     * @param {1|2|3|4|null} [channels] The new number of channels of the image.
     * @private
     */
    private _update;
    /**
     * Clone the image
     * @returns {RawImage} The cloned image
     */
    clone(): RawImage;
    /**
     * Helper method for converting image to have a certain number of channels
     * @param {number} numChannels The number of channels. Must be 1, 3, or 4.
     * @returns {RawImage} `this` to support chaining.
     */
    convert(numChannels: number): RawImage;
    /**
     * Save the image to the given path.
     * @param {string} path The path to save the image to.
     */
    save(path: string): Promise<sharp.OutputInfo>;
    toSharp(): sharp.Sharp;
}
/**
 * Helper function to load an image from a URL, path, etc.
 */
export const load_image: any;
import { Tensor } from './tensor.js';
import sharp from 'sharp';
//# sourceMappingURL=image.d.ts.map