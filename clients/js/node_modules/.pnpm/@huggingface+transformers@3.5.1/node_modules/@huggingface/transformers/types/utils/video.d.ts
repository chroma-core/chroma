/**
 * Loads a video.
 *
 * @param {string|Blob|HTMLVideoElement} src The video to process.
 * @param {Object} [options] Optional parameters.
 * @param {number} [options.num_frames=null] The number of frames to sample uniformly.
 * @param {number} [options.fps=null] The number of frames to sample per second.
 *
 * @returns {Promise<RawVideo>} The loaded video.
 */
export function load_video(src: string | Blob | HTMLVideoElement, { num_frames, fps }?: {
    num_frames?: number;
    fps?: number;
}): Promise<RawVideo>;
export class RawVideoFrame {
    /**
     * @param {RawImage} image
     * @param {number} timestamp
     */
    constructor(image: RawImage, timestamp: number);
    image: RawImage;
    timestamp: number;
}
export class RawVideo {
    /**
     * @param {RawVideoFrame[]|RawImage[]} frames
     * @param {number} duration
     */
    constructor(frames: RawVideoFrame[] | RawImage[], duration: number);
    frames: RawVideoFrame[];
    duration: number;
    get width(): number;
    get height(): number;
    get fps(): number;
}
import { RawImage } from "./image.js";
//# sourceMappingURL=video.d.ts.map