import { RawImage } from "./image.js";
import { apis } from "../env.js";

export class RawVideoFrame {

    /**
     * @param {RawImage} image
     * @param {number} timestamp
     */
    constructor(image, timestamp) {
        this.image = image;
        this.timestamp = timestamp;
    }
}

export class RawVideo {
    /**
     * @param {RawVideoFrame[]|RawImage[]} frames
     * @param {number} duration
     */
    constructor(frames, duration) {
        if (frames.length > 0 && frames[0] instanceof RawImage) {
            // Assume uniform timestamps
            frames = frames.map((image, i) => new RawVideoFrame(image, (i + 1) / (frames.length + 1) * duration));
        }
        this.frames = /** @type {RawVideoFrame[]} */ (frames);
        this.duration = duration;
    }

    get width() {
        return this.frames[0].image.width;
    }
    get height() {
        return this.frames[0].image.height;
    }

    get fps() {
        return this.frames.length / this.duration;
    }
}


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
export async function load_video(src, { num_frames = null, fps = null } = {}) {
    if (!apis.IS_BROWSER_ENV) {
        throw new Error("`load_video` is currently only supported in browser environments.");
    }

    // TODO: Support efficiently loading all frames using the WebCodecs API.
    // Specfically, https://developer.mozilla.org/en-US/docs/Web/API/VideoDecoder
    if (num_frames == null && fps == null) {
        throw new Error("Either num_frames or fps must be provided.");
    }

    const frames = [];

    const video = document.createElement("video");
    video.crossOrigin = "anonymous";
    video.muted = true; // mute to allow autoplay and seeking

    if (typeof src === 'string') {
        video.src = src;
    } else if (src instanceof Blob) {
        video.src = URL.createObjectURL(src);
    } else if (src instanceof HTMLVideoElement) {
        video.src = src.src;
    } else {
        throw new Error("Invalid URL or video element provided.");
    }
    // Wait for metadata to load to obtain duration
    await new Promise((resolve) => video.onloadedmetadata = resolve);

    if (video.seekable.start(0) === video.seekable.end(0)) {
        // Fallback: Download entire video if not seekable
        const response = await fetch(video.src);
        const blob = await response.blob();
        video.src = URL.createObjectURL(blob);
        await new Promise((resolve) => video.onloadedmetadata = resolve);
    }

    const duration = video.duration;

    let count, step;
    if (num_frames != null) {
        count = num_frames;
        step = num_frames === 1 ? 0 : duration / (num_frames - 1);
    } else {
        step = 1 / fps;
        count = Math.floor(duration / step);
    }

    // Build an array of sample times based on num_frames or fps
    let sampleTimes = [];
    for (let i = 0; i < count; ++i) {
        sampleTimes.push(num_frames === 1 ? duration / 2 : i * step);
    }

    const canvas = document.createElement("canvas");
    canvas.width = video.videoWidth;
    canvas.height = video.videoHeight;
    const ctx = canvas.getContext("2d", { willReadFrequently: true });
    for (const t of sampleTimes) {
        video.currentTime = t;
        await new Promise((resolve) => {
            video.onseeked = resolve;
        });
        ctx.drawImage(video, 0, 0, canvas.width, canvas.height);
        const imageData = ctx.getImageData(0, 0, canvas.width, canvas.height);
        const frameData = new RawImage(imageData.data, canvas.width, canvas.height, 4);

        const frame = new RawVideoFrame(frameData, t);
        frames.push(frame);
    }

    // Clean up video element.
    video.remove();

    return new RawVideo(frames, duration);
}
