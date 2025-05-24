
export * from './audio_spectrogram_transformer/feature_extraction_audio_spectrogram_transformer.js';
export * from './encodec/feature_extraction_encodec.js';
export * from './clap/feature_extraction_clap.js';
export * from './dac/feature_extraction_dac.js';
export * from './moonshine/feature_extraction_moonshine.js';
export * from './pyannote/feature_extraction_pyannote.js';
export * from './seamless_m4t/feature_extraction_seamless_m4t.js';
export * from './snac/feature_extraction_snac.js';
export * from './speecht5/feature_extraction_speecht5.js';
export * from './wav2vec2/feature_extraction_wav2vec2.js';
export * from './wespeaker/feature_extraction_wespeaker.js';
export * from './whisper/feature_extraction_whisper.js';

// For legacy support, ImageFeatureExtractor is an alias for ImageProcessor
export { ImageProcessor as ImageFeatureExtractor } from "../base/image_processors_utils.js";
