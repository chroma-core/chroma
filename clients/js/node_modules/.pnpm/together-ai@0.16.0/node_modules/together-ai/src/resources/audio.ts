// File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

import { APIResource } from '../resource';
import * as Core from '../core';
import { type Response } from '../_shims/index';

export class Audio extends APIResource {
  /**
   * Generate audio from input text
   */
  create(body: AudioCreateParams, options?: Core.RequestOptions): Core.APIPromise<Response> {
    return this._client.post('/audio/speech', {
      body,
      ...options,
      headers: { Accept: 'application/octet-stream', ...options?.headers },
      __binaryResponse: true,
    });
  }
}

export type AudioFile = AudioFile.AudioSpeechStreamEvent | AudioFile.StreamSentinel;

export namespace AudioFile {
  export interface AudioSpeechStreamEvent {
    data: AudioSpeechStreamEvent.Data;
  }

  export namespace AudioSpeechStreamEvent {
    export interface Data {
      /**
       * base64 encoded audio stream
       */
      b64: string;

      model: string;

      object: 'audio.tts.chunk';
    }
  }

  export interface StreamSentinel {
    data: '[DONE]';
  }
}

export interface AudioCreateParams {
  /**
   * Input text to generate the audio for
   */
  input: string;

  /**
   * The name of the model to query.
   *
   * [See all of Together AI's chat models](https://docs.together.ai/docs/serverless-models#audio-models)
   */
  model: 'cartesia/sonic' | (string & {});

  /**
   * The voice to use for generating the audio.
   * [View all supported voices here](https://docs.together.ai/docs/text-to-speech#voices-available).
   */
  voice: 'laidback woman' | 'polite man' | 'storyteller lady' | 'friendly sidekick' | (string & {});

  /**
   * Language of input text
   */
  language?:
    | 'en'
    | 'de'
    | 'fr'
    | 'es'
    | 'hi'
    | 'it'
    | 'ja'
    | 'ko'
    | 'nl'
    | 'pl'
    | 'pt'
    | 'ru'
    | 'sv'
    | 'tr'
    | 'zh';

  /**
   * Audio encoding of response
   */
  response_encoding?: 'pcm_f32le' | 'pcm_s16le' | 'pcm_mulaw' | 'pcm_alaw';

  /**
   * The format of audio output
   */
  response_format?: 'mp3' | 'wav' | 'raw';

  /**
   * Sampling rate to use for the output audio
   */
  sample_rate?: number;

  /**
   * If true, output is streamed for several characters at a time instead of waiting
   * for the full response. The stream terminates with `data: [DONE]`. If false,
   * return the encoded audio as octet stream
   */
  stream?: boolean;
}

export declare namespace Audio {
  export { type AudioFile as AudioFile, type AudioCreateParams as AudioCreateParams };
}
