
/**
 * @module generation/streamers
 */

import { mergeArrays } from '../utils/core.js';
import { is_chinese_char } from '../tokenizers.js';
import { apis } from '../env.js';

export class BaseStreamer {
    /**
     * Function that is called by `.generate()` to push new tokens
     * @param {bigint[][]} value 
     */
    put(value) {
        throw Error('Not implemented');
    }

    /**
     * Function that is called by `.generate()` to signal the end of generation
     */
    end() {
        throw Error('Not implemented');
    }
}

const stdout_write = apis.IS_PROCESS_AVAILABLE
    ? x => process.stdout.write(x)
    : x => console.log(x);

/**
 * Simple text streamer that prints the token(s) to stdout as soon as entire words are formed.
 */
export class TextStreamer extends BaseStreamer {
    /**
     * 
     * @param {import('../tokenizers.js').PreTrainedTokenizer} tokenizer
     * @param {Object} options
     * @param {boolean} [options.skip_prompt=false] Whether to skip the prompt tokens
     * @param {boolean} [options.skip_special_tokens=true] Whether to skip special tokens when decoding
     * @param {function(string): void} [options.callback_function=null] Function to call when a piece of text is ready to display
     * @param {function(bigint[]): void} [options.token_callback_function=null] Function to call when a new token is generated
     * @param {Object} [options.decode_kwargs={}] Additional keyword arguments to pass to the tokenizer's decode method
     */
    constructor(tokenizer, {
        skip_prompt = false,
        callback_function = null,
        token_callback_function = null,
        skip_special_tokens = true,
        decode_kwargs = {},
        ...kwargs
    } = {}) {
        super();
        this.tokenizer = tokenizer;
        this.skip_prompt = skip_prompt;
        this.callback_function = callback_function ?? stdout_write;
        this.token_callback_function = token_callback_function;
        this.decode_kwargs = { skip_special_tokens, ...decode_kwargs, ...kwargs };

        // variables used in the streaming process
        this.token_cache = [];
        this.print_len = 0;
        this.next_tokens_are_prompt = true;
    }

    /**
     * Receives tokens, decodes them, and prints them to stdout as soon as they form entire words.
     * @param {bigint[][]} value 
     */
    put(value) {
        if (value.length > 1) {
            throw Error('TextStreamer only supports batch size of 1');
        }

        const is_prompt = this.next_tokens_are_prompt;
        if (is_prompt) {
            this.next_tokens_are_prompt = false;
            if (this.skip_prompt) return;
        }

        const tokens = value[0];
        this.token_callback_function?.(tokens)

        // Add the new token to the cache and decodes the entire thing.
        this.token_cache = mergeArrays(this.token_cache, tokens);
        const text = this.tokenizer.decode(this.token_cache, this.decode_kwargs);

        let printable_text;
        if (is_prompt || text.endsWith('\n')) {
            // After the symbol for a new line, we flush the cache.
            printable_text = text.slice(this.print_len);
            this.token_cache = [];
            this.print_len = 0;
        } else if (text.length > 0 && is_chinese_char(text.charCodeAt(text.length - 1))) {
            // If the last token is a CJK character, we print the characters.
            printable_text = text.slice(this.print_len);
            this.print_len += printable_text.length;
        } else {
            // Otherwise, prints until the last space char (simple heuristic to avoid printing incomplete words,
            // which may change with the subsequent token -- there are probably smarter ways to do this!)
            printable_text = text.slice(this.print_len, text.lastIndexOf(' ') + 1);
            this.print_len += printable_text.length;
        }

        this.on_finalized_text(printable_text, false);
    }

    /**
     * Flushes any remaining cache and prints a newline to stdout.
     */
    end() {
        let printable_text;
        if (this.token_cache.length > 0) {
            const text = this.tokenizer.decode(this.token_cache, this.decode_kwargs);
            printable_text = text.slice(this.print_len);
            this.token_cache = [];
            this.print_len = 0;
        } else {
            printable_text = '';
        }
        this.next_tokens_are_prompt = true;
        this.on_finalized_text(printable_text, true);
    }

    /**
     * Prints the new text to stdout. If the stream is ending, also prints a newline.
     * @param {string} text 
     * @param {boolean} stream_end 
     */
    on_finalized_text(text, stream_end) {
        if (text.length > 0) {
            this.callback_function?.(text);
        }
        if (stream_end && this.callback_function === stdout_write && apis.IS_PROCESS_AVAILABLE) {
            this.callback_function?.('\n');
        }
    }
}

/**
 * Utility class to handle streaming of tokens generated by whisper speech-to-text models.
 * Callback functions are invoked when each of the following events occur:
 *  - A new chunk starts (on_chunk_start)
 *  - A new token is generated (callback_function)
 *  - A chunk ends (on_chunk_end)
 *  - The stream is finalized (on_finalize)
 */
export class WhisperTextStreamer extends TextStreamer {
    /**
     * @param {import('../tokenizers.js').WhisperTokenizer} tokenizer
     * @param {Object} options
     * @param {boolean} [options.skip_prompt=false] Whether to skip the prompt tokens
     * @param {function(string): void} [options.callback_function=null] Function to call when a piece of text is ready to display
     * @param {function(bigint[]): void} [options.token_callback_function=null] Function to call when a new token is generated
     * @param {function(number): void} [options.on_chunk_start=null] Function to call when a new chunk starts
     * @param {function(number): void} [options.on_chunk_end=null] Function to call when a chunk ends
     * @param {function(): void} [options.on_finalize=null] Function to call when the stream is finalized
     * @param {number} [options.time_precision=0.02] Precision of the timestamps
     * @param {boolean} [options.skip_special_tokens=true] Whether to skip special tokens when decoding
     * @param {Object} [options.decode_kwargs={}] Additional keyword arguments to pass to the tokenizer's decode method
     */
    constructor(tokenizer, {
        skip_prompt = false,
        callback_function = null,
        token_callback_function = null,
        on_chunk_start = null,
        on_chunk_end = null,
        on_finalize = null,
        time_precision = 0.02,
        skip_special_tokens = true,
        decode_kwargs = {},
    } = {}) {
        super(tokenizer, {
            skip_prompt,
            skip_special_tokens,
            callback_function,
            token_callback_function,
            decode_kwargs,
        });
        this.timestamp_begin = tokenizer.timestamp_begin;

        this.on_chunk_start = on_chunk_start;
        this.on_chunk_end = on_chunk_end;
        this.on_finalize = on_finalize;

        this.time_precision = time_precision;

        this.waiting_for_timestamp = false;
    }

    /**
     * @param {bigint[][]} value 
     */
    put(value) {
        if (value.length > 1) {
            throw Error('WhisperTextStreamer only supports batch size of 1');
        }
        const tokens = value[0];

        // Check if the token is a timestamp
        if (tokens.length === 1) {
            const offset = Number(tokens[0]) - this.timestamp_begin;
            if (offset >= 0) {
                const time = offset * this.time_precision;
                if (this.waiting_for_timestamp) {
                    this.on_chunk_end?.(time);
                } else {
                    this.on_chunk_start?.(time);
                }
                this.waiting_for_timestamp = !this.waiting_for_timestamp; // Toggle
                value = [[]]; // Skip timestamp
            }
        }
        return super.put(value);
    }

    end() {
        super.end();
        this.on_finalize?.();
    }
}
