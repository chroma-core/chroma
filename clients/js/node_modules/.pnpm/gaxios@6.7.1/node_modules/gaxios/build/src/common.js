"use strict";
// Copyright 2018 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
var _a;
Object.defineProperty(exports, "__esModule", { value: true });
exports.GaxiosError = exports.GAXIOS_ERROR_SYMBOL = void 0;
exports.defaultErrorRedactor = defaultErrorRedactor;
const url_1 = require("url");
const util_1 = require("./util");
const extend_1 = __importDefault(require("extend"));
/**
 * Support `instanceof` operator for `GaxiosError`s in different versions of this library.
 *
 * @see {@link GaxiosError[Symbol.hasInstance]}
 */
exports.GAXIOS_ERROR_SYMBOL = Symbol.for(`${util_1.pkg.name}-gaxios-error`);
/* eslint-disable-next-line @typescript-eslint/no-explicit-any */
class GaxiosError extends Error {
    /**
     * Support `instanceof` operator for `GaxiosError` across builds/duplicated files.
     *
     * @see {@link GAXIOS_ERROR_SYMBOL}
     * @see {@link GaxiosError[GAXIOS_ERROR_SYMBOL]}
     */
    static [(_a = exports.GAXIOS_ERROR_SYMBOL, Symbol.hasInstance)](instance) {
        if (instance &&
            typeof instance === 'object' &&
            exports.GAXIOS_ERROR_SYMBOL in instance &&
            instance[exports.GAXIOS_ERROR_SYMBOL] === util_1.pkg.version) {
            return true;
        }
        // fallback to native
        return Function.prototype[Symbol.hasInstance].call(GaxiosError, instance);
    }
    constructor(message, config, response, error) {
        var _b;
        super(message);
        this.config = config;
        this.response = response;
        this.error = error;
        /**
         * Support `instanceof` operator for `GaxiosError` across builds/duplicated files.
         *
         * @see {@link GAXIOS_ERROR_SYMBOL}
         * @see {@link GaxiosError[Symbol.hasInstance]}
         * @see {@link https://github.com/microsoft/TypeScript/issues/13965#issuecomment-278570200}
         * @see {@link https://stackoverflow.com/questions/46618852/require-and-instanceof}
         * @see {@link https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Function/@@hasInstance#reverting_to_default_instanceof_behavior}
         */
        this[_a] = util_1.pkg.version;
        // deep-copy config as we do not want to mutate
        // the existing config for future retries/use
        this.config = (0, extend_1.default)(true, {}, config);
        if (this.response) {
            this.response.config = (0, extend_1.default)(true, {}, this.response.config);
        }
        if (this.response) {
            try {
                this.response.data = translateData(this.config.responseType, (_b = this.response) === null || _b === void 0 ? void 0 : _b.data);
            }
            catch (_c) {
                // best effort - don't throw an error within an error
                // we could set `this.response.config.responseType = 'unknown'`, but
                // that would mutate future calls with this config object.
            }
            this.status = this.response.status;
        }
        if (error && 'code' in error && error.code) {
            this.code = error.code;
        }
        if (config.errorRedactor) {
            config.errorRedactor({
                config: this.config,
                response: this.response,
            });
        }
    }
}
exports.GaxiosError = GaxiosError;
function translateData(responseType, data) {
    switch (responseType) {
        case 'stream':
            return data;
        case 'json':
            return JSON.parse(JSON.stringify(data));
        case 'arraybuffer':
            return JSON.parse(Buffer.from(data).toString('utf8'));
        case 'blob':
            return JSON.parse(data.text());
        default:
            return data;
    }
}
/**
 * An experimental error redactor.
 *
 * @param config Config to potentially redact properties of
 * @param response Config to potentially redact properties of
 *
 * @experimental
 */
function defaultErrorRedactor(data) {
    const REDACT = '<<REDACTED> - See `errorRedactor` option in `gaxios` for configuration>.';
    function redactHeaders(headers) {
        if (!headers)
            return;
        for (const key of Object.keys(headers)) {
            // any casing of `Authentication`
            if (/^authentication$/i.test(key)) {
                headers[key] = REDACT;
            }
            // any casing of `Authorization`
            if (/^authorization$/i.test(key)) {
                headers[key] = REDACT;
            }
            // anything containing secret, such as 'client secret'
            if (/secret/i.test(key)) {
                headers[key] = REDACT;
            }
        }
    }
    function redactString(obj, key) {
        if (typeof obj === 'object' &&
            obj !== null &&
            typeof obj[key] === 'string') {
            const text = obj[key];
            if (/grant_type=/i.test(text) ||
                /assertion=/i.test(text) ||
                /secret/i.test(text)) {
                obj[key] = REDACT;
            }
        }
    }
    function redactObject(obj) {
        if (typeof obj === 'object' && obj !== null) {
            if ('grant_type' in obj) {
                obj['grant_type'] = REDACT;
            }
            if ('assertion' in obj) {
                obj['assertion'] = REDACT;
            }
            if ('client_secret' in obj) {
                obj['client_secret'] = REDACT;
            }
        }
    }
    if (data.config) {
        redactHeaders(data.config.headers);
        redactString(data.config, 'data');
        redactObject(data.config.data);
        redactString(data.config, 'body');
        redactObject(data.config.body);
        try {
            const url = new url_1.URL('', data.config.url);
            if (url.searchParams.has('token')) {
                url.searchParams.set('token', REDACT);
            }
            if (url.searchParams.has('client_secret')) {
                url.searchParams.set('client_secret', REDACT);
            }
            data.config.url = url.toString();
        }
        catch (_b) {
            // ignore error - no need to parse an invalid URL
        }
    }
    if (data.response) {
        defaultErrorRedactor({ config: data.response.config });
        redactHeaders(data.response.headers);
        redactString(data.response, 'data');
        redactObject(data.response.data);
    }
    return data;
}
//# sourceMappingURL=common.js.map