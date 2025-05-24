// File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.
import { APIResource } from "../../../resource.mjs";
import { isRequestOptions } from "../../../core.mjs";
import { CursorPage } from "../../../pagination.mjs";
export class Messages extends APIResource {
    /**
     * Create a message.
     *
     * @example
     * ```ts
     * const message = await client.beta.threads.messages.create(
     *   'thread_id',
     *   { content: 'string', role: 'user' },
     * );
     * ```
     */
    create(threadId, body, options) {
        return this._client.post(`/threads/${threadId}/messages`, {
            body,
            ...options,
            headers: { 'OpenAI-Beta': 'assistants=v2', ...options?.headers },
        });
    }
    /**
     * Retrieve a message.
     *
     * @example
     * ```ts
     * const message = await client.beta.threads.messages.retrieve(
     *   'thread_id',
     *   'message_id',
     * );
     * ```
     */
    retrieve(threadId, messageId, options) {
        return this._client.get(`/threads/${threadId}/messages/${messageId}`, {
            ...options,
            headers: { 'OpenAI-Beta': 'assistants=v2', ...options?.headers },
        });
    }
    /**
     * Modifies a message.
     *
     * @example
     * ```ts
     * const message = await client.beta.threads.messages.update(
     *   'thread_id',
     *   'message_id',
     * );
     * ```
     */
    update(threadId, messageId, body, options) {
        return this._client.post(`/threads/${threadId}/messages/${messageId}`, {
            body,
            ...options,
            headers: { 'OpenAI-Beta': 'assistants=v2', ...options?.headers },
        });
    }
    list(threadId, query = {}, options) {
        if (isRequestOptions(query)) {
            return this.list(threadId, {}, query);
        }
        return this._client.getAPIList(`/threads/${threadId}/messages`, MessagesPage, {
            query,
            ...options,
            headers: { 'OpenAI-Beta': 'assistants=v2', ...options?.headers },
        });
    }
    /**
     * Deletes a message.
     *
     * @example
     * ```ts
     * const messageDeleted =
     *   await client.beta.threads.messages.del(
     *     'thread_id',
     *     'message_id',
     *   );
     * ```
     */
    del(threadId, messageId, options) {
        return this._client.delete(`/threads/${threadId}/messages/${messageId}`, {
            ...options,
            headers: { 'OpenAI-Beta': 'assistants=v2', ...options?.headers },
        });
    }
}
export class MessagesPage extends CursorPage {
}
Messages.MessagesPage = MessagesPage;
//# sourceMappingURL=messages.mjs.map