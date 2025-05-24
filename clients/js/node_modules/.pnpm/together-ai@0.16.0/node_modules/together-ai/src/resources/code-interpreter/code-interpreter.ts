// File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

import { APIResource } from '../../resource';
import * as Core from '../../core';
import * as SessionsAPI from './sessions';
import { SessionListResponse, Sessions } from './sessions';

export class CodeInterpreter extends APIResource {
  sessions: SessionsAPI.Sessions = new SessionsAPI.Sessions(this._client);

  /**
   * Executes the given code snippet and returns the output. Without a session_id, a
   * new session will be created to run the code. If you do pass in a valid
   * session_id, the code will be run in that session. This is useful for running
   * multiple code snippets in the same environment, because dependencies and similar
   * things are persisted between calls to the same session.
   */
  execute(
    body: CodeInterpreterExecuteParams,
    options?: Core.RequestOptions,
  ): Core.APIPromise<ExecuteResponse> {
    return this._client.post('/tci/execute', { body, ...options });
  }
}

/**
 * The result of the execution. If successful, `data` contains the result and
 * `errors` will be null. If unsuccessful, `data` will be null and `errors` will
 * contain the errors.
 */
export type ExecuteResponse = ExecuteResponse.SuccessfulExecution | ExecuteResponse.FailedExecution;

export namespace ExecuteResponse {
  export interface SuccessfulExecution {
    data: SuccessfulExecution.Data;

    errors: null;
  }

  export namespace SuccessfulExecution {
    export interface Data {
      outputs: Array<Data.StreamOutput | Data.Error | Data.DisplayorExecuteOutput>;

      /**
       * Identifier of the current session. Used to make follow-up calls.
       */
      session_id: string;
    }

    export namespace Data {
      /**
       * Outputs that were printed to stdout or stderr
       */
      export interface StreamOutput {
        data: string;

        type: 'stdout' | 'stderr';
      }

      /**
       * Errors and exceptions that occurred. If this output type is present, your code
       * did not execute successfully.
       */
      export interface Error {
        data: string;

        type: 'error';
      }

      export interface DisplayorExecuteOutput {
        data: DisplayorExecuteOutput.Data;

        type: 'display_data' | 'execute_result';
      }

      export namespace DisplayorExecuteOutput {
        export interface Data {
          'application/geo+json'?: Record<string, unknown>;

          'application/javascript'?: string;

          'application/json'?: Record<string, unknown>;

          'application/pdf'?: string;

          'application/vnd.vega.v5+json'?: Record<string, unknown>;

          'application/vnd.vegalite.v4+json'?: Record<string, unknown>;

          'image/gif'?: string;

          'image/jpeg'?: string;

          'image/png'?: string;

          'image/svg+xml'?: string;

          'text/html'?: string;

          'text/latex'?: string;

          'text/markdown'?: string;

          'text/plain'?: string;
        }
      }
    }
  }

  export interface FailedExecution {
    data: null;

    errors: Array<string | Record<string, unknown>>;
  }
}

export interface CodeInterpreterExecuteParams {
  /**
   * Code snippet to execute.
   */
  code: string;

  /**
   * Programming language for the code to execute. Currently only supports Python,
   * but more will be added.
   */
  language: 'python';

  /**
   * Files to upload to the session. If present, files will be uploaded before
   * executing the given code.
   */
  files?: Array<CodeInterpreterExecuteParams.File>;

  /**
   * Identifier of the current session. Used to make follow-up calls. Requests will
   * return an error if the session does not belong to the caller or has expired.
   */
  session_id?: string;
}

export namespace CodeInterpreterExecuteParams {
  export interface File {
    content: string;

    /**
     * Encoding of the file content. Use `string` for text files such as code, and
     * `base64` for binary files, such as images.
     */
    encoding: 'string' | 'base64';

    name: string;
  }
}

CodeInterpreter.Sessions = Sessions;

export declare namespace CodeInterpreter {
  export {
    type ExecuteResponse as ExecuteResponse,
    type CodeInterpreterExecuteParams as CodeInterpreterExecuteParams,
  };

  export { Sessions as Sessions, type SessionListResponse as SessionListResponse };
}
