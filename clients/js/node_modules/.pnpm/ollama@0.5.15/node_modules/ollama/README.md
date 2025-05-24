# Ollama JavaScript Library

The Ollama JavaScript library provides the easiest way to integrate your JavaScript project with [Ollama](https://github.com/jmorganca/ollama).

## Getting Started

```
npm i ollama
```

## Usage

```javascript
import ollama from 'ollama'

const response = await ollama.chat({
  model: 'llama3.1',
  messages: [{ role: 'user', content: 'Why is the sky blue?' }],
})
console.log(response.message.content)
```

### Browser Usage
To use the library without node, import the browser module.
```javascript
import ollama from 'ollama/browser'
```

## Streaming responses

Response streaming can be enabled by setting `stream: true`, modifying function calls to return an `AsyncGenerator` where each part is an object in the stream.

```javascript
import ollama from 'ollama'

const message = { role: 'user', content: 'Why is the sky blue?' }
const response = await ollama.chat({ model: 'llama3.1', messages: [message], stream: true })
for await (const part of response) {
  process.stdout.write(part.message.content)
}
```

## API

The Ollama JavaScript library's API is designed around the [Ollama REST API](https://github.com/jmorganca/ollama/blob/main/docs/api.md)

### chat

```javascript
ollama.chat(request)
```

- `request` `<Object>`: The request object containing chat parameters.

  - `model` `<string>` The name of the model to use for the chat.
  - `messages` `<Message[]>`: Array of message objects representing the chat history.
    - `role` `<string>`: The role of the message sender ('user', 'system', or 'assistant').
    - `content` `<string>`: The content of the message.
    - `images` `<Uint8Array[] | string[]>`: (Optional) Images to be included in the message, either as Uint8Array or base64 encoded strings.
  - `format` `<string>`: (Optional) Set the expected format of the response (`json`).
  - `stream` `<boolean>`: (Optional) When true an `AsyncGenerator` is returned.
  - `keep_alive` `<string | number>`: (Optional) How long to keep the model loaded. A number (seconds) or a string with a duration unit suffix ("300ms", "1.5h", "2h45m", etc.)
  - `tools` `<Tool[]>`: (Optional) A list of tool calls the model may make.
  - `options` `<Options>`: (Optional) Options to configure the runtime.

- Returns: `<ChatResponse>`

### generate

```javascript
ollama.generate(request)
```

- `request` `<Object>`: The request object containing generate parameters.
  - `model` `<string>` The name of the model to use for the chat.
  - `prompt` `<string>`: The prompt to send to the model.
  - `suffix` `<string>`: (Optional) Suffix is the text that comes after the inserted text.
  - `system` `<string>`: (Optional) Override the model system prompt.
  - `template` `<string>`: (Optional) Override the model template.
  - `raw` `<boolean>`: (Optional) Bypass the prompt template and pass the prompt directly to the model.
  - `images` `<Uint8Array[] | string[]>`: (Optional) Images to be included, either as Uint8Array or base64 encoded strings.
  - `format` `<string>`: (Optional) Set the expected format of the response (`json`).
  - `stream` `<boolean>`: (Optional) When true an `AsyncGenerator` is returned.
  - `keep_alive` `<string | number>`: (Optional) How long to keep the model loaded. A number (seconds) or a string with a duration unit suffix ("300ms", "1.5h", "2h45m", etc.)
  - `options` `<Options>`: (Optional) Options to configure the runtime.
- Returns: `<GenerateResponse>`

### pull

```javascript
ollama.pull(request)
```

- `request` `<Object>`: The request object containing pull parameters.
  - `model` `<string>` The name of the model to pull.
  - `insecure` `<boolean>`: (Optional) Pull from servers whose identity cannot be verified.
  - `stream` `<boolean>`: (Optional) When true an `AsyncGenerator` is returned.
- Returns: `<ProgressResponse>`

### push

```javascript
ollama.push(request)
```

- `request` `<Object>`: The request object containing push parameters.
  - `model` `<string>` The name of the model to push.
  - `insecure` `<boolean>`: (Optional) Push to servers whose identity cannot be verified.
  - `stream` `<boolean>`: (Optional) When true an `AsyncGenerator` is returned.
- Returns: `<ProgressResponse>`

### create

```javascript
ollama.create(request)
```

- `request` `<Object>`: The request object containing create parameters.
  - `model` `<string>` The name of the model to create.
  - `from` `<string>`: The base model to derive from.
  - `stream` `<boolean>`: (Optional) When true an `AsyncGenerator` is returned.
  - `quantize` `<string>`: Quanization precision level (`q8_0`, `q4_K_M`, etc.).
  - `template` `<string>`: (Optional) The prompt template to use with the model.
  - `license` `<string|string[]>`: (Optional) The license(s) associated with the model.
  - `system` `<string>`: (Optional) The system prompt for the model.
  - `parameters` `<Record<string, unknown>>`: (Optional) Additional model parameters as key-value pairs.
  - `messages` `<Message[]>`: (Optional) Initial chat messages for the model.
  - `adapters` `<Record<string, string>>`: (Optional) A key-value map of LoRA adapter configurations.
- Returns: `<ProgressResponse>`

Note: The `files` parameter is not currently supported in `ollama-js`.

### delete

```javascript
ollama.delete(request)
```

- `request` `<Object>`: The request object containing delete parameters.
  - `model` `<string>` The name of the model to delete.
- Returns: `<StatusResponse>`

### copy

```javascript
ollama.copy(request)
```

- `request` `<Object>`: The request object containing copy parameters.
  - `source` `<string>` The name of the model to copy from.
  - `destination` `<string>` The name of the model to copy to.
- Returns: `<StatusResponse>`

### list

```javascript
ollama.list()
```

- Returns: `<ListResponse>`

### show

```javascript
ollama.show(request)
```

- `request` `<Object>`: The request object containing show parameters.
  - `model` `<string>` The name of the model to show.
  - `system` `<string>`: (Optional) Override the model system prompt returned.
  - `template` `<string>`: (Optional) Override the model template returned.
  - `options` `<Options>`: (Optional) Options to configure the runtime.
- Returns: `<ShowResponse>`

### embed

```javascript
ollama.embed(request)
```

- `request` `<Object>`: The request object containing embedding parameters.
  - `model` `<string>` The name of the model used to generate the embeddings.
  - `input` `<string> | <string[]>`: The input used to generate the embeddings.
  - `truncate` `<boolean>`: (Optional) Truncate the input to fit the maximum context length supported by the model.
  - `keep_alive` `<string | number>`: (Optional) How long to keep the model loaded. A number (seconds) or a string with a duration unit suffix ("300ms", "1.5h", "2h45m", etc.)
  - `options` `<Options>`: (Optional) Options to configure the runtime.
- Returns: `<EmbedResponse>`

### ps

```javascript
ollama.ps()
```

- Returns: `<ListResponse>`

### abort

```javascript
ollama.abort()
```

This method will abort **all** streamed generations currently running with the client instance.
If there is a need to manage streams with timeouts, it is recommended to have one Ollama client per stream.

All asynchronous threads listening to streams (typically the ```for await (const part of response)```) will throw an ```AbortError``` exception. See [examples/abort/abort-all-requests.ts](examples/abort/abort-all-requests.ts) for an example.

## Custom client

A custom client can be created with the following fields:

- `host` `<string>`: (Optional) The Ollama host address. Default: `"http://127.0.0.1:11434"`.
- `fetch` `<Object>`: (Optional) The fetch library used to make requests to the Ollama host.

```javascript
import { Ollama } from 'ollama'

const ollama = new Ollama({ host: 'http://127.0.0.1:11434' })
const response = await ollama.chat({
  model: 'llama3.1',
  messages: [{ role: 'user', content: 'Why is the sky blue?' }],
})
```

## Building

To build the project files run:

```sh
npm run build
```
