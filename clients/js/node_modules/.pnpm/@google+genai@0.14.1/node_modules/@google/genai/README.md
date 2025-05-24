# Google Gen AI SDK for TypeScript and JavaScript

[![NPM Downloads](https://img.shields.io/npm/dw/%40google%2Fgenai)](https://www.npmjs.com/package/@google/genai)
[![Node Current](https://img.shields.io/node/v/%40google%2Fgenai)](https://www.npmjs.com/package/@google/genai)

----------------------
**Documentation:** https://googleapis.github.io/js-genai/

----------------------

The Google Gen AI JavaScript SDK is designed for
TypeScript and JavaScript developers to build applications powered by Gemini. The SDK
supports both the [Gemini Developer API](https://ai.google.dev/gemini-api/docs)
and [Vertex AI](https://cloud.google.com/vertex-ai/generative-ai/docs/learn/overview).

The Google Gen AI SDK is designed to work with Gemini 2.0 features.

> [!NOTE]
> **SDK Preview:**
> See: [Preview Launch](#preview-launch).

> [!CAUTION]
> **API Key Security:** Avoid exposing API keys in client-side code.
> Use server-side implementations in production environments.

## Prerequisites

* Node.js version 18 or later

## Installation

To install the SDK, run the following command:

```shell
npm install @google/genai
```

## Quickstart

The simplest way to get started is to using an API key from
[Google AI Studio](https://aistudio.google.com/apikey):

```typescript
import {GoogleGenAI} from '@google/genai';
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;

const ai = new GoogleGenAI({apiKey: GEMINI_API_KEY});

async function main() {
  const response = await ai.models.generateContent({
    model: 'gemini-2.0-flash-001',
    contents: 'Why is the sky blue?',
  });
  console.log(response.text);
}

main();
```

## Initialization

The Google Gen AI SDK provides support for both the
[Google AI Studio](https://ai.google.dev/gemini-api/docs) and
[Vertex AI](https://cloud.google.com/vertex-ai/generative-ai/docs/learn/overview)
 implementations of the Gemini API.

### Gemini Developer API

For server-side applications, initialize using an API key, which can
be acquired from [Google AI Studio](https://aistudio.google.com/apikey):

```typescript
import { GoogleGenAI } from '@google/genai';
const ai = new GoogleGenAI({apiKey: 'GEMINI_API_KEY'});
```

#### Browser

> [!CAUTION]
> **API Key Security:** Avoid exposing API keys in client-side code.
>   Use server-side implementations in production environments.

In the browser the initialization code is identical:


```typescript
import { GoogleGenAI } from '@google/genai';
const ai = new GoogleGenAI({apiKey: 'GEMINI_API_KEY'});
```

### Vertex AI

Sample code for VertexAI initialization:

```typescript
import { GoogleGenAI } from '@google/genai';

const ai = new GoogleGenAI({
    vertexai: true,
    project: 'your_project',
    location: 'your_location',
});
```

## API Selection

By default, the SDK uses the beta API endpoints provided by Google to support
preview features in the APIs. The stable API endpoints can be selected by
setting the API version to `v1`.

To set the API version use `apiVersion`. For example, to set the API version to
`v1` for Vertex AI:

```typescript
const ai = new GoogleGenAI({
    vertexai: true,
    project: 'your_project',
    location: 'your_location',
    apiVersion: 'v1'
});
```

To set the API version to `v1alpha` for the Gemini Developer API:

```typescript
const ai = new GoogleGenAI({
    apiKey: 'GEMINI_API_KEY',
    apiVersion: 'v1alpha'
});
```

## GoogleGenAI overview

All API features are accessed through an instance of the `GoogleGenAI` classes.
The submodules bundle together related API methods:

- [`ai.models`](https://googleapis.github.io/js-genai/main/classes/models.Models.html):
  Use `models` to query models (`generateContent`, `generateImages`, ...), or
  examine their metadata.
- [`ai.caches`](https://googleapis.github.io/js-genai/main/classes/caches.Caches.html):
  Create and manage `caches` to reduce costs when repeatedly using the same
  large prompt prefix.
- [`ai.chats`](https://googleapis.github.io/js-genai/main/classes/chats.Chats.html):
  Create local stateful `chat` objects to simplify multi turn interactions.
- [`ai.files`](https://googleapis.github.io/js-genai/main/classes/files.Files.html):
  Upload `files` to the API and reference them in your prompts.
  This reduces bandwidth if you use a file many times, and handles files too
  large to fit inline with your prompt.
- [`ai.live`](https://googleapis.github.io/js-genai/main/classes/live.Live.html):
  Start a `live` session for real time interaction, allows text + audio + video
  input, and text or audio output.

## Samples

More samples can be found in the
[github samples directory](https://github.com/googleapis/js-genai/tree/main/sdk-samples).

### Streaming

For quicker, more responsive API interactions use the `generateContentStream`
method which yields chunks as they're generated:

```typescript
import {GoogleGenAI} from '@google/genai';
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;

const ai = new GoogleGenAI({apiKey: GEMINI_API_KEY});

async function main() {
  const response = await ai.models.generateContentStream({
    model: 'gemini-2.0-flash-001',
    contents: 'Write a 100-word poem.',
  });
  for await (const chunk of response) {
    console.log(chunk.text);
  }
}

main();
```

### Function Calling

To let Gemini to interact with external systems, you can provide provide
`functionDeclaration` objects as `tools`. To use these tools it's a 4 step

1. **Declare the function name, description, and parameters**
2. **Call `generateContent` with function calling enabled**
3. **Use the returned `FunctionCall` parameters to call your actual function**
3. **Send the result back to the model (with history, easier in `ai.chat`)
   as a `FunctionResponse`**

```typescript
import {GoogleGenAI, FunctionCallingConfigMode, FunctionDeclaration, Type} from '@google/genai';
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;

async function main() {
  const controlLightDeclaration: FunctionDeclaration = {
    name: 'controlLight',
    parameters: {
      type: Type.OBJECT,
      description: 'Set the brightness and color temperature of a room light.',
      properties: {
        brightness: {
          type: Type.NUMBER,
          description:
              'Light level from 0 to 100. Zero is off and 100 is full brightness.',
        },
        colorTemperature: {
          type: Type.STRING,
          description:
              'Color temperature of the light fixture which can be `daylight`, `cool`, or `warm`.',
        },
      },
      required: ['brightness', 'colorTemperature'],
    },
  };

  const ai = new GoogleGenAI({apiKey: GEMINI_API_KEY});
  const response = await ai.models.generateContent({
    model: 'gemini-2.0-flash-001',
    contents: 'Dim the lights so the room feels cozy and warm.',
    config: {
      toolConfig: {
        functionCallingConfig: {
          // Force it to call any function
          mode: FunctionCallingConfigMode.ANY,
          allowedFunctionNames: ['controlLight'],
        }
      },
      tools: [{functionDeclarations: [controlLightDeclaration]}]
    }
  });

  console.log(response.functionCalls);
}

main();
```

### Generate Content

#### How to structure `contents` argument for `generateContent`

The SDK allows you to specify the following types in the `contents` parameter:

#### Content

- `Content`: The SDK will wrap the singular `Content` instance in an array which
contains only the given content instance
- `Content[]`: No transformation happens

#### Part

Parts will be aggregated on a singular Content, with role 'user'.

- `Part | string`: The SDK will wrap the `string` or `Part` in a `Content`
instance with role 'user'.
- `Part[] | string[]`: The SDK will wrap the full provided list into a single
`Content` with role 'user'.

**_NOTE:_** This doesn't apply to `FunctionCall` and `FunctionResponse` parts,
if you are specifying those, you need to explicitly provide the full
`Content[]` structure making it explicit which Parts are 'spoken' by the model,
or the user. The SDK will throw an exception if you try this.

## Preview Launch

The SDK is curently in a preview launch stage, per [Google's launch stages](https://cloud.google.com/products?hl=en#section-22) this means:

> At Preview, products or features are ready for testing by customers. Preview offerings are often publicly announced, but are not necessarily feature-complete, and no SLAs or technical support commitments are provided for these. Unless stated otherwise by Google, Preview offerings are intended for use in test environments only. The average Preview stage lasts about six months.

## How is this different from the other Google AI SDKs
This SDK (`@google/genai`) is Google Deepmind’s "vanilla" SDK for its generative AI offerings, and is where Google Deepmind adds new AI features.

Models hosted either on the [Vertex AI platform](https://cloud.google.com/vertex-ai/generative-ai/docs/learn/overview) or the [Gemini Developer platform](https://ai.google.dev/gemini-api/docs) are accessible through this SDK.

Other SDKs may be offering additional AI frameworks on top of this SDK, or may be targeting specific project environments (like Firebase).

The `@google/generative_language` and `@google-cloud/vertexai` SDKs are previous iterations of this SDK and are no longer receiving new Gemini 2.0+ features.

