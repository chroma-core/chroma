

<p align="center">
    <br/>
    <picture> 
        <source media="(prefers-color-scheme: dark)" srcset="https://huggingface.co/datasets/Xenova/transformers.js-docs/resolve/main/transformersjs-dark.svg" width="500" style="max-width: 100%;">
        <source media="(prefers-color-scheme: light)" srcset="https://huggingface.co/datasets/Xenova/transformers.js-docs/resolve/main/transformersjs-light.svg" width="500" style="max-width: 100%;">
        <img alt="transformers.js javascript library logo" src="https://huggingface.co/datasets/Xenova/transformers.js-docs/resolve/main/transformersjs-light.svg" width="500" style="max-width: 100%;">
    </picture>
    <br/>
</p>

<p align="center">
    <a href="https://www.npmjs.com/package/@huggingface/transformers"><img alt="NPM" src="https://img.shields.io/npm/v/@huggingface/transformers"></a>
    <a href="https://www.npmjs.com/package/@huggingface/transformers"><img alt="NPM Downloads" src="https://img.shields.io/npm/dw/@huggingface/transformers"></a>
    <a href="https://www.jsdelivr.com/package/npm/@huggingface/transformers"><img alt="jsDelivr Hits" src="https://img.shields.io/jsdelivr/npm/hw/@huggingface/transformers"></a>
    <a href="https://github.com/huggingface/transformers.js/blob/main/LICENSE"><img alt="License" src="https://img.shields.io/github/license/huggingface/transformers.js?color=blue"></a>
    <a href="https://huggingface.co/docs/transformers.js/index"><img alt="Documentation" src="https://img.shields.io/website/http/huggingface.co/docs/transformers.js/index.svg?down_color=red&down_message=offline&up_message=online"></a>
</p>


<h3 align="center">
  <p>State-of-the-art Machine Learning for the Web</p>
</h3>

Run 🤗 Transformers directly in your browser, with no need for a server!

Transformers.js is designed to be functionally equivalent to Hugging Face's [transformers](https://github.com/huggingface/transformers) python library, meaning you can run the same pretrained models using a very similar API. These models support common tasks in different modalities, such as:
  - 📝 **Natural Language Processing**: text classification, named entity recognition, question answering, language modeling, summarization, translation, multiple choice, and text generation.
  - 🖼️ **Computer Vision**: image classification, object detection, segmentation, and depth estimation.
  - 🗣️ **Audio**: automatic speech recognition, audio classification, and text-to-speech.
  - 🐙 **Multimodal**: embeddings, zero-shot audio classification, zero-shot image classification, and zero-shot object detection.

Transformers.js uses [ONNX Runtime](https://onnxruntime.ai/) to run models in the browser. The best part about it, is that you can easily [convert](#convert-your-models-to-onnx) your pretrained PyTorch, TensorFlow, or JAX models to ONNX using [🤗 Optimum](https://github.com/huggingface/optimum#onnx--onnx-runtime). 

For more information, check out the full [documentation](https://huggingface.co/docs/transformers.js).


## Installation


To install via [NPM](https://www.npmjs.com/package/@huggingface/transformers), run:
```bash
npm i @huggingface/transformers
```

Alternatively, you can use it in vanilla JS, without any bundler, by using a CDN or static hosting. For example, using [ES Modules](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Modules), you can import the library with:
```html
<script type="module">
    import { pipeline } from 'https://cdn.jsdelivr.net/npm/@huggingface/transformers@3.5.1';
</script>
```


## Quick tour


It's super simple to translate from existing code! Just like the python library, we support the `pipeline` API. Pipelines group together a pretrained model with preprocessing of inputs and postprocessing of outputs, making it the easiest way to run models with the library.

<table>
<tr>
<th width="440px" align="center"><b>Python (original)</b></th>
<th width="440px" align="center"><b>Javascript (ours)</b></th>
</tr>
<tr>
<td>

```python
from transformers import pipeline

# Allocate a pipeline for sentiment-analysis
pipe = pipeline('sentiment-analysis')

out = pipe('I love transformers!')
# [{'label': 'POSITIVE', 'score': 0.999806941}]
```

</td>
<td>

```javascript
import { pipeline } from '@huggingface/transformers';

// Allocate a pipeline for sentiment-analysis
const pipe = await pipeline('sentiment-analysis');

const out = await pipe('I love transformers!');
// [{'label': 'POSITIVE', 'score': 0.999817686}]
```

</td>
</tr>
</table>


You can also use a different model by specifying the model id or path as the second argument to the `pipeline` function. For example:
```javascript
// Use a different model for sentiment-analysis
const pipe = await pipeline('sentiment-analysis', 'Xenova/bert-base-multilingual-uncased-sentiment');
```

By default, when running in the browser, the model will be run on your CPU (via WASM). If you would like
to run the model on your GPU (via WebGPU), you can do this by setting `device: 'webgpu'`, for example:
```javascript
// Run the model on WebGPU
const pipe = await pipeline('sentiment-analysis', 'Xenova/distilbert-base-uncased-finetuned-sst-2-english', {
  device: 'webgpu',
});
```

For more information, check out the [WebGPU guide](https://huggingface.co/docs/transformers.js/guides/webgpu).

> [!WARNING]
> The WebGPU API is still experimental in many browsers, so if you run into any issues,
> please file a [bug report](https://github.com/huggingface/transformers.js/issues/new?title=%5BWebGPU%5D%20Error%20running%20MODEL_ID_GOES_HERE&assignees=&labels=bug,webgpu&projects=&template=1_bug-report.yml).

In resource-constrained environments, such as web browsers, it is advisable to use a quantized version of
the model to lower bandwidth and optimize performance. This can be achieved by adjusting the `dtype` option,
which allows you to select the appropriate data type for your model. While the available options may vary
depending on the specific model, typical choices include `"fp32"` (default for WebGPU), `"fp16"`, `"q8"`
(default for WASM), and `"q4"`. For more information, check out the [quantization guide](https://huggingface.co/docs/transformers.js/guides/dtypes).
```javascript
// Run the model at 4-bit quantization
const pipe = await pipeline('sentiment-analysis', 'Xenova/distilbert-base-uncased-finetuned-sst-2-english', {
  dtype: 'q4',
});
```


## Examples

Want to jump straight in? Get started with one of our sample applications/templates, which can be found [here](https://github.com/huggingface/transformers.js-examples).

| Name              | Description                      | Links                   |
|-------------------|----------------------------------|-------------------------------|
| Whisper Web       | Speech recognition w/ Whisper    | [code](https://github.com/xenova/whisper-web), [demo](https://huggingface.co/spaces/Xenova/whisper-web) |
| Doodle Dash       | Real-time sketch-recognition game | [blog](https://huggingface.co/blog/ml-web-games), [code](https://github.com/xenova/doodle-dash), [demo](https://huggingface.co/spaces/Xenova/doodle-dash) |
| Code Playground   | In-browser code completion website | [code](https://github.com/huggingface/transformers.js/tree/main/examples/code-completion/), [demo](https://huggingface.co/spaces/Xenova/ai-code-playground) |
| Semantic Image Search (client-side) | Search for images with text | [code](https://github.com/huggingface/transformers.js/tree/main/examples/semantic-image-search-client/), [demo](https://huggingface.co/spaces/Xenova/semantic-image-search-client) |
| Semantic Image Search (server-side) | Search for images with text (Supabase) | [code](https://github.com/huggingface/transformers.js/tree/main/examples/semantic-image-search/), [demo](https://huggingface.co/spaces/Xenova/semantic-image-search) |
| Vanilla JavaScript | In-browser object detection     | [video](https://scrimba.com/scrim/cKm9bDAg), [code](https://github.com/huggingface/transformers.js/tree/main/examples/vanilla-js/), [demo](https://huggingface.co/spaces/Scrimba/vanilla-js-object-detector) |
| React             | Multilingual translation website | [code](https://github.com/huggingface/transformers.js/tree/main/examples/react-translator/), [demo](https://huggingface.co/spaces/Xenova/react-translator) |
| Text to speech (client-side) | In-browser speech synthesis | [code](https://github.com/huggingface/transformers.js/tree/main/examples/text-to-speech-client/), [demo](https://huggingface.co/spaces/Xenova/text-to-speech-client) |
| Browser extension | Text classification extension    | [code](https://github.com/huggingface/transformers.js/tree/main/examples/extension/) |
| Electron          | Text classification application  | [code](https://github.com/huggingface/transformers.js/tree/main/examples/electron/)  |
| Next.js (client-side) | Sentiment analysis (in-browser inference) | [code](https://github.com/huggingface/transformers.js/tree/main/examples/next-client/), [demo](https://huggingface.co/spaces/Xenova/next-example-app) |
| Next.js (server-side) | Sentiment analysis (Node.js inference) | [code](https://github.com/huggingface/transformers.js/tree/main/examples/next-server/), [demo](https://huggingface.co/spaces/Xenova/next-server-example-app) |
| Node.js           | Sentiment analysis API           | [code](https://github.com/huggingface/transformers.js/tree/main/examples/node/)      |
| Demo site         | A collection of demos | [code](https://github.com/huggingface/transformers.js/tree/main/examples/demo-site/), [demo](https://huggingface.github.io/transformers.js/) |

Check out the Transformers.js [template](https://huggingface.co/new-space?template=static-templates%2Ftransformers.js) on Hugging Face to get started in one click!


## Custom usage



By default, Transformers.js uses [hosted pretrained models](https://huggingface.co/models?library=transformers.js) and [precompiled WASM binaries](https://cdn.jsdelivr.net/npm/@huggingface/transformers@3.5.1/dist/), which should work out-of-the-box. You can customize this as follows:

### Settings

```javascript
import { env } from '@huggingface/transformers';

// Specify a custom location for models (defaults to '/models/').
env.localModelPath = '/path/to/models/';

// Disable the loading of remote models from the Hugging Face Hub:
env.allowRemoteModels = false;

// Set location of .wasm files. Defaults to use a CDN.
env.backends.onnx.wasm.wasmPaths = '/path/to/files/';
```

For a full list of available settings, check out the [API Reference](https://huggingface.co/docs/transformers.js/api/env).

### Convert your models to ONNX

We recommend using our [conversion script](https://github.com/huggingface/transformers.js/blob/main/scripts/convert.py) to convert your PyTorch, TensorFlow, or JAX models to ONNX in a single command. Behind the scenes, it uses [🤗 Optimum](https://huggingface.co/docs/optimum) to perform conversion and quantization of your model.

```bash
python -m scripts.convert --quantize --model_id <model_name_or_path>
```

For example, convert and quantize [bert-base-uncased](https://huggingface.co/bert-base-uncased) using:
```bash
python -m scripts.convert --quantize --model_id bert-base-uncased
```

This will save the following files to `./models/`:

```
bert-base-uncased/
├── config.json
├── tokenizer.json
├── tokenizer_config.json
└── onnx/
    ├── model.onnx
    └── model_quantized.onnx
```

For the full list of supported architectures, see the [Optimum documentation](https://huggingface.co/docs/optimum/main/en/exporters/onnx/overview).


## Supported tasks/models

Here is the list of all tasks and architectures currently supported by Transformers.js.
If you don't see your task/model listed here or it is not yet supported, feel free
to open up a feature request [here](https://github.com/huggingface/transformers.js/issues/new/choose).

To find compatible models on the Hub, select the "transformers.js" library tag in the filter menu (or visit [this link](https://huggingface.co/models?library=transformers.js)).
You can refine your search by selecting the task you're interested in (e.g., [text-classification](https://huggingface.co/models?pipeline_tag=text-classification&library=transformers.js)).


### Tasks

#### Natural Language Processing

| Task                     | ID | Description | Supported? |
|--------------------------|----|-------------|------------|
| [Fill-Mask](https://huggingface.co/tasks/fill-mask)                     | `fill-mask`   | Masking some of the words in a sentence and predicting which words should replace those masks. | ✅ [(docs)](https://huggingface.co/docs/transformers.js/api/pipelines#module_pipelines.FillMaskPipeline)<br>[(models)](https://huggingface.co/models?pipeline_tag=fill-mask&library=transformers.js) |
| [Question Answering](https://huggingface.co/tasks/question-answering)   | `question-answering`   | Retrieve the answer to a question from a given text. | ✅ [(docs)](https://huggingface.co/docs/transformers.js/api/pipelines#module_pipelines.QuestionAnsweringPipeline)<br>[(models)](https://huggingface.co/models?pipeline_tag=question-answering&library=transformers.js) |
| [Sentence Similarity](https://huggingface.co/tasks/sentence-similarity) | `sentence-similarity`  | Determining how similar two texts are. | ✅ [(docs)](https://huggingface.co/docs/transformers.js/api/pipelines#module_pipelines.FeatureExtractionPipeline)<br>[(models)](https://huggingface.co/models?pipeline_tag=sentence-similarity&library=transformers.js) |
| [Summarization](https://huggingface.co/tasks/summarization)             |  `summarization`  | Producing a shorter version of a document while preserving its important information. | ✅ [(docs)](https://huggingface.co/docs/transformers.js/api/pipelines#module_pipelines.SummarizationPipeline)<br>[(models)](https://huggingface.co/models?pipeline_tag=summarization&library=transformers.js) |
| [Table Question Answering](https://huggingface.co/tasks/table-question-answering) |  `table-question-answering`  | Answering a question about information from a given table. | ❌ |
| [Text Classification](https://huggingface.co/tasks/text-classification)      | `text-classification` or `sentiment-analysis`  | Assigning a label or class to a given text. | ✅ [(docs)](https://huggingface.co/docs/transformers.js/api/pipelines#module_pipelines.TextClassificationPipeline)<br>[(models)](https://huggingface.co/models?pipeline_tag=text-classification&library=transformers.js) |
| [Text Generation](https://huggingface.co/tasks/text-generation#completion-generation-models)          | `text-generation`  | Producing new text by predicting the next word in a sequence. | ✅ [(docs)](https://huggingface.co/docs/transformers.js/api/pipelines#module_pipelines.TextGenerationPipeline)<br>[(models)](https://huggingface.co/models?pipeline_tag=text-generation&library=transformers.js) |
| [Text-to-text Generation](https://huggingface.co/tasks/text-generation#text-to-text-generation-models)  | `text2text-generation`  | Converting one text sequence into another text sequence. | ✅ [(docs)](https://huggingface.co/docs/transformers.js/api/pipelines#module_pipelines.Text2TextGenerationPipeline)<br>[(models)](https://huggingface.co/models?pipeline_tag=text2text-generation&library=transformers.js) |
| [Token Classification](https://huggingface.co/tasks/token-classification)     | `token-classification` or `ner`  | Assigning a label to each token in a text. | ✅ [(docs)](https://huggingface.co/docs/transformers.js/api/pipelines#module_pipelines.TokenClassificationPipeline)<br>[(models)](https://huggingface.co/models?pipeline_tag=token-classification&library=transformers.js) |
| [Translation](https://huggingface.co/tasks/translation)              |  `translation`  | Converting text from one language to another. | ✅ [(docs)](https://huggingface.co/docs/transformers.js/api/pipelines#module_pipelines.TranslationPipeline)<br>[(models)](https://huggingface.co/models?pipeline_tag=translation&library=transformers.js) |
| [Zero-Shot Classification](https://huggingface.co/tasks/zero-shot-classification) | `zero-shot-classification`  | Classifying text into classes that are unseen during training.  | ✅ [(docs)](https://huggingface.co/docs/transformers.js/api/pipelines#module_pipelines.ZeroShotClassificationPipeline)<br>[(models)](https://huggingface.co/models?pipeline_tag=zero-shot-classification&library=transformers.js) |
| [Feature Extraction](https://huggingface.co/tasks/feature-extraction)         |  `feature-extraction`  | Transforming raw data into numerical features that can be processed while preserving the information in the original dataset. | ✅ [(docs)](https://huggingface.co/docs/transformers.js/api/pipelines#module_pipelines.FeatureExtractionPipeline)<br>[(models)](https://huggingface.co/models?pipeline_tag=feature-extraction&library=transformers.js) |

#### Vision

| Task                     | ID | Description | Supported? |
|--------------------------|----|-------------|------------|
| [Background Removal](https://huggingface.co/tasks/image-segmentation#background-removal)       | `background-removal`   | Isolating the main subject of an image by removing or making the background transparent. | ✅ [(docs)](https://huggingface.co/docs/transformers.js/api/pipelines#module_pipelines.BackgroundRemovalPipeline)<br>[(models)](https://huggingface.co/models?other=background-removal&library=transformers.js) |
| [Depth Estimation](https://huggingface.co/tasks/depth-estimation)         |  `depth-estimation`  | Predicting the depth of objects present in an image. | ✅ [(docs)](https://huggingface.co/docs/transformers.js/api/pipelines#module_pipelines.DepthEstimationPipeline)<br>[(models)](https://huggingface.co/models?pipeline_tag=depth-estimation&library=transformers.js) |
| [Image Classification](https://huggingface.co/tasks/image-classification)                | `image-classification`   | Assigning a label or class to an entire image. | ✅ [(docs)](https://huggingface.co/docs/transformers.js/api/pipelines#module_pipelines.ImageClassificationPipeline)<br>[(models)](https://huggingface.co/models?pipeline_tag=image-classification&library=transformers.js) |
| [Image Segmentation](https://huggingface.co/tasks/image-segmentation)       | `image-segmentation`   | Divides an image into segments where each pixel is mapped to an object. This task has multiple variants such as instance segmentation, panoptic segmentation and semantic segmentation. | ✅ [(docs)](https://huggingface.co/docs/transformers.js/api/pipelines#module_pipelines.ImageSegmentationPipeline)<br>[(models)](https://huggingface.co/models?pipeline_tag=image-segmentation&library=transformers.js) |
| [Image-to-Image](https://huggingface.co/tasks/image-to-image)      |  `image-to-image` | Transforming a source image to match the characteristics of a target image or a target image domain. | ✅ [(docs)](https://huggingface.co/docs/transformers.js/api/pipelines#module_pipelines.ImageToImagePipeline)<br>[(models)](https://huggingface.co/models?pipeline_tag=image-to-image&library=transformers.js) |
| [Mask Generation](https://huggingface.co/tasks/mask-generation)            |  `mask-generation`  | Generate masks for the objects in an image. | ❌ |
| [Object Detection](https://huggingface.co/tasks/object-detection)            | `object-detection`   | Identify objects of certain defined classes within an image. | ✅ [(docs)](https://huggingface.co/docs/transformers.js/api/pipelines#module_pipelines.ObjectDetectionPipeline)<br>[(models)](https://huggingface.co/models?pipeline_tag=object-detection&library=transformers.js) |
| [Video Classification](https://huggingface.co/tasks/video-classification) |  n/a  | Assigning a label or class to an entire video. | ❌ |
| [Unconditional Image Generation](https://huggingface.co/tasks/unconditional-image-generation)      |  n/a   | Generating images with no condition in any context (like a prompt text or another image). | ❌ |
| [Image Feature Extraction](https://huggingface.co/tasks/image-feature-extraction)         |  `image-feature-extraction`  | Transforming raw data into numerical features that can be processed while preserving the information in the original image. | ✅ [(docs)](https://huggingface.co/docs/transformers.js/api/pipelines#module_pipelines.ImageFeatureExtractionPipeline)<br>[(models)](https://huggingface.co/models?pipeline_tag=image-feature-extraction&library=transformers.js) |

#### Audio

| Task                     | ID | Description | Supported? |
|--------------------------|----|-------------|------------|
| [Audio Classification](https://huggingface.co/tasks/audio-classification)         |  `audio-classification`  | Assigning a label or class to a given audio. | ✅ [(docs)](https://huggingface.co/docs/transformers.js/api/pipelines#module_pipelines.AudioClassificationPipeline)<br>[(models)](https://huggingface.co/models?pipeline_tag=audio-classification&library=transformers.js) |
| [Audio-to-Audio](https://huggingface.co/tasks/audio-to-audio)         |  n/a  | Generating audio from an input audio source. | ❌ |
| [Automatic Speech Recognition](https://huggingface.co/tasks/automatic-speech-recognition)         | `automatic-speech-recognition`  | Transcribing a given audio into text. | ✅ [(docs)](https://huggingface.co/docs/transformers.js/api/pipelines#module_pipelines.AutomaticSpeechRecognitionPipeline)<br>[(models)](https://huggingface.co/models?pipeline_tag=automatic-speech-recognition&library=transformers.js) |
| [Text-to-Speech](https://huggingface.co/tasks/text-to-speech)         | `text-to-speech` or `text-to-audio` | Generating natural-sounding speech given text input. | ✅ [(docs)](https://huggingface.co/docs/transformers.js/api/pipelines#module_pipelines.TextToAudioPipeline)<br>[(models)](https://huggingface.co/models?pipeline_tag=text-to-audio&library=transformers.js) |


#### Tabular

| Task                     | ID | Description | Supported? |
|--------------------------|----|-------------|------------|
| [Tabular Classification](https://huggingface.co/tasks/tabular-classification)         |  n/a  | Classifying a target category (a group) based on set of attributes. | ❌ |
| [Tabular Regression](https://huggingface.co/tasks/tabular-regression)         |  n/a  | Predicting a numerical value given a set of attributes. | ❌ |


#### Multimodal

| Task                     | ID | Description | Supported? |
|--------------------------|----|-------------|------------|
| [Document Question Answering](https://huggingface.co/tasks/document-question-answering)         | `document-question-answering`  | Answering questions on document images. | ✅ [(docs)](https://huggingface.co/docs/transformers.js/api/pipelines#module_pipelines.DocumentQuestionAnsweringPipeline)<br>[(models)](https://huggingface.co/models?pipeline_tag=document-question-answering&library=transformers.js) |
| [Image-to-Text](https://huggingface.co/tasks/image-to-text)         |  `image-to-text`  | Output text from a given image. | ✅ [(docs)](https://huggingface.co/docs/transformers.js/api/pipelines#module_pipelines.ImageToTextPipeline)<br>[(models)](https://huggingface.co/models?pipeline_tag=image-to-text&library=transformers.js) |
| [Text-to-Image](https://huggingface.co/tasks/text-to-image)         |  `text-to-image`  | Generates images from input text.  | ❌ |
| [Visual Question Answering](https://huggingface.co/tasks/visual-question-answering)         |  `visual-question-answering`  | Answering open-ended questions based on an image. | ❌ |
| [Zero-Shot Audio Classification](https://huggingface.co/learn/audio-course/chapter4/classification_models#zero-shot-audio-classification) | `zero-shot-audio-classification`  | Classifying audios into classes that are unseen during training. | ✅ [(docs)](https://huggingface.co/docs/transformers.js/api/pipelines#module_pipelines.ZeroShotAudioClassificationPipeline)<br>[(models)](https://huggingface.co/models?other=zero-shot-audio-classification&library=transformers.js) |
| [Zero-Shot Image Classification](https://huggingface.co/tasks/zero-shot-image-classification) | `zero-shot-image-classification`  | Classifying images into classes that are unseen during training. | ✅ [(docs)](https://huggingface.co/docs/transformers.js/api/pipelines#module_pipelines.ZeroShotImageClassificationPipeline)<br>[(models)](https://huggingface.co/models?pipeline_tag=zero-shot-image-classification&library=transformers.js) |
| [Zero-Shot Object Detection](https://huggingface.co/tasks/zero-shot-object-detection) | `zero-shot-object-detection`  | Identify objects of classes that are unseen during training. | ✅ [(docs)](https://huggingface.co/docs/transformers.js/api/pipelines#module_pipelines.ZeroShotObjectDetectionPipeline)<br>[(models)](https://huggingface.co/models?other=zero-shot-object-detection&library=transformers.js) |


#### Reinforcement Learning

| Task                     | ID | Description | Supported? |
|--------------------------|----|-------------|------------|
| [Reinforcement Learning](https://huggingface.co/tasks/reinforcement-learning)   |  n/a  | Learning from actions by interacting with an environment through trial and error and receiving rewards (negative or positive) as feedback. | ✅ |



### Models

1. **[ALBERT](https://huggingface.co/docs/transformers/model_doc/albert)** (from Google Research and the Toyota Technological Institute at Chicago) released with the paper [ALBERT: A Lite BERT for Self-supervised Learning of Language Representations](https://arxiv.org/abs/1909.11942), by Zhenzhong Lan, Mingda Chen, Sebastian Goodman, Kevin Gimpel, Piyush Sharma, Radu Soricut.
1. **[Audio Spectrogram Transformer](https://huggingface.co/docs/transformers/model_doc/audio-spectrogram-transformer)** (from MIT) released with the paper [AST: Audio Spectrogram Transformer](https://arxiv.org/abs/2104.01778) by Yuan Gong, Yu-An Chung, James Glass.
1. **[BART](https://huggingface.co/docs/transformers/model_doc/bart)** (from Facebook) released with the paper [BART: Denoising Sequence-to-Sequence Pre-training for Natural Language Generation, Translation, and Comprehension](https://arxiv.org/abs/1910.13461) by Mike Lewis, Yinhan Liu, Naman Goyal, Marjan Ghazvininejad, Abdelrahman Mohamed, Omer Levy, Ves Stoyanov and Luke Zettlemoyer.
1. **[BEiT](https://huggingface.co/docs/transformers/model_doc/beit)** (from Microsoft) released with the paper [BEiT: BERT Pre-Training of Image Transformers](https://arxiv.org/abs/2106.08254) by Hangbo Bao, Li Dong, Furu Wei.
1. **[BERT](https://huggingface.co/docs/transformers/model_doc/bert)** (from Google) released with the paper [BERT: Pre-training of Deep Bidirectional Transformers for Language Understanding](https://arxiv.org/abs/1810.04805) by Jacob Devlin, Ming-Wei Chang, Kenton Lee and Kristina Toutanova.
1. **[Blenderbot](https://huggingface.co/docs/transformers/model_doc/blenderbot)** (from Facebook) released with the paper [Recipes for building an open-domain chatbot](https://arxiv.org/abs/2004.13637) by Stephen Roller, Emily Dinan, Naman Goyal, Da Ju, Mary Williamson, Yinhan Liu, Jing Xu, Myle Ott, Kurt Shuster, Eric M. Smith, Y-Lan Boureau, Jason Weston.
1. **[BlenderbotSmall](https://huggingface.co/docs/transformers/model_doc/blenderbot-small)** (from Facebook) released with the paper [Recipes for building an open-domain chatbot](https://arxiv.org/abs/2004.13637) by Stephen Roller, Emily Dinan, Naman Goyal, Da Ju, Mary Williamson, Yinhan Liu, Jing Xu, Myle Ott, Kurt Shuster, Eric M. Smith, Y-Lan Boureau, Jason Weston.
1. **[BLOOM](https://huggingface.co/docs/transformers/model_doc/bloom)** (from BigScience workshop) released by the [BigScience Workshop](https://bigscience.huggingface.co/).
1. **[CamemBERT](https://huggingface.co/docs/transformers/model_doc/camembert)** (from Inria/Facebook/Sorbonne) released with the paper [CamemBERT: a Tasty French Language Model](https://arxiv.org/abs/1911.03894) by Louis Martin*, Benjamin Muller*, Pedro Javier Ortiz Suárez*, Yoann Dupont, Laurent Romary, Éric Villemonte de la Clergerie, Djamé Seddah and Benoît Sagot.
1. **[Chinese-CLIP](https://huggingface.co/docs/transformers/model_doc/chinese_clip)** (from OFA-Sys) released with the paper [Chinese CLIP: Contrastive Vision-Language Pretraining in Chinese](https://arxiv.org/abs/2211.01335) by An Yang, Junshu Pan, Junyang Lin, Rui Men, Yichang Zhang, Jingren Zhou, Chang Zhou.
1. **[CLAP](https://huggingface.co/docs/transformers/model_doc/clap)** (from LAION-AI) released with the paper [Large-scale Contrastive Language-Audio Pretraining with Feature Fusion and Keyword-to-Caption Augmentation](https://arxiv.org/abs/2211.06687) by Yusong Wu, Ke Chen, Tianyu Zhang, Yuchen Hui, Taylor Berg-Kirkpatrick, Shlomo Dubnov.
1. **[CLIP](https://huggingface.co/docs/transformers/model_doc/clip)** (from OpenAI) released with the paper [Learning Transferable Visual Models From Natural Language Supervision](https://arxiv.org/abs/2103.00020) by Alec Radford, Jong Wook Kim, Chris Hallacy, Aditya Ramesh, Gabriel Goh, Sandhini Agarwal, Girish Sastry, Amanda Askell, Pamela Mishkin, Jack Clark, Gretchen Krueger, Ilya Sutskever.
1. **[CLIPSeg](https://huggingface.co/docs/transformers/model_doc/clipseg)** (from University of Göttingen) released with the paper [Image Segmentation Using Text and Image Prompts](https://arxiv.org/abs/2112.10003) by Timo Lüddecke and Alexander Ecker.
1. **[CodeGen](https://huggingface.co/docs/transformers/model_doc/codegen)** (from Salesforce) released with the paper [A Conversational Paradigm for Program Synthesis](https://arxiv.org/abs/2203.13474) by Erik Nijkamp, Bo Pang, Hiroaki Hayashi, Lifu Tu, Huan Wang, Yingbo Zhou, Silvio Savarese, Caiming Xiong.
1. **[CodeLlama](https://huggingface.co/docs/transformers/model_doc/llama_code)** (from MetaAI) released with the paper [Code Llama: Open Foundation Models for Code](https://ai.meta.com/research/publications/code-llama-open-foundation-models-for-code/) by Baptiste Rozière, Jonas Gehring, Fabian Gloeckle, Sten Sootla, Itai Gat, Xiaoqing Ellen Tan, Yossi Adi, Jingyu Liu, Tal Remez, Jérémy Rapin, Artyom Kozhevnikov, Ivan Evtimov, Joanna Bitton, Manish Bhatt, Cristian Canton Ferrer, Aaron Grattafiori, Wenhan Xiong, Alexandre Défossez, Jade Copet, Faisal Azhar, Hugo Touvron, Louis Martin, Nicolas Usunier, Thomas Scialom, Gabriel Synnaeve.
1. **[Cohere](https://huggingface.co/docs/transformers/main/model_doc/cohere)** (from Cohere) released with the paper [Command-R: Retrieval Augmented Generation at Production Scale](<https://txt.cohere.com/command-r/>) by Cohere.
1. **[ConvBERT](https://huggingface.co/docs/transformers/model_doc/convbert)** (from YituTech) released with the paper [ConvBERT: Improving BERT with Span-based Dynamic Convolution](https://arxiv.org/abs/2008.02496) by Zihang Jiang, Weihao Yu, Daquan Zhou, Yunpeng Chen, Jiashi Feng, Shuicheng Yan.
1. **[ConvNeXT](https://huggingface.co/docs/transformers/model_doc/convnext)** (from Facebook AI) released with the paper [A ConvNet for the 2020s](https://arxiv.org/abs/2201.03545) by Zhuang Liu, Hanzi Mao, Chao-Yuan Wu, Christoph Feichtenhofer, Trevor Darrell, Saining Xie.
1. **[ConvNeXTV2](https://huggingface.co/docs/transformers/model_doc/convnextv2)** (from Facebook AI) released with the paper [ConvNeXt V2: Co-designing and Scaling ConvNets with Masked Autoencoders](https://arxiv.org/abs/2301.00808) by Sanghyun Woo, Shoubhik Debnath, Ronghang Hu, Xinlei Chen, Zhuang Liu, In So Kweon, Saining Xie.
1. **[D-FINE](https://huggingface.co/docs/transformers/model_doc/d_fine)** (from University of Science and Technology of China) released with the paper [D-FINE: Redefine Regression Task in DETRs as Fine-grained Distribution Refinement](https://arxiv.org/abs/2410.13842) by Yansong Peng, Hebei Li, Peixi Wu, Yueyi Zhang, Xiaoyan Sun, Feng Wu.
1. **[DAC](https://huggingface.co/docs/transformers/model_doc/dac)** (from Descript) released with the paper [Descript Audio Codec: High-Fidelity Audio Compression with Improved RVQGAN](https://arxiv.org/abs/2306.06546) by Rithesh Kumar, Prem Seetharaman, Alejandro Luebs, Ishaan Kumar, Kundan Kumar.
1. **[DeBERTa](https://huggingface.co/docs/transformers/model_doc/deberta)** (from Microsoft) released with the paper [DeBERTa: Decoding-enhanced BERT with Disentangled Attention](https://arxiv.org/abs/2006.03654) by Pengcheng He, Xiaodong Liu, Jianfeng Gao, Weizhu Chen.
1. **[DeBERTa-v2](https://huggingface.co/docs/transformers/model_doc/deberta-v2)** (from Microsoft) released with the paper [DeBERTa: Decoding-enhanced BERT with Disentangled Attention](https://arxiv.org/abs/2006.03654) by Pengcheng He, Xiaodong Liu, Jianfeng Gao, Weizhu Chen.
1. **[Decision Transformer](https://huggingface.co/docs/transformers/model_doc/decision_transformer)** (from Berkeley/Facebook/Google) released with the paper [Decision Transformer: Reinforcement Learning via Sequence Modeling](https://arxiv.org/abs/2106.01345) by Lili Chen, Kevin Lu, Aravind Rajeswaran, Kimin Lee, Aditya Grover, Michael Laskin, Pieter Abbeel, Aravind Srinivas, Igor Mordatch.
1. **[DeiT](https://huggingface.co/docs/transformers/model_doc/deit)** (from Facebook) released with the paper [Training data-efficient image transformers & distillation through attention](https://arxiv.org/abs/2012.12877) by Hugo Touvron, Matthieu Cord, Matthijs Douze, Francisco Massa, Alexandre Sablayrolles, Hervé Jégou.
1. **[Depth Anything](https://huggingface.co/docs/transformers/main/model_doc/depth_anything)** (from University of Hong Kong and TikTok) released with the paper [Depth Anything: Unleashing the Power of Large-Scale Unlabeled Data](https://arxiv.org/abs/2401.10891) by Lihe Yang, Bingyi Kang, Zilong Huang, Xiaogang Xu, Jiashi Feng, Hengshuang Zhao.
1. **Depth Pro** (from Apple) released with the paper [Depth Pro: Sharp Monocular Metric Depth in Less Than a Second](https://arxiv.org/abs/2410.02073) by Aleksei Bochkovskii, Amaël Delaunoy, Hugo Germain, Marcel Santos, Yichao Zhou, Stephan R. Richter, Vladlen Koltun.
1. **[DETR](https://huggingface.co/docs/transformers/model_doc/detr)** (from Facebook) released with the paper [End-to-End Object Detection with Transformers](https://arxiv.org/abs/2005.12872) by Nicolas Carion, Francisco Massa, Gabriel Synnaeve, Nicolas Usunier, Alexander Kirillov, Sergey Zagoruyko.
1. **[DINOv2](https://huggingface.co/docs/transformers/model_doc/dinov2)** (from Meta AI) released with the paper [DINOv2: Learning Robust Visual Features without Supervision](https://arxiv.org/abs/2304.07193) by Maxime Oquab, Timothée Darcet, Théo Moutakanni, Huy Vo, Marc Szafraniec, Vasil Khalidov, Pierre Fernandez, Daniel Haziza, Francisco Massa, Alaaeldin El-Nouby, Mahmoud Assran, Nicolas Ballas, Wojciech Galuba, Russell Howes, Po-Yao Huang, Shang-Wen Li, Ishan Misra, Michael Rabbat, Vasu Sharma, Gabriel Synnaeve, Hu Xu, Hervé Jegou, Julien Mairal, Patrick Labatut, Armand Joulin, Piotr Bojanowski.
1. **[DINOv2 with Registers](https://huggingface.co/docs/transformers/model_doc/dinov2_with_registers)** (from Meta AI) released with the paper [DINOv2 with Registers](https://arxiv.org/abs/2309.16588) by Timothée Darcet, Maxime Oquab, Julien Mairal, Piotr Bojanowski.
1. **[DistilBERT](https://huggingface.co/docs/transformers/model_doc/distilbert)** (from HuggingFace), released together with the paper [DistilBERT, a distilled version of BERT: smaller, faster, cheaper and lighter](https://arxiv.org/abs/1910.01108) by Victor Sanh, Lysandre Debut and Thomas Wolf. The same method has been applied to compress GPT2 into [DistilGPT2](https://github.com/huggingface/transformers/tree/main/examples/research_projects/distillation), RoBERTa into [DistilRoBERTa](https://github.com/huggingface/transformers/tree/main/examples/research_projects/distillation), Multilingual BERT into [DistilmBERT](https://github.com/huggingface/transformers/tree/main/examples/research_projects/distillation) and a German version of DistilBERT.
1. **[DiT](https://huggingface.co/docs/transformers/model_doc/dit)** (from Microsoft Research) released with the paper [DiT: Self-supervised Pre-training for Document Image Transformer](https://arxiv.org/abs/2203.02378) by Junlong Li, Yiheng Xu, Tengchao Lv, Lei Cui, Cha Zhang, Furu Wei.
1. **[Donut](https://huggingface.co/docs/transformers/model_doc/donut)** (from NAVER), released together with the paper [OCR-free Document Understanding Transformer](https://arxiv.org/abs/2111.15664) by Geewook Kim, Teakgyu Hong, Moonbin Yim, Jeongyeon Nam, Jinyoung Park, Jinyeong Yim, Wonseok Hwang, Sangdoo Yun, Dongyoon Han, Seunghyun Park.
1. **[DPT](https://huggingface.co/docs/transformers/master/model_doc/dpt)** (from Intel Labs) released with the paper [Vision Transformers for Dense Prediction](https://arxiv.org/abs/2103.13413) by René Ranftl, Alexey Bochkovskiy, Vladlen Koltun.
1. **[EfficientNet](https://huggingface.co/docs/transformers/model_doc/efficientnet)** (from Google Brain) released with the paper [EfficientNet: Rethinking Model Scaling for Convolutional Neural Networks](https://arxiv.org/abs/1905.11946) by Mingxing Tan, Quoc V. Le.
1. **[ELECTRA](https://huggingface.co/docs/transformers/model_doc/electra)** (from Google Research/Stanford University) released with the paper [ELECTRA: Pre-training text encoders as discriminators rather than generators](https://arxiv.org/abs/2003.10555) by Kevin Clark, Minh-Thang Luong, Quoc V. Le, Christopher D. Manning.
1. **[ESM](https://huggingface.co/docs/transformers/model_doc/esm)** (from Meta AI) are transformer protein language models.  **ESM-1b** was released with the paper [Biological structure and function emerge from scaling unsupervised learning to 250 million protein sequences](https://www.pnas.org/content/118/15/e2016239118) by Alexander Rives, Joshua Meier, Tom Sercu, Siddharth Goyal, Zeming Lin, Jason Liu, Demi Guo, Myle Ott, C. Lawrence Zitnick, Jerry Ma, and Rob Fergus. **ESM-1v** was released with the paper [Language models enable zero-shot prediction of the effects of mutations on protein function](https://doi.org/10.1101/2021.07.09.450648) by Joshua Meier, Roshan Rao, Robert Verkuil, Jason Liu, Tom Sercu and Alexander Rives. **ESM-2 and ESMFold** were released with the paper [Language models of protein sequences at the scale of evolution enable accurate structure prediction](https://doi.org/10.1101/2022.07.20.500902) by Zeming Lin, Halil Akin, Roshan Rao, Brian Hie, Zhongkai Zhu, Wenting Lu, Allan dos Santos Costa, Maryam Fazel-Zarandi, Tom Sercu, Sal Candido, Alexander Rives.
1. **EXAONE** (from LG AI Research) released with the papers [EXAONE 3.0 7.8B Instruction Tuned Language Model](https://arxiv.org/abs/2408.03541) and [EXAONE 3.5: Series of Large Language Models for Real-world Use Cases](https://arxiv.org/abs/2412.04862) by the LG AI Research team.
1. **[Falcon](https://huggingface.co/docs/transformers/model_doc/falcon)** (from Technology Innovation Institute) by Almazrouei, Ebtesam and Alobeidli, Hamza and Alshamsi, Abdulaziz and Cappelli, Alessandro and Cojocaru, Ruxandra and Debbah, Merouane and Goffinet, Etienne and Heslow, Daniel and Launay, Julien and Malartic, Quentin and Noune, Badreddine and Pannier, Baptiste and Penedo, Guilherme.
1. **FastViT** (from Apple) released with the paper [FastViT: A Fast Hybrid Vision Transformer using Structural Reparameterization](https://arxiv.org/abs/2303.14189) by Pavan Kumar Anasosalu Vasu, James Gabriel, Jeff Zhu, Oncel Tuzel and Anurag Ranjan.
1. **[FLAN-T5](https://huggingface.co/docs/transformers/model_doc/flan-t5)** (from Google AI) released in the repository [google-research/t5x](https://github.com/google-research/t5x/blob/main/docs/models.md#flan-t5-checkpoints) by Hyung Won Chung, Le Hou, Shayne Longpre, Barret Zoph, Yi Tay, William Fedus, Eric Li, Xuezhi Wang, Mostafa Dehghani, Siddhartha Brahma, Albert Webson, Shixiang Shane Gu, Zhuyun Dai, Mirac Suzgun, Xinyun Chen, Aakanksha Chowdhery, Sharan Narang, Gaurav Mishra, Adams Yu, Vincent Zhao, Yanping Huang, Andrew Dai, Hongkun Yu, Slav Petrov, Ed H. Chi, Jeff Dean, Jacob Devlin, Adam Roberts, Denny Zhou, Quoc V. Le, and Jason Wei
1. **Florence2** (from Microsoft) released with the paper [Florence-2: Advancing a Unified Representation for a Variety of Vision Tasks](https://arxiv.org/abs/2311.06242) by Bin Xiao, Haiping Wu, Weijian Xu, Xiyang Dai, Houdong Hu, Yumao Lu, Michael Zeng, Ce Liu, Lu Yuan.
1. **[Gemma](https://huggingface.co/docs/transformers/main/model_doc/gemma)** (from Google) released with the paper [Gemma: Open Models Based on Gemini Technology and Research](https://blog.google/technology/developers/gemma-open-models/) by the Gemma Google team.
1. **[Gemma2](https://huggingface.co/docs/transformers/main/model_doc/gemma2)** (from Google) released with the paper [Gemma2: Open Models Based on Gemini Technology and Research](https://blog.google/technology/developers/google-gemma-2/) by the Gemma Google team.
1. **[Gemma3](https://huggingface.co/docs/transformers/main/model_doc/gemma3)** (from Google) released with the paper [Introducing Gemma 3: The most capable model you can run on a single GPU or TPU](https://blog.google/technology/developers/gemma-3/) by the Gemma Google team.
1. **[GLM](https://huggingface.co/docs/transformers/main/model_doc/glm)** (from the GLM Team, THUDM & ZhipuAI) released with the paper [ChatGLM: A Family of Large Language Models from GLM-130B to GLM-4 All Tools](https://arxiv.org/abs/2406.12793v2) by Team GLM: Aohan Zeng, Bin Xu, Bowen Wang, Chenhui Zhang, Da Yin, Dan Zhang, Diego Rojas, Guanyu Feng, Hanlin Zhao, Hanyu Lai, Hao Yu, Hongning Wang, Jiadai Sun, Jiajie Zhang, Jiale Cheng, Jiayi Gui, Jie Tang, Jing Zhang, Jingyu Sun, Juanzi Li, Lei Zhao, Lindong Wu, Lucen Zhong, Mingdao Liu, Minlie Huang, Peng Zhang, Qinkai Zheng, Rui Lu, Shuaiqi Duan, Shudan Zhang, Shulin Cao, Shuxun Yang, Weng Lam Tam, Wenyi Zhao, Xiao Liu, Xiao Xia, Xiaohan Zhang, Xiaotao Gu, Xin Lv, Xinghan Liu, Xinyi Liu, Xinyue Yang, Xixuan Song, Xunkai Zhang, Yifan An, Yifan Xu, Yilin Niu, Yuantao Yang, Yueyan Li, Yushi Bai, Yuxiao Dong, Zehan Qi, Zhaoyu Wang, Zhen Yang, Zhengxiao Du, Zhenyu Hou, Zihan Wang.
1. **[GLPN](https://huggingface.co/docs/transformers/model_doc/glpn)** (from KAIST) released with the paper [Global-Local Path Networks for Monocular Depth Estimation with Vertical CutDepth](https://arxiv.org/abs/2201.07436) by Doyeon Kim, Woonghyun Ga, Pyungwhan Ahn, Donggyu Joo, Sehwan Chun, Junmo Kim.
1. **[GPT Neo](https://huggingface.co/docs/transformers/model_doc/gpt_neo)** (from EleutherAI) released in the repository [EleutherAI/gpt-neo](https://github.com/EleutherAI/gpt-neo) by Sid Black, Stella Biderman, Leo Gao, Phil Wang and Connor Leahy.
1. **[GPT NeoX](https://huggingface.co/docs/transformers/model_doc/gpt_neox)** (from EleutherAI) released with the paper [GPT-NeoX-20B: An Open-Source Autoregressive Language Model](https://arxiv.org/abs/2204.06745) by Sid Black, Stella Biderman, Eric Hallahan, Quentin Anthony, Leo Gao, Laurence Golding, Horace He, Connor Leahy, Kyle McDonell, Jason Phang, Michael Pieler, USVSN Sai Prashanth, Shivanshu Purohit, Laria Reynolds, Jonathan Tow, Ben Wang, Samuel Weinbach
1. **[GPT-2](https://huggingface.co/docs/transformers/model_doc/gpt2)** (from OpenAI) released with the paper [Language Models are Unsupervised Multitask Learners](https://blog.openai.com/better-language-models/) by Alec Radford*, Jeffrey Wu*, Rewon Child, David Luan, Dario Amodei** and Ilya Sutskever**.
1. **[GPT-J](https://huggingface.co/docs/transformers/model_doc/gptj)** (from EleutherAI) released in the repository [kingoflolz/mesh-transformer-jax](https://github.com/kingoflolz/mesh-transformer-jax/) by Ben Wang and Aran Komatsuzaki.
1. **[GPTBigCode](https://huggingface.co/docs/transformers/model_doc/gpt_bigcode)** (from BigCode) released with the paper [SantaCoder: don't reach for the stars!](https://arxiv.org/abs/2301.03988) by Loubna Ben Allal, Raymond Li, Denis Kocetkov, Chenghao Mou, Christopher Akiki, Carlos Munoz Ferrandis, Niklas Muennighoff, Mayank Mishra, Alex Gu, Manan Dey, Logesh Kumar Umapathi, Carolyn Jane Anderson, Yangtian Zi, Joel Lamy Poirier, Hailey Schoelkopf, Sergey Troshin, Dmitry Abulkhanov, Manuel Romero, Michael Lappert, Francesco De Toni, Bernardo García del Río, Qian Liu, Shamik Bose, Urvashi Bhattacharyya, Terry Yue Zhuo, Ian Yu, Paulo Villegas, Marco Zocca, Sourab Mangrulkar, David Lansky, Huu Nguyen, Danish Contractor, Luis Villa, Jia Li, Dzmitry Bahdanau, Yacine Jernite, Sean Hughes, Daniel Fried, Arjun Guha, Harm de Vries, Leandro von Werra.
1. **[Granite](https://huggingface.co/docs/transformers/main/model_doc/granite)** (from IBM) released with the paper [Power Scheduler: A Batch Size and Token Number Agnostic Learning Rate Scheduler](https://arxiv.org/abs/2408.13359) by Yikang Shen, Matthew Stallone, Mayank Mishra, Gaoyuan Zhang, Shawn Tan, Aditya Prasad, Adriana Meza Soria, David D. Cox, Rameswar Panda.
1. **[Grounding DINO](https://huggingface.co/docs/transformers/model_doc/grounding-dino)** (from IDEA-Research) released with the paper [Grounding DINO: Marrying DINO with Grounded Pre-Training for Open-Set Object Detection](https://arxiv.org/abs/2303.05499) by Shilong Liu, Zhaoyang Zeng, Tianhe Ren, Feng Li, Hao Zhang, Jie Yang, Qing Jiang, Chunyuan Li, Jianwei Yang, Hang Su, Jun Zhu, Lei Zhang.
1. **[GroupViT](https://huggingface.co/docs/transformers/model_doc/groupvit)** (from UCSD, NVIDIA) released with the paper [GroupViT: Semantic Segmentation Emerges from Text Supervision](https://arxiv.org/abs/2202.11094) by Jiarui Xu, Shalini De Mello, Sifei Liu, Wonmin Byeon, Thomas Breuel, Jan Kautz, Xiaolong Wang.
1. **[Helium](https://huggingface.co/docs/transformers/main/model_doc/helium)** (from the Kyutai Team) released with the blog post [Announcing Helium-1 Preview](https://kyutai.org/2025/01/13/helium.html) by the Kyutai Team.
1. **[HerBERT](https://huggingface.co/docs/transformers/model_doc/herbert)** (from Allegro.pl, AGH University of Science and Technology) released with the paper [KLEJ: Comprehensive Benchmark for Polish Language Understanding](https://www.aclweb.org/anthology/2020.acl-main.111.pdf) by Piotr Rybak, Robert Mroczkowski, Janusz Tracz, Ireneusz Gawlik.
1. **[Hiera](https://huggingface.co/docs/transformers/model_doc/hiera)** (from Meta) released with the paper [Hiera: A Hierarchical Vision Transformer without the Bells-and-Whistles](https://arxiv.org/pdf/2306.00989) by Chaitanya Ryali, Yuan-Ting Hu, Daniel Bolya, Chen Wei, Haoqi Fan, Po-Yao Huang, Vaibhav Aggarwal, Arkabandhu Chowdhury, Omid Poursaeed, Judy Hoffman, Jitendra Malik, Yanghao Li, Christoph Feichtenhofer.
1. **[Hubert](https://huggingface.co/docs/transformers/model_doc/hubert)** (from Facebook) released with the paper [HuBERT: Self-Supervised Speech Representation Learning by Masked Prediction of Hidden Units](https://arxiv.org/abs/2106.07447) by Wei-Ning Hsu, Benjamin Bolte, Yao-Hung Hubert Tsai, Kushal Lakhotia, Ruslan Salakhutdinov, Abdelrahman Mohamed.
1. **[I-JEPA](https://huggingface.co/docs/transformers/model_doc/ijepa)** (from Meta) released with the paper [Self-Supervised Learning from Images with a Joint-Embedding Predictive Architecture](https://arxiv.org/abs/2301.08243) by Mahmoud Assran, Quentin Duval, Ishan Misra, Piotr Bojanowski, Pascal Vincent, Michael Rabbat, Yann LeCun, Nicolas Ballas.
1. **[Idefics3](https://huggingface.co/docs/transformers/model_doc/idefics3)** (from Hugging Face) released with the paper [Building and better understanding vision-language models: insights and future directions](https://arxiv.org/abs/2408.12637) by Hugo Laurençon, Andrés Marafioti, Victor Sanh, Léo Tronchon.
1. **JAIS** (from Core42) released with the paper [Jais and Jais-chat: Arabic-Centric Foundation and Instruction-Tuned Open Generative Large Language Models](https://arxiv.org/pdf/2308.16149) by Neha Sengupta, Sunil Kumar Sahu, Bokang Jia, Satheesh Katipomu, Haonan Li, Fajri Koto, William Marshall, Gurpreet Gosal, Cynthia Liu, Zhiming Chen, Osama Mohammed Afzal, Samta Kamboj, Onkar Pandit, Rahul Pal, Lalit Pradhan, Zain Muhammad Mujahid, Massa Baali, Xudong Han, Sondos Mahmoud Bsharat, Alham Fikri Aji, Zhiqiang Shen, Zhengzhong Liu, Natalia Vassilieva, Joel Hestness, Andy Hock, Andrew Feldman, Jonathan Lee, Andrew Jackson, Hector Xuguang Ren, Preslav Nakov, Timothy Baldwin, Eric Xing.
1. **Janus** (from DeepSeek) released with the paper [Janus: Decoupling Visual Encoding for Unified Multimodal Understanding and Generation](https://arxiv.org/abs/2410.13848) Chengyue Wu, Xiaokang Chen, Zhiyu Wu, Yiyang Ma, Xingchao Liu, Zizheng Pan, Wen Liu, Zhenda Xie, Xingkai Yu, Chong Ruan, Ping Luo.
1. **JinaCLIP** (from Jina AI) released with the paper [Jina CLIP: Your CLIP Model Is Also Your Text Retriever](https://arxiv.org/abs/2405.20204) by Andreas Koukounas, Georgios Mastrapas, Michael Günther, Bo Wang, Scott Martens, Isabelle Mohr, Saba Sturua, Mohammad Kalim Akram, Joan Fontanals Martínez, Saahil Ognawala, Susana Guzman, Maximilian Werk, Nan Wang, Han Xiao.
1. **LiteWhisper** (from University of Washington, Kotoba Technologies) released with the paper [LiteASR: Efficient Automatic Speech Recognition with Low-Rank Approximation](https://arxiv.org/abs/2502.20583) by Keisuke Kamahori, Jungo Kasai, Noriyuki Kojima, Baris Kasikci.
1. **[LongT5](https://huggingface.co/docs/transformers/model_doc/longt5)** (from Google AI) released with the paper [LongT5: Efficient Text-To-Text Transformer for Long Sequences](https://arxiv.org/abs/2112.07916) by Mandy Guo, Joshua Ainslie, David Uthus, Santiago Ontanon, Jianmo Ni, Yun-Hsuan Sung, Yinfei Yang.
1. **[LLaMA](https://huggingface.co/docs/transformers/model_doc/llama)** (from The FAIR team of Meta AI) released with the paper [LLaMA: Open and Efficient Foundation Language Models](https://arxiv.org/abs/2302.13971) by Hugo Touvron, Thibaut Lavril, Gautier Izacard, Xavier Martinet, Marie-Anne Lachaux, Timothée Lacroix, Baptiste Rozière, Naman Goyal, Eric Hambro, Faisal Azhar, Aurelien Rodriguez, Armand Joulin, Edouard Grave, Guillaume Lample.
1. **[Llama2](https://huggingface.co/docs/transformers/model_doc/llama2)** (from The FAIR team of Meta AI) released with the paper [Llama2: Open Foundation and Fine-Tuned Chat Models](https://ai.meta.com/research/publications/llama-2-open-foundation-and-fine-tuned-chat-models/XXX) by Hugo Touvron, Louis Martin, Kevin Stone, Peter Albert, Amjad Almahairi, Yasmine Babaei, Nikolay Bashlykov, Soumya Batra, Prajjwal Bhargava, Shruti Bhosale, Dan Bikel, Lukas Blecher, Cristian Canton Ferrer, Moya Chen, Guillem Cucurull, David Esiobu, Jude Fernandes, Jeremy Fu, Wenyin Fu, Brian Fuller, Cynthia Gao, Vedanuj Goswami, Naman Goyal, Anthony Hartshorn, Saghar Hosseini, Rui Hou, Hakan Inan, Marcin Kardas, Viktor Kerkez Madian Khabsa, Isabel Kloumann, Artem Korenev, Punit Singh Koura, Marie-Anne Lachaux, Thibaut Lavril, Jenya Lee, Diana Liskovich, Yinghai Lu, Yuning Mao, Xavier Martinet, Todor Mihaylov, Pushka rMishra, Igor Molybog, Yixin Nie, Andrew Poulton, Jeremy Reizenstein, Rashi Rungta, Kalyan Saladi, Alan Schelten, Ruan Silva, Eric Michael Smith, Ranjan Subramanian, Xiaoqing EllenTan, Binh Tang, Ross Taylor, Adina Williams, Jian Xiang Kuan, Puxin Xu, Zheng Yan, Iliyan Zarov, Yuchen Zhang, Angela Fan, Melanie Kambadur, Sharan Narang, Aurelien Rodriguez, Robert Stojnic, Sergey Edunov, Thomas Scialom.
1. **[LLaVa](https://huggingface.co/docs/transformers/model_doc/llava)** (from Microsoft Research & University of Wisconsin-Madison) released with the paper [Visual Instruction Tuning](https://arxiv.org/abs/2304.08485) by Haotian Liu, Chunyuan Li, Yuheng Li and Yong Jae Lee.
1. **[LLaVA-OneVision](https://huggingface.co/docs/transformers/model_doc/llava_onevision)** (from ByteDance & NTU & CUHK & HKUST) released with the paper [LLaVA-OneVision: Easy Visual Task Transfer](https://arxiv.org/abs/2408.03326) by Bo Li, Yuanhan Zhang, Dong Guo, Renrui Zhang, Feng Li, Hao Zhang, Kaichen Zhang, Yanwei Li, Ziwei Liu, Chunyuan Li
1. **[M2M100](https://huggingface.co/docs/transformers/model_doc/m2m_100)** (from Facebook) released with the paper [Beyond English-Centric Multilingual Machine Translation](https://arxiv.org/abs/2010.11125) by Angela Fan, Shruti Bhosale, Holger Schwenk, Zhiyi Ma, Ahmed El-Kishky, Siddharth Goyal, Mandeep Baines, Onur Celebi, Guillaume Wenzek, Vishrav Chaudhary, Naman Goyal, Tom Birch, Vitaliy Liptchinsky, Sergey Edunov, Edouard Grave, Michael Auli, Armand Joulin.
1. **[MarianMT](https://huggingface.co/docs/transformers/model_doc/marian)** Machine translation models trained using [OPUS](http://opus.nlpl.eu/) data by Jörg Tiedemann. The [Marian Framework](https://marian-nmt.github.io/) is being developed by the Microsoft Translator Team.
1. **[MaskFormer](https://huggingface.co/docs/transformers/model_doc/maskformer)** (from Meta and UIUC) released with the paper [Per-Pixel Classification is Not All You Need for Semantic Segmentation](https://arxiv.org/abs/2107.06278) by Bowen Cheng, Alexander G. Schwing, Alexander Kirillov.
1. **[mBART](https://huggingface.co/docs/transformers/model_doc/mbart)** (from Facebook) released with the paper [Multilingual Denoising Pre-training for Neural Machine Translation](https://arxiv.org/abs/2001.08210) by Yinhan Liu, Jiatao Gu, Naman Goyal, Xian Li, Sergey Edunov, Marjan Ghazvininejad, Mike Lewis, Luke Zettlemoyer.
1. **[mBART-50](https://huggingface.co/docs/transformers/model_doc/mbart)** (from Facebook) released with the paper [Multilingual Translation with Extensible Multilingual Pretraining and Finetuning](https://arxiv.org/abs/2008.00401) by Yuqing Tang, Chau Tran, Xian Li, Peng-Jen Chen, Naman Goyal, Vishrav Chaudhary, Jiatao Gu, Angela Fan.
1. **Metric3D** released with the paper [Metric3D: Towards Zero-shot Metric 3D Prediction from A Single Image](https://arxiv.org/abs/2307.10984) by Wei Yin, Chi Zhang, Hao Chen, Zhipeng Cai, Gang Yu, Kaixuan Wang, Xiaozhi Chen, Chunhua Shen.
1. **Metric3Dv2** released with the paper [Metric3Dv2: A Versatile Monocular Geometric Foundation Model for Zero-shot Metric Depth and Surface Normal Estimation](https://arxiv.org/abs/2404.15506) by Mu Hu, Wei Yin, Chi Zhang, Zhipeng Cai, Xiaoxiao Long, Kaixuan Wang, Hao Chen, Gang Yu, Chunhua Shen, Shaojie Shen.
1. **[MusicGen](https://huggingface.co/docs/transformers/model_doc/musicgen)** (from Meta) released with the paper [Simple and Controllable Music Generation](https://arxiv.org/abs/2306.05284) by Jade Copet, Felix Kreuk, Itai Gat, Tal Remez, David Kant, Gabriel Synnaeve, Yossi Adi and Alexandre Défossez.
1. **[MGP-STR](https://huggingface.co/docs/transformers/model_doc/mgp-str)** (from Alibaba Research) released with the paper [Multi-Granularity Prediction for Scene Text Recognition](https://arxiv.org/abs/2209.03592) by Peng Wang, Cheng Da, and Cong Yao.
1. **[Mimi](https://huggingface.co/docs/transformers/model_doc/mimi)** (from Kyutai) released with the paper [Moshi: a speech-text foundation model for real-time dialogue](https://arxiv.org/abs/2410.00037) by Alexandre Défossez, Laurent Mazaré, Manu Orsini, Amélie Royer, Patrick Pérez, Hervé Jégou, Edouard Grave and Neil Zeghidour.
1. **[Mistral](https://huggingface.co/docs/transformers/model_doc/mistral)** (from Mistral AI) by The [Mistral AI](https://mistral.ai) team: Albert Jiang, Alexandre Sablayrolles, Arthur Mensch, Chris Bamford, Devendra Singh Chaplot, Diego de las Casas, Florian Bressand, Gianna Lengyel, Guillaume Lample, Lélio Renard Lavaud, Lucile Saulnier, Marie-Anne Lachaux, Pierre Stock, Teven Le Scao, Thibaut Lavril, Thomas Wang, Timothée Lacroix, William El Sayed.
1. **[MMS](https://huggingface.co/docs/transformers/model_doc/mms)** (from Facebook) released with the paper [Scaling Speech Technology to 1,000+ Languages](https://arxiv.org/abs/2305.13516) by Vineel Pratap, Andros Tjandra, Bowen Shi, Paden Tomasello, Arun Babu, Sayani Kundu, Ali Elkahky, Zhaoheng Ni, Apoorv Vyas, Maryam Fazel-Zarandi, Alexei Baevski, Yossi Adi, Xiaohui Zhang, Wei-Ning Hsu, Alexis Conneau, Michael Auli.
1. **[MobileBERT](https://huggingface.co/docs/transformers/model_doc/mobilebert)** (from CMU/Google Brain) released with the paper [MobileBERT: a Compact Task-Agnostic BERT for Resource-Limited Devices](https://arxiv.org/abs/2004.02984) by Zhiqing Sun, Hongkun Yu, Xiaodan Song, Renjie Liu, Yiming Yang, and Denny Zhou.
1. **MobileCLIP** (from Apple) released with the paper [MobileCLIP: Fast Image-Text Models through Multi-Modal Reinforced Training](https://arxiv.org/abs/2311.17049) by Pavan Kumar Anasosalu Vasu, Hadi Pouransari, Fartash Faghri, Raviteja Vemulapalli, Oncel Tuzel.
1. **MobileLLM** (from Meta) released with the paper [MobileLLM: Optimizing Sub-billion Parameter Language Models for On-Device Use Cases](https://arxiv.org/abs/2402.14905) by Zechun Liu, Changsheng Zhao, Forrest Iandola, Chen Lai, Yuandong Tian, Igor Fedorov, Yunyang Xiong, Ernie Chang, Yangyang Shi, Raghuraman Krishnamoorthi, Liangzhen Lai, Vikas Chandra.
1. **[MobileNetV1](https://huggingface.co/docs/transformers/model_doc/mobilenet_v1)** (from Google Inc.) released with the paper [MobileNets: Efficient Convolutional Neural Networks for Mobile Vision Applications](https://arxiv.org/abs/1704.04861) by Andrew G. Howard, Menglong Zhu, Bo Chen, Dmitry Kalenichenko, Weijun Wang, Tobias Weyand, Marco Andreetto, Hartwig Adam.
1. **[MobileNetV2](https://huggingface.co/docs/transformers/model_doc/mobilenet_v2)** (from Google Inc.) released with the paper [MobileNetV2: Inverted Residuals and Linear Bottlenecks](https://arxiv.org/abs/1801.04381) by Mark Sandler, Andrew Howard, Menglong Zhu, Andrey Zhmoginov, Liang-Chieh Chen.
1. **MobileNetV3** (from Google Inc.) released with the paper [Searching for MobileNetV3](https://arxiv.org/abs/1905.02244) by Andrew Howard, Mark Sandler, Grace Chu, Liang-Chieh Chen, Bo Chen, Mingxing Tan, Weijun Wang, Yukun Zhu, Ruoming Pang, Vijay Vasudevan, Quoc V. Le, Hartwig Adam.
1. **MobileNetV4** (from Google Inc.) released with the paper [MobileNetV4 - Universal Models for the Mobile Ecosystem](https://arxiv.org/abs/2404.10518) by Danfeng Qin, Chas Leichner, Manolis Delakis, Marco Fornoni, Shixin Luo, Fan Yang, Weijun Wang, Colby Banbury, Chengxi Ye, Berkin Akin, Vaibhav Aggarwal, Tenghui Zhu, Daniele Moro, Andrew Howard.
1. **[MobileViT](https://huggingface.co/docs/transformers/model_doc/mobilevit)** (from Apple) released with the paper [MobileViT: Light-weight, General-purpose, and Mobile-friendly Vision Transformer](https://arxiv.org/abs/2110.02178) by Sachin Mehta and Mohammad Rastegari.
1. **[MobileViTV2](https://huggingface.co/docs/transformers/model_doc/mobilevitv2)** (from Apple) released with the paper [Separable Self-attention for Mobile Vision Transformers](https://arxiv.org/abs/2206.02680) by Sachin Mehta and Mohammad Rastegari.
1. **[ModernBERT](https://huggingface.co/docs/transformers/model_doc/modernbert)** (from Answer.AI and LightOn) released with the paper [Smarter, Better, Faster, Longer: A Modern Bidirectional Encoder for Fast, Memory Efficient, and Long Context Finetuning and Inference](https://arxiv.org/abs/2412.13663) by Benjamin Warner, Antoine Chaffin, Benjamin Clavié, Orion Weller, Oskar Hallström, Said Taghadouini, Alexis Gallagher, Raja Biswas, Faisal Ladhak, Tom Aarsen, Nathan Cooper, Griffin Adams, Jeremy Howard, Iacopo Poli.
1. **Moondream1** released in the repository [moondream](https://github.com/vikhyat/moondream) by vikhyat.
1. **[Moonshine](https://huggingface.co/docs/transformers/model_doc/moonshine)** (from Useful Sensors) released with the paper [Moonshine: Speech Recognition for Live Transcription and Voice Commands](https://arxiv.org/abs/2410.15608) by Nat Jeffries, Evan King, Manjunath Kudlur, Guy Nicholson, James Wang, Pete Warden.
1. **[MPNet](https://huggingface.co/docs/transformers/model_doc/mpnet)** (from Microsoft Research) released with the paper [MPNet: Masked and Permuted Pre-training for Language Understanding](https://arxiv.org/abs/2004.09297) by Kaitao Song, Xu Tan, Tao Qin, Jianfeng Lu, Tie-Yan Liu.
1. **[MPT](https://huggingface.co/docs/transformers/model_doc/mpt)** (from MosaicML) released with the repository [llm-foundry](https://github.com/mosaicml/llm-foundry/) by the MosaicML NLP Team.
1. **[MT5](https://huggingface.co/docs/transformers/model_doc/mt5)** (from Google AI) released with the paper [mT5: A massively multilingual pre-trained text-to-text transformer](https://arxiv.org/abs/2010.11934) by Linting Xue, Noah Constant, Adam Roberts, Mihir Kale, Rami Al-Rfou, Aditya Siddhant, Aditya Barua, Colin Raffel.
1. **[NLLB](https://huggingface.co/docs/transformers/model_doc/nllb)** (from Meta) released with the paper [No Language Left Behind: Scaling Human-Centered Machine Translation](https://arxiv.org/abs/2207.04672) by the NLLB team.
1. **[Nougat](https://huggingface.co/docs/transformers/model_doc/nougat)** (from Meta AI) released with the paper [Nougat: Neural Optical Understanding for Academic Documents](https://arxiv.org/abs/2308.13418) by Lukas Blecher, Guillem Cucurull, Thomas Scialom, Robert Stojnic.
1. **[OLMo](https://huggingface.co/docs/transformers/master/model_doc/olmo)** (from Ai2) released with the paper [OLMo: Accelerating the Science of Language Models](https://arxiv.org/abs/2402.00838) by Dirk Groeneveld, Iz Beltagy, Pete Walsh, Akshita Bhagia, Rodney Kinney, Oyvind Tafjord, Ananya Harsh Jha, Hamish Ivison, Ian Magnusson, Yizhong Wang, Shane Arora, David Atkinson, Russell Authur, Khyathi Raghavi Chandu, Arman Cohan, Jennifer Dumas, Yanai Elazar, Yuling Gu, Jack Hessel, Tushar Khot, William Merrill, Jacob Morrison, Niklas Muennighoff, Aakanksha Naik, Crystal Nam, Matthew E. Peters, Valentina Pyatkin, Abhilasha Ravichander, Dustin Schwenk, Saurabh Shah, Will Smith, Emma Strubell, Nishant Subramani, Mitchell Wortsman, Pradeep Dasigi, Nathan Lambert, Kyle Richardson, Luke Zettlemoyer, Jesse Dodge, Kyle Lo, Luca Soldaini, Noah A. Smith, Hannaneh Hajishirzi.
1. **[OLMo2](https://huggingface.co/docs/transformers/master/model_doc/olmo2)** (from Ai2) released with the blog [OLMo 2: The best fully open language model to date](https://allenai.org/blog/olmo2) by the Ai2 OLMo team.
1. **OpenELM** (from Apple) released with the paper [OpenELM: An Efficient Language Model Family with Open-source Training and Inference Framework](https://arxiv.org/abs/2404.14619) by Sachin Mehta, Mohammad Hossein Sekhavat, Qingqing Cao, Maxwell Horton, Yanzi Jin, Chenfan Sun, Iman Mirzadeh, Mahyar Najibi, Dmitry Belenko, Peter Zatloukal, Mohammad Rastegari.
1. **[OPT](https://huggingface.co/docs/transformers/master/model_doc/opt)** (from Meta AI) released with the paper [OPT: Open Pre-trained Transformer Language Models](https://arxiv.org/abs/2205.01068) by Susan Zhang, Stephen Roller, Naman Goyal, Mikel Artetxe, Moya Chen, Shuohui Chen et al.
1. **[OWL-ViT](https://huggingface.co/docs/transformers/model_doc/owlvit)** (from Google AI) released with the paper [Simple Open-Vocabulary Object Detection with Vision Transformers](https://arxiv.org/abs/2205.06230) by Matthias Minderer, Alexey Gritsenko, Austin Stone, Maxim Neumann, Dirk Weissenborn, Alexey Dosovitskiy, Aravindh Mahendran, Anurag Arnab, Mostafa Dehghani, Zhuoran Shen, Xiao Wang, Xiaohua Zhai, Thomas Kipf, and Neil Houlsby.
1. **[OWLv2](https://huggingface.co/docs/transformers/model_doc/owlv2)** (from Google AI) released with the paper [Scaling Open-Vocabulary Object Detection](https://arxiv.org/abs/2306.09683) by Matthias Minderer, Alexey Gritsenko, Neil Houlsby.
1. **[PaliGemma](https://huggingface.co/docs/transformers/main/model_doc/paligemma)** (from Google) released with the papers [PaliGemma: A versatile 3B VLM for transfer](https://arxiv.org/abs/2407.07726) and [PaliGemma 2: A Family of Versatile VLMs for Transfer](https://arxiv.org/abs/2412.03555) by the PaliGemma Google team.
1. **[PatchTSMixer](https://huggingface.co/docs/transformers/main/model_doc/patchtsmixer)** (from IBM) released with the paper [TSMixer: Lightweight MLP-Mixer Model for Multivariate Time Series Forecasting](https://arxiv.org/abs/2306.09364) by Vijay Ekambaram, Arindam Jati, Nam Nguyen, Phanwadee Sinthong, Jayant Kalagnanam.
1. **[PatchTST](https://huggingface.co/docs/transformers/main/model_doc/patchtst)** (from Princeton University, IBM) released with the paper [A Time Series is Worth 64 Words: Long-term Forecasting with Transformers](https://arxiv.org/abs/2211.14730) by Yuqi Nie, Nam H. Nguyen, Phanwadee Sinthong, Jayant Kalagnanam.
1. **[Phi](https://huggingface.co/docs/transformers/main/model_doc/phi)** (from Microsoft) released with the papers - [Textbooks Are All You Need](https://arxiv.org/abs/2306.11644) by Suriya Gunasekar, Yi Zhang, Jyoti Aneja, Caio César Teodoro Mendes, Allie Del Giorno, Sivakanth Gopi, Mojan Javaheripi, Piero Kauffmann, Gustavo de Rosa, Olli Saarikivi, Adil Salim, Shital Shah, Harkirat Singh Behl, Xin Wang, Sébastien Bubeck, Ronen Eldan, Adam Tauman Kalai, Yin Tat Lee and Yuanzhi Li, [Textbooks Are All You Need II: phi-1.5 technical report](https://arxiv.org/abs/2309.05463) by Yuanzhi Li, Sébastien Bubeck, Ronen Eldan, Allie Del Giorno, Suriya Gunasekar and Yin Tat Lee.
1. **[Phi3](https://huggingface.co/docs/transformers/main/model_doc/phi3)** (from Microsoft) released with the paper [Phi-3 Technical Report: A Highly Capable Language Model Locally on Your Phone](https://arxiv.org/abs/2404.14219v2) by Marah Abdin, Sam Ade Jacobs, Ammar Ahmad Awan, Jyoti Aneja, Ahmed Awadallah, Hany Awadalla, Nguyen Bach, Amit Bahree, Arash Bakhtiari, Harkirat Behl, Alon Benhaim, Misha Bilenko, Johan Bjorck, Sébastien Bubeck, Martin Cai, Caio César Teodoro Mendes, Weizhu Chen, Vishrav Chaudhary, Parul Chopra, Allie Del Giorno, Gustavo de Rosa, Matthew Dixon, Ronen Eldan, Dan Iter, Amit Garg, Abhishek Goswami, Suriya Gunasekar, Emman Haider, Junheng Hao, Russell J. Hewett, Jamie Huynh, Mojan Javaheripi, Xin Jin, Piero Kauffmann, Nikos Karampatziakis, Dongwoo Kim, Mahoud Khademi, Lev Kurilenko, James R. Lee, Yin Tat Lee, Yuanzhi Li, Chen Liang, Weishung Liu, Eric Lin, Zeqi Lin, Piyush Madan, Arindam Mitra, Hardik Modi, Anh Nguyen, Brandon Norick, Barun Patra, Daniel Perez-Becker, Thomas Portet, Reid Pryzant, Heyang Qin, Marko Radmilac, Corby Rosset, Sambudha Roy, Olatunji Ruwase, Olli Saarikivi, Amin Saied, Adil Salim, Michael Santacroce, Shital Shah, Ning Shang, Hiteshi Sharma, Xia Song, Masahiro Tanaka, Xin Wang, Rachel Ward, Guanhua Wang, Philipp Witte, Michael Wyatt, Can Xu, Jiahang Xu, Sonali Yadav, Fan Yang, Ziyi Yang, Donghan Yu, Chengruidong Zhang, Cyril Zhang, Jianwen Zhang, Li Lyna Zhang, Yi Zhang, Yue Zhang, Yunan Zhang, Xiren Zhou.
1. **Phi3V** (from Microsoft) released with the paper [Phi-3 Technical Report: A Highly Capable Language Model Locally on Your Phone](https://arxiv.org/abs/2404.14219v4) by Marah Abdin, Jyoti Aneja, Hany Awadalla, Ahmed Awadallah, Ammar Ahmad Awan, Nguyen Bach, Amit Bahree, Arash Bakhtiari, Jianmin Bao, Harkirat Behl, Alon Benhaim, Misha Bilenko, Johan Bjorck, Sébastien Bubeck, Martin Cai, Qin Cai, Vishrav Chaudhary, Dong Chen, Dongdong Chen, Weizhu Chen, Yen-Chun Chen, Yi-Ling Chen, Hao Cheng, Parul Chopra, Xiyang Dai, Matthew Dixon, Ronen Eldan, Victor Fragoso, Jianfeng Gao, Mei Gao, Min Gao, Amit Garg, Allie Del Giorno, Abhishek Goswami, Suriya Gunasekar, Emman Haider, Junheng Hao, Russell J. Hewett, Wenxiang Hu, Jamie Huynh, Dan Iter, Sam Ade Jacobs, Mojan Javaheripi, Xin Jin, Nikos Karampatziakis, Piero Kauffmann, Mahoud Khademi, Dongwoo Kim, Young Jin Kim, Lev Kurilenko, James R. Lee, Yin Tat Lee, Yuanzhi Li, Yunsheng Li, Chen Liang, Lars Liden, Xihui Lin, Zeqi Lin, Ce Liu, Liyuan Liu, Mengchen Liu, Weishung Liu, Xiaodong Liu, Chong Luo, Piyush Madan, Ali Mahmoudzadeh, David Majercak, Matt Mazzola, Caio César Teodoro Mendes, Arindam Mitra, Hardik Modi, Anh Nguyen, Brandon Norick, Barun Patra, Daniel Perez-Becker, Thomas Portet, Reid Pryzant, Heyang Qin, Marko Radmilac, Liliang Ren, Gustavo de Rosa, Corby Rosset, Sambudha Roy, Olatunji Ruwase, Olli Saarikivi, Amin Saied, Adil Salim, Michael Santacroce, Shital Shah, Ning Shang, Hiteshi Sharma, Yelong Shen, Swadheen Shukla, Xia Song, Masahiro Tanaka, Andrea Tupini, Praneetha Vaddamanu, Chunyu Wang, Guanhua Wang, Lijuan Wang , Shuohang Wang, Xin Wang, Yu Wang, Rachel Ward, Wen Wen, Philipp Witte, Haiping Wu, Xiaoxia Wu, Michael Wyatt, Bin Xiao, Can Xu, Jiahang Xu, Weijian Xu, Jilong Xue, Sonali Yadav, Fan Yang, Jianwei Yang, Yifan Yang, Ziyi Yang, Donghan Yu, Lu Yuan, Chenruidong Zhang, Cyril Zhang, Jianwen Zhang, Li Lyna Zhang, Yi Zhang, Yue Zhang, Yunan Zhang, Xiren Zhou.
1. **[PVT](https://huggingface.co/docs/transformers/main/model_doc/pvt)** (from Nanjing University, The University of Hong Kong etc.) released with the paper [Pyramid Vision Transformer: A Versatile Backbone for Dense Prediction without Convolutions](https://arxiv.org/pdf/2102.12122.pdf) by Wenhai Wang, Enze Xie, Xiang Li, Deng-Ping Fan, Kaitao Song, Ding Liang, Tong Lu, Ping Luo, Ling Shao.
1. **PyAnnote** released in the repository [pyannote/pyannote-audio](https://github.com/pyannote/pyannote-audio) by Hervé Bredin.
1. **[Qwen2](https://huggingface.co/docs/transformers/model_doc/qwen2)** (from the Qwen team, Alibaba Group) released with the paper [Qwen Technical Report](https://arxiv.org/abs/2309.16609) by Jinze Bai, Shuai Bai, Yunfei Chu, Zeyu Cui, Kai Dang, Xiaodong Deng, Yang Fan, Wenbin Ge, Yu Han, Fei Huang, Binyuan Hui, Luo Ji, Mei Li, Junyang Lin, Runji Lin, Dayiheng Liu, Gao Liu, Chengqiang Lu, Keming Lu, Jianxin Ma, Rui Men, Xingzhang Ren, Xuancheng Ren, Chuanqi Tan, Sinan Tan, Jianhong Tu, Peng Wang, Shijie Wang, Wei Wang, Shengguang Wu, Benfeng Xu, Jin Xu, An Yang, Hao Yang, Jian Yang, Shusheng Yang, Yang Yao, Bowen Yu, Hongyi Yuan, Zheng Yuan, Jianwei Zhang, Xingxuan Zhang, Yichang Zhang, Zhenru Zhang, Chang Zhou, Jingren Zhou, Xiaohuan Zhou and Tianhang Zhu.
1. **[Qwen2-VL](https://huggingface.co/docs/transformers/model_doc/qwen2_vl)** (from the Qwen team, Alibaba Group) released with the paper [Qwen-VL: A Versatile Vision-Language Model for Understanding, Localization, Text Reading, and Beyond](https://arxiv.org/abs/2308.12966) by Jinze Bai, Shuai Bai, Shusheng Yang, Shijie Wang, Sinan Tan, Peng Wang, Junyang Lin, Chang Zhou, Jingren Zhou.
1. **[Qwen3](https://huggingface.co/docs/transformers/en/model_doc/qwen3)** (from the Qwen team, Alibaba Group) released with the blog post [Qwen3: Think Deeper, Act Faster](https://qwenlm.github.io/blog/qwen3/) by the Qwen team.
1. **[ResNet](https://huggingface.co/docs/transformers/model_doc/resnet)** (from Microsoft Research) released with the paper [Deep Residual Learning for Image Recognition](https://arxiv.org/abs/1512.03385) by Kaiming He, Xiangyu Zhang, Shaoqing Ren, Jian Sun.
1. **[RF-DETR](https://huggingface.co/docs/transformers/model_doc/rf_detr)** (from Roboflow) released with the blog post [RF-DETR: A SOTA Real-Time Object Detection Model](https://blog.roboflow.com/rf-detr/) by Peter Robicheaux, James Gallagher, Joseph Nelson, Isaac Robinson.
1. **[RoBERTa](https://huggingface.co/docs/transformers/model_doc/roberta)** (from Facebook), released together with the paper [RoBERTa: A Robustly Optimized BERT Pretraining Approach](https://arxiv.org/abs/1907.11692) by Yinhan Liu, Myle Ott, Naman Goyal, Jingfei Du, Mandar Joshi, Danqi Chen, Omer Levy, Mike Lewis, Luke Zettlemoyer, Veselin Stoyanov.
1. **[RoFormer](https://huggingface.co/docs/transformers/model_doc/roformer)** (from ZhuiyiTechnology), released together with the paper [RoFormer: Enhanced Transformer with Rotary Position Embedding](https://arxiv.org/abs/2104.09864) by Jianlin Su and Yu Lu and Shengfeng Pan and Bo Wen and Yunfeng Liu.
1. **[RT-DETR](https://huggingface.co/docs/transformers/model_doc/rt_detr)** (from Baidu), released together with the paper [DETRs Beat YOLOs on Real-time Object Detection](https://arxiv.org/abs/2304.08069) by Yian Zhao, Wenyu Lv, Shangliang Xu, Jinman Wei, Guanzhong Wang, Qingqing Dang, Yi Liu, Jie Chen.
1. **[RT-DETRv2](https://huggingface.co/docs/transformers/model_doc/rt_detr_v2)** (from Baidu), released together with the paper [RT-DETRv2: Improved Baseline with Bag-of-Freebies for Real-Time Detection Transformer](https://arxiv.org/abs/2407.17140) by Wenyu Lv, Yian Zhao, Qinyao Chang, Kui Huang, Guanzhong Wang, Yi Liu.
1. **Sapiens** (from Meta AI) released with the paper [Sapiens: Foundation for Human Vision Models](https://arxiv.org/pdf/2408.12569) by Rawal Khirodkar, Timur Bagautdinov, Julieta Martinez, Su Zhaoen, Austin James, Peter Selednik, Stuart Anderson, Shunsuke Saito.
1. **[SegFormer](https://huggingface.co/docs/transformers/model_doc/segformer)** (from NVIDIA) released with the paper [SegFormer: Simple and Efficient Design for Semantic Segmentation with Transformers](https://arxiv.org/abs/2105.15203) by Enze Xie, Wenhai Wang, Zhiding Yu, Anima Anandkumar, Jose M. Alvarez, Ping Luo.
1. **[Segment Anything](https://huggingface.co/docs/transformers/model_doc/sam)** (from Meta AI) released with the paper [Segment Anything](https://arxiv.org/pdf/2304.02643v1.pdf) by Alexander Kirillov, Eric Mintun, Nikhila Ravi, Hanzi Mao, Chloe Rolland, Laura Gustafson, Tete Xiao, Spencer Whitehead, Alex Berg, Wan-Yen Lo, Piotr Dollar, Ross Girshick.
1. **[SigLIP](https://huggingface.co/docs/transformers/main/model_doc/siglip)** (from Google AI) released with the paper [Sigmoid Loss for Language Image Pre-Training](https://arxiv.org/abs/2303.15343) by Xiaohua Zhai, Basil Mustafa, Alexander Kolesnikov, Lucas Beyer.
1. **[SmolVLM](https://huggingface.co/docs/transformers/main/model_doc/smolvlm) (from Hugging Face) released with the blog posts [SmolVLM - small yet mighty Vision Language Model](https://huggingface.co/blog/smolvlm) and [SmolVLM Grows Smaller – Introducing the 250M & 500M Models!](https://huggingface.co/blog/smolervlm) by the Hugging Face TB Research team.
1. **SNAC** (from Papla Media, ETH Zurich) released with the paper [SNAC: Multi-Scale Neural Audio Codec](https://arxiv.org/abs/2410.14411) by Hubert Siuzdak, Florian Grötschla, Luca A. Lanzendörfer.
1. **[SpeechT5](https://huggingface.co/docs/transformers/model_doc/speecht5)** (from Microsoft Research) released with the paper [SpeechT5: Unified-Modal Encoder-Decoder Pre-Training for Spoken Language Processing](https://arxiv.org/abs/2110.07205) by Junyi Ao, Rui Wang, Long Zhou, Chengyi Wang, Shuo Ren, Yu Wu, Shujie Liu, Tom Ko, Qing Li, Yu Zhang, Zhihua Wei, Yao Qian, Jinyu Li, Furu Wei.
1. **[SqueezeBERT](https://huggingface.co/docs/transformers/model_doc/squeezebert)** (from Berkeley) released with the paper [SqueezeBERT: What can computer vision teach NLP about efficient neural networks?](https://arxiv.org/abs/2006.11316) by Forrest N. Iandola, Albert E. Shaw, Ravi Krishna, and Kurt W. Keutzer.
1. **[StableLm](https://huggingface.co/docs/transformers/model_doc/stablelm)** (from Stability AI) released with the paper [StableLM 3B 4E1T (Technical Report)](https://stability.wandb.io/stability-llm/stable-lm/reports/StableLM-3B-4E1T--VmlldzoyMjU4?accessToken=u3zujipenkx5g7rtcj9qojjgxpconyjktjkli2po09nffrffdhhchq045vp0wyfo) by Jonathan Tow, Marco Bellagente, Dakota Mahan, Carlos Riquelme Ruiz, Duy Phung, Maksym Zhuravinskyi, Nathan Cooper, Nikhil Pinnaparaju, Reshinth Adithyan, and James Baicoianu.
1. **[Starcoder2](https://huggingface.co/docs/transformers/main/model_doc/starcoder2)** (from BigCode team) released with the paper [StarCoder 2 and The Stack v2: The Next Generation](https://arxiv.org/abs/2402.19173) by Anton Lozhkov, Raymond Li, Loubna Ben Allal, Federico Cassano, Joel Lamy-Poirier, Nouamane Tazi, Ao Tang, Dmytro Pykhtar, Jiawei Liu, Yuxiang Wei, Tianyang Liu, Max Tian, Denis Kocetkov, Arthur Zucker, Younes Belkada, Zijian Wang, Qian Liu, Dmitry Abulkhanov, Indraneil Paul, Zhuang Li, Wen-Ding Li, Megan Risdal, Jia Li, Jian Zhu, Terry Yue Zhuo, Evgenii Zheltonozhskii, Nii Osae Osae Dade, Wenhao Yu, Lucas Krauß, Naman Jain, Yixuan Su, Xuanli He, Manan Dey, Edoardo Abati, Yekun Chai, Niklas Muennighoff, Xiangru Tang, Muhtasham Oblokulov, Christopher Akiki, Marc Marone, Chenghao Mou, Mayank Mishra, Alex Gu, Binyuan Hui, Tri Dao, Armel Zebaze, Olivier Dehaene, Nicolas Patry, Canwen Xu, Julian McAuley, Han Hu, Torsten Scholak, Sebastien Paquet, Jennifer Robinson, Carolyn Jane Anderson, Nicolas Chapados, Mostofa Patwary, Nima Tajbakhsh, Yacine Jernite, Carlos Muñoz Ferrandis, Lingming Zhang, Sean Hughes, Thomas Wolf, Arjun Guha, Leandro von Werra, and Harm de Vries.
1. StyleTTS 2 (from Columbia University) released with the paper [StyleTTS 2: Towards Human-Level Text-to-Speech through Style Diffusion and Adversarial Training with Large Speech Language Models](https://arxiv.org/abs/2306.07691) by Yinghao Aaron Li, Cong Han, Vinay S. Raghavan, Gavin Mischler, Nima Mesgarani.
1. **[Swin Transformer](https://huggingface.co/docs/transformers/model_doc/swin)** (from Microsoft) released with the paper [Swin Transformer: Hierarchical Vision Transformer using Shifted Windows](https://arxiv.org/abs/2103.14030) by Ze Liu, Yutong Lin, Yue Cao, Han Hu, Yixuan Wei, Zheng Zhang, Stephen Lin, Baining Guo.
1. **[Swin2SR](https://huggingface.co/docs/transformers/model_doc/swin2sr)** (from University of Würzburg) released with the paper [Swin2SR: SwinV2 Transformer for Compressed Image Super-Resolution and Restoration](https://arxiv.org/abs/2209.11345) by Marcos V. Conde, Ui-Jin Choi, Maxime Burchi, Radu Timofte.
1. **[T5](https://huggingface.co/docs/transformers/model_doc/t5)** (from Google AI) released with the paper [Exploring the Limits of Transfer Learning with a Unified Text-to-Text Transformer](https://arxiv.org/abs/1910.10683) by Colin Raffel and Noam Shazeer and Adam Roberts and Katherine Lee and Sharan Narang and Michael Matena and Yanqi Zhou and Wei Li and Peter J. Liu.
1. **[T5v1.1](https://huggingface.co/docs/transformers/model_doc/t5v1.1)** (from Google AI) released in the repository [google-research/text-to-text-transfer-transformer](https://github.com/google-research/text-to-text-transfer-transformer/blob/main/released_checkpoints.md#t511) by Colin Raffel and Noam Shazeer and Adam Roberts and Katherine Lee and Sharan Narang and Michael Matena and Yanqi Zhou and Wei Li and Peter J. Liu.
1. **[Table Transformer](https://huggingface.co/docs/transformers/model_doc/table-transformer)** (from Microsoft Research) released with the paper [PubTables-1M: Towards Comprehensive Table Extraction From Unstructured Documents](https://arxiv.org/abs/2110.00061) by Brandon Smock, Rohith Pesala, Robin Abraham.
1. **[TrOCR](https://huggingface.co/docs/transformers/model_doc/trocr)** (from Microsoft), released together with the paper [TrOCR: Transformer-based Optical Character Recognition with Pre-trained Models](https://arxiv.org/abs/2109.10282) by Minghao Li, Tengchao Lv, Lei Cui, Yijuan Lu, Dinei Florencio, Cha Zhang, Zhoujun Li, Furu Wei.
1. **Ultravox** (from Fixie.ai) released with the repository [fixie-ai/ultravox](https://github.com/fixie-ai/ultravox) by the Fixie.ai team.
1. **[UniSpeech](https://huggingface.co/docs/transformers/model_doc/unispeech)** (from Microsoft Research) released with the paper [UniSpeech: Unified Speech Representation Learning with Labeled and Unlabeled Data](https://arxiv.org/abs/2101.07597) by Chengyi Wang, Yu Wu, Yao Qian, Kenichi Kumatani, Shujie Liu, Furu Wei, Michael Zeng, Xuedong Huang.
1. **[UniSpeechSat](https://huggingface.co/docs/transformers/model_doc/unispeech-sat)** (from Microsoft Research) released with the paper [UNISPEECH-SAT: UNIVERSAL SPEECH REPRESENTATION LEARNING WITH SPEAKER AWARE PRE-TRAINING](https://arxiv.org/abs/2110.05752) by Sanyuan Chen, Yu Wu, Chengyi Wang, Zhengyang Chen, Zhuo Chen, Shujie Liu, Jian Wu, Yao Qian, Furu Wei, Jinyu Li, Xiangzhan Yu.
1. **[Vision Transformer (ViT)](https://huggingface.co/docs/transformers/model_doc/vit)** (from Google AI) released with the paper [An Image is Worth 16x16 Words: Transformers for Image Recognition at Scale](https://arxiv.org/abs/2010.11929) by Alexey Dosovitskiy, Lucas Beyer, Alexander Kolesnikov, Dirk Weissenborn, Xiaohua Zhai, Thomas Unterthiner, Mostafa Dehghani, Matthias Minderer, Georg Heigold, Sylvain Gelly, Jakob Uszkoreit, Neil Houlsby.
1. **[ViTMAE](https://huggingface.co/docs/transformers/model_doc/vit_mae)** (from Meta AI) released with the paper [Masked Autoencoders Are Scalable Vision Learners](https://arxiv.org/abs/2111.06377) by Kaiming He, Xinlei Chen, Saining Xie, Yanghao Li, Piotr Dollár, Ross Girshick.
1. **[ViTMatte](https://huggingface.co/docs/transformers/model_doc/vitmatte)** (from HUST-VL) released with the paper [ViTMatte: Boosting Image Matting with Pretrained Plain Vision Transformers](https://arxiv.org/abs/2305.15272) by Jingfeng Yao, Xinggang Wang, Shusheng Yang, Baoyuan Wang.
1. **[ViTMSN](https://huggingface.co/docs/transformers/model_doc/vit_msn)** (from Meta AI) released with the paper [Masked Siamese Networks for Label-Efficient Learning](https://arxiv.org/abs/2204.07141) by Mahmoud Assran, Mathilde Caron, Ishan Misra, Piotr Bojanowski, Florian Bordes, Pascal Vincent, Armand Joulin, Michael Rabbat, Nicolas Ballas.
1. **[ViTPose](https://huggingface.co/docs/transformers/model_doc/vitpose)** (from The University of Sydney) released with the paper [ViTPose: Simple Vision Transformer Baselines for Human Pose Estimation](https://arxiv.org/abs/2204.12484) by Yufei Xu, Jing Zhang, Qiming Zhang, Dacheng Tao.
1. **[VITS](https://huggingface.co/docs/transformers/model_doc/vits)** (from Kakao Enterprise) released with the paper [Conditional Variational Autoencoder with Adversarial Learning for End-to-End Text-to-Speech](https://arxiv.org/abs/2106.06103) by Jaehyeon Kim, Jungil Kong, Juhee Son.
1. **[Wav2Vec2](https://huggingface.co/docs/transformers/model_doc/wav2vec2)** (from Facebook AI) released with the paper [wav2vec 2.0: A Framework for Self-Supervised Learning of Speech Representations](https://arxiv.org/abs/2006.11477) by Alexei Baevski, Henry Zhou, Abdelrahman Mohamed, Michael Auli.
1. **[Wav2Vec2-BERT](https://huggingface.co/docs/transformers/main/model_doc/wav2vec2-bert)** (from Meta AI) released with the paper [Seamless: Multilingual Expressive and Streaming Speech Translation](https://ai.meta.com/research/publications/seamless-multilingual-expressive-and-streaming-speech-translation/) by the Seamless Communication team.
1. **[WavLM](https://huggingface.co/docs/transformers/model_doc/wavlm)** (from Microsoft Research) released with the paper [WavLM: Large-Scale Self-Supervised Pre-Training for Full Stack Speech Processing](https://arxiv.org/abs/2110.13900) by Sanyuan Chen, Chengyi Wang, Zhengyang Chen, Yu Wu, Shujie Liu, Zhuo Chen, Jinyu Li, Naoyuki Kanda, Takuya Yoshioka, Xiong Xiao, Jian Wu, Long Zhou, Shuo Ren, Yanmin Qian, Yao Qian, Jian Wu, Michael Zeng, Furu Wei.
1. **[Whisper](https://huggingface.co/docs/transformers/model_doc/whisper)** (from OpenAI) released with the paper [Robust Speech Recognition via Large-Scale Weak Supervision](https://cdn.openai.com/papers/whisper.pdf) by Alec Radford, Jong Wook Kim, Tao Xu, Greg Brockman, Christine McLeavey, Ilya Sutskever.
1. **[XLM](https://huggingface.co/docs/transformers/model_doc/xlm)** (from Facebook) released together with the paper [Cross-lingual Language Model Pretraining](https://arxiv.org/abs/1901.07291) by Guillaume Lample and Alexis Conneau.
1. **[XLM-RoBERTa](https://huggingface.co/docs/transformers/model_doc/xlm-roberta)** (from Facebook AI), released together with the paper [Unsupervised Cross-lingual Representation Learning at Scale](https://arxiv.org/abs/1911.02116) by Alexis Conneau*, Kartikay Khandelwal*, Naman Goyal, Vishrav Chaudhary, Guillaume Wenzek, Francisco Guzmán, Edouard Grave, Myle Ott, Luke Zettlemoyer and Veselin Stoyanov.
1. **[YOLOS](https://huggingface.co/docs/transformers/model_doc/yolos)** (from Huazhong University of Science & Technology) released with the paper [You Only Look at One Sequence: Rethinking Transformer in Vision through Object Detection](https://arxiv.org/abs/2106.00666) by Yuxin Fang, Bencheng Liao, Xinggang Wang, Jiemin Fang, Jiyang Qi, Rui Wu, Jianwei Niu, Wenyu Liu.

