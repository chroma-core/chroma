declare const PreTrainedModel_base: new () => {
    (...args: any[]): any;
    _call(...args: any[]): any;
};
/**
 * A base class for pre-trained models that provides the model configuration and an ONNX session.
 */
export class PreTrainedModel extends PreTrainedModel_base {
    /**
     * Instantiate one of the model classes of the library from a pretrained model.
     *
     * The model class to instantiate is selected based on the `model_type` property of the config object
     * (either passed as an argument or loaded from `pretrained_model_name_or_path` if possible)
     *
     * @param {string} pretrained_model_name_or_path The name or path of the pretrained model. Can be either:
     * - A string, the *model id* of a pretrained model hosted inside a model repo on huggingface.co.
     *   Valid model ids can be located at the root-level, like `bert-base-uncased`, or namespaced under a
     *   user or organization name, like `dbmdz/bert-base-german-cased`.
     * - A path to a *directory* containing model weights, e.g., `./my_model_directory/`.
     * @param {import('./utils/hub.js').PretrainedModelOptions} options Additional options for loading the model.
     *
     * @returns {Promise<PreTrainedModel>} A new instance of the `PreTrainedModel` class.
     */
    static from_pretrained(pretrained_model_name_or_path: string, { progress_callback, config, cache_dir, local_files_only, revision, model_file_name, subfolder, device, dtype, use_external_data_format, session_options, }?: import("./utils/hub.js").PretrainedModelOptions): Promise<PreTrainedModel>;
    /**
     * Creates a new instance of the `PreTrainedModel` class.
     * @param {import('./configs.js').PretrainedConfig} config The model configuration.
     * @param {Record<string, any>} sessions The inference sessions for the model.
     * @param {Record<string, Object>} configs Additional configuration files (e.g., generation_config.json).
     */
    constructor(config: import("./configs.js").PretrainedConfig, sessions: Record<string, any>, configs: Record<string, any>);
    main_input_name: string;
    forward_params: string[];
    config: import("./configs.js").PretrainedConfig;
    sessions: Record<string, any>;
    configs: Record<string, any>;
    can_generate: boolean;
    _forward: typeof decoderForward | typeof autoEncoderForward;
    _prepare_inputs_for_generation: typeof multimodal_text_to_text_prepare_inputs_for_generation;
    /** @type {import('./configs.js').TransformersJSConfig} */
    custom_config: import("./configs.js").TransformersJSConfig;
    /**
    * Disposes of all the ONNX sessions that were created during inference.
    * @returns {Promise<unknown[]>} An array of promises, one for each ONNX session that is being disposed.
    * @todo Use https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/FinalizationRegistry
    */
    dispose(): Promise<unknown[]>;
    /**
     * Runs the model with the provided inputs
     * @param {Object} model_inputs Object containing input tensors
     * @returns {Promise<Object>} Object containing output tensors
     */
    _call(model_inputs: any): Promise<any>;
    /**
     * Forward method for a pretrained model. If not overridden by a subclass, the correct forward method
     * will be chosen based on the model type.
     * @param {Object} model_inputs The input data to the model in the format specified in the ONNX model.
     * @returns {Promise<Object>} The output data from the model in the format specified in the ONNX model.
     * @throws {Error} This method must be implemented in subclasses.
     */
    forward(model_inputs: any): Promise<any>;
    /**
     * Get the model's generation config, if it exists.
     * @returns {GenerationConfig|null} The model's generation config if it exists, otherwise `null`.
     */
    get generation_config(): GenerationConfig | null;
    /**
     * This function returns a [`LogitsProcessorList`] list object that contains all relevant [`LogitsWarper`]
     * instances used for multinomial sampling.
     * @param {GenerationConfig} generation_config The generation config.
     * @returns {LogitsProcessorList} generation_config
     */
    _get_logits_warper(generation_config: GenerationConfig): LogitsProcessorList;
    /**
     * @param {GenerationConfig} generation_config
     * @param {number} input_ids_seq_length The starting sequence length for the input ids.
     * @returns {LogitsProcessorList}
     * @private
     */
    private _get_logits_processor;
    /**
     * This function merges multiple generation configs together to form a final generation config to be used by the model for text generation.
     * It first creates an empty `GenerationConfig` object, then it applies the model's own `generation_config` property to it. Finally, if a `generation_config` object was passed in the arguments, it overwrites the corresponding properties in the final config with those of the passed config object.
     * @param {GenerationConfig|null} generation_config A `GenerationConfig` object containing generation parameters.
     * @param {Object} kwargs Additional generation parameters to be used in place of those in the `generation_config` object.
     * @returns {GenerationConfig} The final generation config object to be used by the model for text generation.
     */
    _prepare_generation_config(generation_config: GenerationConfig | null, kwargs: any, cls?: typeof GenerationConfig): GenerationConfig;
    /**
     *
     * @param {GenerationConfig} generation_config
     * @param {StoppingCriteriaList} [stopping_criteria=null]
     */
    _get_stopping_criteria(generation_config: GenerationConfig, stopping_criteria?: StoppingCriteriaList): StoppingCriteriaList;
    /**
     * Confirms that the model class is compatible with generation.
     * If not, raises an exception that points to the right class to use.
     */
    _validate_model_class(): void;
    prepare_inputs_for_generation(...args: any[]): any;
    /**
     *
     * @param {Object} inputs
     * @param {bigint[][]} inputs.generated_input_ids
     * @param {Object} inputs.outputs
     * @param {Object} inputs.model_inputs
     * @param {boolean} inputs.is_encoder_decoder
     * @returns {Object} The updated model inputs for the next generation iteration.
     */
    _update_model_kwargs_for_generation({ generated_input_ids, outputs, model_inputs, is_encoder_decoder }: {
        generated_input_ids: bigint[][];
        outputs: any;
        model_inputs: any;
        is_encoder_decoder: boolean;
    }): any;
    /**
     * This function extracts the model-specific `inputs` for generation.
     * @param {Object} params
     * @param {Tensor} [params.inputs=null]
     * @param {number} [params.bos_token_id=null]
     * @param {Record<string, Tensor|number[]>} [params.model_kwargs]
     * @returns {{inputs_tensor: Tensor, model_inputs: Record<string, Tensor>, model_input_name: string}} The model-specific inputs for generation.
     */
    _prepare_model_inputs({ inputs, bos_token_id, model_kwargs }: {
        inputs?: Tensor;
        bos_token_id?: number;
        model_kwargs?: Record<string, Tensor | number[]>;
    }): {
        inputs_tensor: Tensor;
        model_inputs: Record<string, Tensor>;
        model_input_name: string;
    };
    _prepare_encoder_decoder_kwargs_for_generation({ inputs_tensor, model_inputs, model_input_name, generation_config }: {
        inputs_tensor: any;
        model_inputs: any;
        model_input_name: any;
        generation_config: any;
    }): Promise<any>;
    /**
     * Prepares `decoder_input_ids` for generation with encoder-decoder models
     * @param {*} param0
     */
    _prepare_decoder_input_ids_for_generation({ batch_size, model_input_name, model_kwargs, decoder_start_token_id, bos_token_id, generation_config }: any): {
        input_ids: any;
        model_inputs: any;
    };
    /**
     * Generates sequences of token ids for models with a language modeling head.
     * @param {import('./generation/parameters.js').GenerationFunctionParameters} options
     * @returns {Promise<ModelOutput|Tensor>} The output of the model, which can contain the generated token ids, attentions, and scores.
     */
    generate({ inputs, generation_config, logits_processor, stopping_criteria, streamer, ...kwargs }: import("./generation/parameters.js").GenerationFunctionParameters): Promise<ModelOutput | Tensor>;
    /**
     * Returns an object containing past key values from the given decoder results object.
     *
     * @param {Object} decoderResults The decoder results object.
     * @param {Object} pastKeyValues The previous past key values.
     * @returns {Object} An object containing past key values.
     */
    getPastKeyValues(decoderResults: any, pastKeyValues: any, disposeEncoderPKVs?: boolean): any;
    /**
     * Returns an object containing attentions from the given model output object.
     *
     * @param {Object} model_output The output of the model.
     * @returns {{cross_attentions?: Tensor[]}} An object containing attentions.
     */
    getAttentions(model_output: any): {
        cross_attentions?: Tensor[];
    };
    /**
     * Adds past key values to the decoder feeds object. If pastKeyValues is null, creates new tensors for past key values.
     *
     * @param {Object} decoderFeeds The decoder feeds object to add past key values to.
     * @param {Object} pastKeyValues An object containing past key values.
     */
    addPastKeyValues(decoderFeeds: any, pastKeyValues: any): void;
    encode_image({ pixel_values }: {
        pixel_values: any;
    }): Promise<any>;
    encode_text({ input_ids }: {
        input_ids: any;
    }): Promise<any>;
    encode_audio({ audio_values }: {
        audio_values: any;
    }): Promise<any>;
}
export class ModelOutput {
}
/**
 * Base class for model's outputs, with potential hidden states and attentions.
 */
export class BaseModelOutput extends ModelOutput {
    /**
     * @param {Object} output The output of the model.
     * @param {Tensor} output.last_hidden_state Sequence of hidden-states at the output of the last layer of the model.
     * @param {Tensor} [output.hidden_states] Hidden-states of the model at the output of each layer plus the optional initial embedding outputs.
     * @param {Tensor} [output.attentions] Attentions weights after the attention softmax, used to compute the weighted average in the self-attention heads.
     */
    constructor({ last_hidden_state, hidden_states, attentions }: {
        last_hidden_state: Tensor;
        hidden_states?: Tensor;
        attentions?: Tensor;
    });
    last_hidden_state: Tensor;
    hidden_states: Tensor;
    attentions: Tensor;
}
export class BertPreTrainedModel extends PreTrainedModel {
}
export class BertModel extends BertPreTrainedModel {
}
/**
 * BertForMaskedLM is a class representing a BERT model for masked language modeling.
 */
export class BertForMaskedLM extends BertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<MaskedLMOutput>} An object containing the model's output logits for masked language modeling.
     */
    _call(model_inputs: any): Promise<MaskedLMOutput>;
}
/**
 * BertForSequenceClassification is a class representing a BERT model for sequence classification.
 */
export class BertForSequenceClassification extends BertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<SequenceClassifierOutput>} An object containing the model's output logits for sequence classification.
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
/**
 * BertForTokenClassification is a class representing a BERT model for token classification.
 */
export class BertForTokenClassification extends BertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<TokenClassifierOutput>} An object containing the model's output logits for token classification.
     */
    _call(model_inputs: any): Promise<TokenClassifierOutput>;
}
/**
 * BertForQuestionAnswering is a class representing a BERT model for question answering.
 */
export class BertForQuestionAnswering extends BertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<QuestionAnsweringModelOutput>} An object containing the model's output logits for question answering.
     */
    _call(model_inputs: any): Promise<QuestionAnsweringModelOutput>;
}
export class ModernBertPreTrainedModel extends PreTrainedModel {
}
export class ModernBertModel extends ModernBertPreTrainedModel {
}
export class ModernBertForMaskedLM extends ModernBertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<MaskedLMOutput>} An object containing the model's output logits for masked language modeling.
     */
    _call(model_inputs: any): Promise<MaskedLMOutput>;
}
export class ModernBertForSequenceClassification extends ModernBertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<SequenceClassifierOutput>} An object containing the model's output logits for sequence classification.
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
export class ModernBertForTokenClassification extends ModernBertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<TokenClassifierOutput>} An object containing the model's output logits for token classification.
     */
    _call(model_inputs: any): Promise<TokenClassifierOutput>;
}
export class NomicBertPreTrainedModel extends PreTrainedModel {
}
export class NomicBertModel extends NomicBertPreTrainedModel {
}
export class RoFormerPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare RoFormer Model transformer outputting raw hidden-states without any specific head on top.
 */
export class RoFormerModel extends RoFormerPreTrainedModel {
}
/**
 * RoFormer Model with a `language modeling` head on top.
 */
export class RoFormerForMaskedLM extends RoFormerPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<MaskedLMOutput>} An object containing the model's output logits for masked language modeling.
     */
    _call(model_inputs: any): Promise<MaskedLMOutput>;
}
/**
 * RoFormer Model transformer with a sequence classification/regression head on top (a linear layer on top of the pooled output)
 */
export class RoFormerForSequenceClassification extends RoFormerPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<SequenceClassifierOutput>} An object containing the model's output logits for sequence classification.
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
/**
 * RoFormer Model with a token classification head on top (a linear layer on top of the hidden-states output)
 * e.g. for Named-Entity-Recognition (NER) tasks.
 */
export class RoFormerForTokenClassification extends RoFormerPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<TokenClassifierOutput>} An object containing the model's output logits for token classification.
     */
    _call(model_inputs: any): Promise<TokenClassifierOutput>;
}
/**
 * RoFormer Model with a span classification head on top for extractive question-answering tasks like SQuAD
 * (a linear layers on top of the hidden-states output to compute `span start logits` and `span end logits`).
 */
export class RoFormerForQuestionAnswering extends RoFormerPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<QuestionAnsweringModelOutput>} An object containing the model's output logits for question answering.
     */
    _call(model_inputs: any): Promise<QuestionAnsweringModelOutput>;
}
export class ConvBertPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare ConvBERT Model transformer outputting raw hidden-states without any specific head on top.
 */
export class ConvBertModel extends ConvBertPreTrainedModel {
}
/**
 * ConvBERT Model with a language modeling head on top.
 */
export class ConvBertForMaskedLM extends ConvBertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<MaskedLMOutput>} An object containing the model's output logits for masked language modeling.
     */
    _call(model_inputs: any): Promise<MaskedLMOutput>;
}
/**
 * ConvBERT Model transformer with a sequence classification/regression head on top (a linear layer on top of the pooled output)
 */
export class ConvBertForSequenceClassification extends ConvBertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<SequenceClassifierOutput>} An object containing the model's output logits for sequence classification.
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
/**
 * ConvBERT Model with a token classification head on top (a linear layer on top of the hidden-states output)
 * e.g. for Named-Entity-Recognition (NER) tasks.
 */
export class ConvBertForTokenClassification extends ConvBertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<TokenClassifierOutput>} An object containing the model's output logits for token classification.
     */
    _call(model_inputs: any): Promise<TokenClassifierOutput>;
}
/**
 * ConvBERT Model with a span classification head on top for extractive question-answering tasks like SQuAD
 * (a linear layers on top of the hidden-states output to compute `span start logits` and `span end logits`)
 */
export class ConvBertForQuestionAnswering extends ConvBertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<QuestionAnsweringModelOutput>} An object containing the model's output logits for question answering.
     */
    _call(model_inputs: any): Promise<QuestionAnsweringModelOutput>;
}
export class ElectraPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare Electra Model transformer outputting raw hidden-states without any specific head on top.
 * Identical to the BERT model except that it uses an additional linear layer between the embedding
 * layer and the encoder if the hidden size and embedding size are different.
 */
export class ElectraModel extends ElectraPreTrainedModel {
}
/**
 * Electra model with a language modeling head on top.
 */
export class ElectraForMaskedLM extends ElectraPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<MaskedLMOutput>} An object containing the model's output logits for masked language modeling.
     */
    _call(model_inputs: any): Promise<MaskedLMOutput>;
}
/**
 * ELECTRA Model transformer with a sequence classification/regression head on top (a linear layer on top of the pooled output)
 */
export class ElectraForSequenceClassification extends ElectraPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<SequenceClassifierOutput>} An object containing the model's output logits for sequence classification.
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
/**
 * Electra model with a token classification head on top.
 */
export class ElectraForTokenClassification extends ElectraPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<TokenClassifierOutput>} An object containing the model's output logits for token classification.
     */
    _call(model_inputs: any): Promise<TokenClassifierOutput>;
}
/**
 * LECTRA Model with a span classification head on top for extractive question-answering tasks like SQuAD
 * (a linear layers on top of the hidden-states output to compute `span start logits` and `span end logits`).
 */
export class ElectraForQuestionAnswering extends ElectraPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<QuestionAnsweringModelOutput>} An object containing the model's output logits for question answering.
     */
    _call(model_inputs: any): Promise<QuestionAnsweringModelOutput>;
}
export class CamembertPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare CamemBERT Model transformer outputting raw hidden-states without any specific head on top.
 */
export class CamembertModel extends CamembertPreTrainedModel {
}
/**
 * CamemBERT Model with a `language modeling` head on top.
 */
export class CamembertForMaskedLM extends CamembertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<MaskedLMOutput>} An object containing the model's output logits for masked language modeling.
     */
    _call(model_inputs: any): Promise<MaskedLMOutput>;
}
/**
 * CamemBERT Model transformer with a sequence classification/regression head on top (a linear layer on top of the pooled output) e.g. for GLUE tasks.
 */
export class CamembertForSequenceClassification extends CamembertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<SequenceClassifierOutput>} An object containing the model's output logits for sequence classification.
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
/**
 * CamemBERT Model with a token classification head on top (a linear layer on top of the hidden-states output) e.g. for Named-Entity-Recognition (NER) tasks.
 */
export class CamembertForTokenClassification extends CamembertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<TokenClassifierOutput>} An object containing the model's output logits for token classification.
     */
    _call(model_inputs: any): Promise<TokenClassifierOutput>;
}
/**
 * CamemBERT Model with a span classification head on top for extractive question-answering tasks
 */
export class CamembertForQuestionAnswering extends CamembertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<QuestionAnsweringModelOutput>} An object containing the model's output logits for question answering.
     */
    _call(model_inputs: any): Promise<QuestionAnsweringModelOutput>;
}
export class DebertaPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare DeBERTa Model transformer outputting raw hidden-states without any specific head on top.
 */
export class DebertaModel extends DebertaPreTrainedModel {
}
/**
 * DeBERTa Model with a `language modeling` head on top.
 */
export class DebertaForMaskedLM extends DebertaPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<MaskedLMOutput>} An object containing the model's output logits for masked language modeling.
     */
    _call(model_inputs: any): Promise<MaskedLMOutput>;
}
/**
 * DeBERTa Model transformer with a sequence classification/regression head on top (a linear layer on top of the pooled output)
 */
export class DebertaForSequenceClassification extends DebertaPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<SequenceClassifierOutput>} An object containing the model's output logits for sequence classification.
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
/**
 * DeBERTa Model with a token classification head on top (a linear layer on top of the hidden-states output) e.g. for Named-Entity-Recognition (NER) tasks.
 */
export class DebertaForTokenClassification extends DebertaPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<TokenClassifierOutput>} An object containing the model's output logits for token classification.
     */
    _call(model_inputs: any): Promise<TokenClassifierOutput>;
}
/**
 * DeBERTa Model with a span classification head on top for extractive question-answering tasks like SQuAD (a linear
 * layers on top of the hidden-states output to compute `span start logits` and `span end logits`).
 */
export class DebertaForQuestionAnswering extends DebertaPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<QuestionAnsweringModelOutput>} An object containing the model's output logits for question answering.
     */
    _call(model_inputs: any): Promise<QuestionAnsweringModelOutput>;
}
export class DebertaV2PreTrainedModel extends PreTrainedModel {
}
/**
 * The bare DeBERTa-V2 Model transformer outputting raw hidden-states without any specific head on top.
 */
export class DebertaV2Model extends DebertaV2PreTrainedModel {
}
/**
 * DeBERTa-V2 Model with a `language modeling` head on top.
 */
export class DebertaV2ForMaskedLM extends DebertaV2PreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<MaskedLMOutput>} An object containing the model's output logits for masked language modeling.
     */
    _call(model_inputs: any): Promise<MaskedLMOutput>;
}
/**
 * DeBERTa-V2 Model transformer with a sequence classification/regression head on top (a linear layer on top of the pooled output)
 */
export class DebertaV2ForSequenceClassification extends DebertaV2PreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<SequenceClassifierOutput>} An object containing the model's output logits for sequence classification.
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
/**
 * DeBERTa-V2 Model with a token classification head on top (a linear layer on top of the hidden-states output) e.g. for Named-Entity-Recognition (NER) tasks.
 */
export class DebertaV2ForTokenClassification extends DebertaV2PreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<TokenClassifierOutput>} An object containing the model's output logits for token classification.
     */
    _call(model_inputs: any): Promise<TokenClassifierOutput>;
}
/**
 * DeBERTa-V2 Model with a span classification head on top for extractive question-answering tasks like SQuAD (a linear
 * layers on top of the hidden-states output to compute `span start logits` and `span end logits`).
 */
export class DebertaV2ForQuestionAnswering extends DebertaV2PreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<QuestionAnsweringModelOutput>} An object containing the model's output logits for question answering.
     */
    _call(model_inputs: any): Promise<QuestionAnsweringModelOutput>;
}
export class DistilBertPreTrainedModel extends PreTrainedModel {
}
export class DistilBertModel extends DistilBertPreTrainedModel {
}
/**
 * DistilBertForSequenceClassification is a class representing a DistilBERT model for sequence classification.
 */
export class DistilBertForSequenceClassification extends DistilBertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<SequenceClassifierOutput>} An object containing the model's output logits for sequence classification.
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
/**
 * DistilBertForTokenClassification is a class representing a DistilBERT model for token classification.
 */
export class DistilBertForTokenClassification extends DistilBertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<TokenClassifierOutput>} An object containing the model's output logits for token classification.
     */
    _call(model_inputs: any): Promise<TokenClassifierOutput>;
}
/**
 * DistilBertForQuestionAnswering is a class representing a DistilBERT model for question answering.
 */
export class DistilBertForQuestionAnswering extends DistilBertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<QuestionAnsweringModelOutput>} An object containing the model's output logits for question answering.
     */
    _call(model_inputs: any): Promise<QuestionAnsweringModelOutput>;
}
/**
 * DistilBertForMaskedLM is a class representing a DistilBERT model for masking task.
 */
export class DistilBertForMaskedLM extends DistilBertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<MaskedLMOutput>} returned object
     */
    _call(model_inputs: any): Promise<MaskedLMOutput>;
}
export class EsmPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare ESM Model transformer outputting raw hidden-states without any specific head on top.
 */
export class EsmModel extends EsmPreTrainedModel {
}
/**
 * ESM Model with a `language modeling` head on top.
 */
export class EsmForMaskedLM extends EsmPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<MaskedLMOutput>} An object containing the model's output logits for masked language modeling.
     */
    _call(model_inputs: any): Promise<MaskedLMOutput>;
}
/**
 * ESM Model transformer with a sequence classification/regression head on top (a linear layer on top of the pooled output)
 */
export class EsmForSequenceClassification extends EsmPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<SequenceClassifierOutput>} An object containing the model's output logits for sequence classification.
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
/**
 * ESM Model with a token classification head on top (a linear layer on top of the hidden-states output)
 * e.g. for Named-Entity-Recognition (NER) tasks.
 */
export class EsmForTokenClassification extends EsmPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<TokenClassifierOutput>} An object containing the model's output logits for token classification.
     */
    _call(model_inputs: any): Promise<TokenClassifierOutput>;
}
export class MobileBertPreTrainedModel extends PreTrainedModel {
}
export class MobileBertModel extends MobileBertPreTrainedModel {
}
/**
 * MobileBertForMaskedLM is a class representing a MobileBERT model for masking task.
 */
export class MobileBertForMaskedLM extends MobileBertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<MaskedLMOutput>} returned object
     */
    _call(model_inputs: any): Promise<MaskedLMOutput>;
}
/**
 * MobileBert Model transformer with a sequence classification/regression head on top (a linear layer on top of the pooled output)
 */
export class MobileBertForSequenceClassification extends MobileBertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<SequenceClassifierOutput>} returned object
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
/**
 * MobileBert Model with a span classification head on top for extractive question-answering tasks
 */
export class MobileBertForQuestionAnswering extends MobileBertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<QuestionAnsweringModelOutput>} returned object
     */
    _call(model_inputs: any): Promise<QuestionAnsweringModelOutput>;
}
export class MPNetPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare MPNet Model transformer outputting raw hidden-states without any specific head on top.
 */
export class MPNetModel extends MPNetPreTrainedModel {
}
/**
 * MPNetForMaskedLM is a class representing a MPNet model for masked language modeling.
 */
export class MPNetForMaskedLM extends MPNetPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<MaskedLMOutput>} An object containing the model's output logits for masked language modeling.
     */
    _call(model_inputs: any): Promise<MaskedLMOutput>;
}
/**
 * MPNetForSequenceClassification is a class representing a MPNet model for sequence classification.
 */
export class MPNetForSequenceClassification extends MPNetPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<SequenceClassifierOutput>} An object containing the model's output logits for sequence classification.
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
/**
 * MPNetForTokenClassification is a class representing a MPNet model for token classification.
 */
export class MPNetForTokenClassification extends MPNetPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<TokenClassifierOutput>} An object containing the model's output logits for token classification.
     */
    _call(model_inputs: any): Promise<TokenClassifierOutput>;
}
/**
 * MPNetForQuestionAnswering is a class representing a MPNet model for question answering.
 */
export class MPNetForQuestionAnswering extends MPNetPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<QuestionAnsweringModelOutput>} An object containing the model's output logits for question answering.
     */
    _call(model_inputs: any): Promise<QuestionAnsweringModelOutput>;
}
export class SqueezeBertPreTrainedModel extends PreTrainedModel {
}
export class SqueezeBertModel extends SqueezeBertPreTrainedModel {
}
export class SqueezeBertForMaskedLM extends SqueezeBertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<MaskedLMOutput>} returned object
     */
    _call(model_inputs: any): Promise<MaskedLMOutput>;
}
export class SqueezeBertForSequenceClassification extends SqueezeBertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<SequenceClassifierOutput>} returned object
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
export class SqueezeBertForQuestionAnswering extends SqueezeBertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<QuestionAnsweringModelOutput>} returned object
     */
    _call(model_inputs: any): Promise<QuestionAnsweringModelOutput>;
}
export class AlbertPreTrainedModel extends PreTrainedModel {
}
export class AlbertModel extends AlbertPreTrainedModel {
}
export class AlbertForSequenceClassification extends AlbertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<SequenceClassifierOutput>} returned object
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
export class AlbertForQuestionAnswering extends AlbertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<QuestionAnsweringModelOutput>} returned object
     */
    _call(model_inputs: any): Promise<QuestionAnsweringModelOutput>;
}
export class AlbertForMaskedLM extends AlbertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<MaskedLMOutput>} returned object
     */
    _call(model_inputs: any): Promise<MaskedLMOutput>;
}
export class T5PreTrainedModel extends PreTrainedModel {
}
export class T5Model extends T5PreTrainedModel {
}
/**
 * T5Model is a class representing a T5 model for conditional generation.
 */
export class T5ForConditionalGeneration extends T5PreTrainedModel {
}
/**
 * An abstract class to handle weights initialization and a simple interface for downloading and loading pretrained models.
 */
export class LongT5PreTrainedModel extends PreTrainedModel {
}
/**
 * The bare LONGT5 Model transformer outputting raw hidden-states without any specific head on top.
 */
export class LongT5Model extends LongT5PreTrainedModel {
}
/**
 * LONGT5 Model with a `language modeling` head on top.
 */
export class LongT5ForConditionalGeneration extends LongT5PreTrainedModel {
}
export class MT5PreTrainedModel extends PreTrainedModel {
}
export class MT5Model extends MT5PreTrainedModel {
}
/**
 * A class representing a conditional sequence-to-sequence model based on the MT5 architecture.
 */
export class MT5ForConditionalGeneration extends MT5PreTrainedModel {
}
export class BartPretrainedModel extends PreTrainedModel {
}
/**
 * The bare BART Model outputting raw hidden-states without any specific head on top.
 */
export class BartModel extends BartPretrainedModel {
}
/**
 * The BART Model with a language modeling head. Can be used for summarization.
 */
export class BartForConditionalGeneration extends BartPretrainedModel {
}
/**
 * Bart model with a sequence classification/head on top (a linear layer on top of the pooled output)
 */
export class BartForSequenceClassification extends BartPretrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<SequenceClassifierOutput>} An object containing the model's output logits for sequence classification.
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
export class MBartPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare MBART Model outputting raw hidden-states without any specific head on top.
 */
export class MBartModel extends MBartPreTrainedModel {
}
/**
 * The MBART Model with a language modeling head. Can be used for summarization, after fine-tuning the pretrained models.
 */
export class MBartForConditionalGeneration extends MBartPreTrainedModel {
}
/**
 * MBart model with a sequence classification/head on top (a linear layer on top of the pooled output).
 */
export class MBartForSequenceClassification extends MBartPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<SequenceClassifierOutput>} An object containing the model's output logits for sequence classification.
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
export class MBartForCausalLM extends MBartPreTrainedModel {
}
export class BlenderbotPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare Blenderbot Model outputting raw hidden-states without any specific head on top.
 */
export class BlenderbotModel extends BlenderbotPreTrainedModel {
}
/**
 * The Blenderbot Model with a language modeling head. Can be used for summarization.
 */
export class BlenderbotForConditionalGeneration extends BlenderbotPreTrainedModel {
}
export class BlenderbotSmallPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare BlenderbotSmall Model outputting raw hidden-states without any specific head on top.
 */
export class BlenderbotSmallModel extends BlenderbotSmallPreTrainedModel {
}
/**
 * The BlenderbotSmall Model with a language modeling head. Can be used for summarization.
 */
export class BlenderbotSmallForConditionalGeneration extends BlenderbotSmallPreTrainedModel {
}
export class RobertaPreTrainedModel extends PreTrainedModel {
}
export class RobertaModel extends RobertaPreTrainedModel {
}
/**
 * RobertaForMaskedLM class for performing masked language modeling on Roberta models.
 */
export class RobertaForMaskedLM extends RobertaPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<MaskedLMOutput>} returned object
     */
    _call(model_inputs: any): Promise<MaskedLMOutput>;
}
/**
 * RobertaForSequenceClassification class for performing sequence classification on Roberta models.
 */
export class RobertaForSequenceClassification extends RobertaPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<SequenceClassifierOutput>} returned object
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
/**
 * RobertaForTokenClassification class for performing token classification on Roberta models.
 */
export class RobertaForTokenClassification extends RobertaPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<TokenClassifierOutput>} An object containing the model's output logits for token classification.
     */
    _call(model_inputs: any): Promise<TokenClassifierOutput>;
}
/**
 * RobertaForQuestionAnswering class for performing question answering on Roberta models.
 */
export class RobertaForQuestionAnswering extends RobertaPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<QuestionAnsweringModelOutput>} returned object
     */
    _call(model_inputs: any): Promise<QuestionAnsweringModelOutput>;
}
/**
 * An abstract class to handle weights initialization and a simple interface for downloading and loading pretrained models.
 */
export class XLMPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare XLM Model transformer outputting raw hidden-states without any specific head on top.
 */
export class XLMModel extends XLMPreTrainedModel {
}
/**
 * The XLM Model transformer with a language modeling head on top (linear layer with weights tied to the input embeddings).
 */
export class XLMWithLMHeadModel extends XLMPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<MaskedLMOutput>} returned object
     */
    _call(model_inputs: any): Promise<MaskedLMOutput>;
}
/**
 * XLM Model with a sequence classification/regression head on top (a linear layer on top of the pooled output)
 */
export class XLMForSequenceClassification extends XLMPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<SequenceClassifierOutput>} returned object
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
/**
 * XLM Model with a token classification head on top (a linear layer on top of the hidden-states output)
 */
export class XLMForTokenClassification extends XLMPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<TokenClassifierOutput>} An object containing the model's output logits for token classification.
     */
    _call(model_inputs: any): Promise<TokenClassifierOutput>;
}
/**
 * XLM Model with a span classification head on top for extractive question-answering tasks
 */
export class XLMForQuestionAnswering extends XLMPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<QuestionAnsweringModelOutput>} returned object
     */
    _call(model_inputs: any): Promise<QuestionAnsweringModelOutput>;
}
export class XLMRobertaPreTrainedModel extends PreTrainedModel {
}
export class XLMRobertaModel extends XLMRobertaPreTrainedModel {
}
/**
 * XLMRobertaForMaskedLM class for performing masked language modeling on XLMRoberta models.
 */
export class XLMRobertaForMaskedLM extends XLMRobertaPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<MaskedLMOutput>} returned object
     */
    _call(model_inputs: any): Promise<MaskedLMOutput>;
}
/**
 * XLMRobertaForSequenceClassification class for performing sequence classification on XLMRoberta models.
 */
export class XLMRobertaForSequenceClassification extends XLMRobertaPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<SequenceClassifierOutput>} returned object
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
/**
 * XLMRobertaForTokenClassification class for performing token classification on XLMRoberta models.
 */
export class XLMRobertaForTokenClassification extends XLMRobertaPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<TokenClassifierOutput>} An object containing the model's output logits for token classification.
     */
    _call(model_inputs: any): Promise<TokenClassifierOutput>;
}
/**
 * XLMRobertaForQuestionAnswering class for performing question answering on XLMRoberta models.
 */
export class XLMRobertaForQuestionAnswering extends XLMRobertaPreTrainedModel {
    /**
     * Calls the model on new inputs.
     *
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<QuestionAnsweringModelOutput>} returned object
     */
    _call(model_inputs: any): Promise<QuestionAnsweringModelOutput>;
}
export class ASTPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare AST Model transformer outputting raw hidden-states without any specific head on top.
 */
export class ASTModel extends ASTPreTrainedModel {
}
/**
 * Audio Spectrogram Transformer model with an audio classification head on top
 * (a linear layer on top of the pooled output) e.g. for datasets like AudioSet, Speech Commands v2.
 */
export class ASTForAudioClassification extends ASTPreTrainedModel {
}
export class WhisperPreTrainedModel extends PreTrainedModel {
    requires_attention_mask: boolean;
}
/**
 * WhisperModel class for training Whisper models without a language model head.
 */
export class WhisperModel extends WhisperPreTrainedModel {
}
/**
 * WhisperForConditionalGeneration class for generating conditional outputs from Whisper models.
 */
export class WhisperForConditionalGeneration extends WhisperPreTrainedModel {
    _prepare_generation_config(generation_config: any, kwargs: any): WhisperGenerationConfig;
    /**
     *
     * @param {WhisperGenerationConfig} generation_config
     */
    _retrieve_init_tokens(generation_config: WhisperGenerationConfig): number[];
    /**
     * Transcribes or translates log-mel input features to a sequence of auto-regressively generated token ids.
     * @param {import('./models/whisper/generation_whisper.js').WhisperGenerationFunctionParameters} options
     * @returns {Promise<ModelOutput|Tensor>} The output of the model, which can contain the generated token ids, attentions, and scores.
     */
    generate({ inputs, generation_config, logits_processor, stopping_criteria, ...kwargs }: import("./models/whisper/generation_whisper.js").WhisperGenerationFunctionParameters): Promise<ModelOutput | Tensor>;
    /**
     * Calculates token-level timestamps using the encoder-decoder cross-attentions and
     * dynamic time-warping (DTW) to map each output token to a position in the input audio.
     * If `num_frames` is specified, the encoder-decoder cross-attentions will be cropped before applying DTW.
     * @param {Object} generate_outputs Outputs generated by the model
     * @param {Tensor[][]} generate_outputs.cross_attentions The cross attentions output by the model
     * @param {Tensor} generate_outputs.sequences The sequences output by the model
     * @param {number[][]} alignment_heads Alignment heads of the model
     * @param {number} [num_frames=null] Number of frames in the input audio.
     * @param {number} [time_precision=0.02] Precision of the timestamps in seconds
     * @returns {Tensor} tensor containing the timestamps in seconds for each predicted token
     */
    _extract_token_timestamps(generate_outputs: {
        cross_attentions: Tensor[][];
        sequences: Tensor;
    }, alignment_heads: number[][], num_frames?: number, time_precision?: number): Tensor;
}
export class LiteWhisperForConditionalGeneration extends WhisperForConditionalGeneration {
}
export class MoonshinePreTrainedModel extends PreTrainedModel {
    requires_attention_mask: boolean;
}
/**
 * MoonshineModel class for training Moonshine models without a language model head.
 */
export class MoonshineModel extends MoonshinePreTrainedModel {
}
export class MoonshineForConditionalGeneration extends MoonshinePreTrainedModel {
}
/**
 * Vision Encoder-Decoder model based on OpenAI's GPT architecture for image captioning and other vision tasks
 */
export class VisionEncoderDecoderModel extends PreTrainedModel {
}
export class LlavaPreTrainedModel extends PreTrainedModel {
}
/**
 * The LLAVA model which consists of a vision backbone and a language model.
 */
export class LlavaForConditionalGeneration extends LlavaPreTrainedModel {
    _merge_input_ids_with_image_features({ inputs_embeds, image_features, input_ids, attention_mask, }: {
        inputs_embeds: any;
        image_features: any;
        input_ids: any;
        attention_mask: any;
    }): {
        inputs_embeds: any;
        attention_mask: any;
    };
}
export class LlavaOnevisionForConditionalGeneration extends LlavaForConditionalGeneration {
}
export class Moondream1ForConditionalGeneration extends LlavaForConditionalGeneration {
}
export class Florence2PreTrainedModel extends PreTrainedModel {
}
export class Florence2ForConditionalGeneration extends Florence2PreTrainedModel {
    _merge_input_ids_with_image_features({ inputs_embeds, image_features, input_ids, attention_mask, }: {
        inputs_embeds: any;
        image_features: any;
        input_ids: any;
        attention_mask: any;
    }): {
        inputs_embeds: Tensor;
        attention_mask: Tensor;
    };
    _prepare_inputs_embeds({ input_ids, pixel_values, inputs_embeds, attention_mask }: {
        input_ids: any;
        pixel_values: any;
        inputs_embeds: any;
        attention_mask: any;
    }): Promise<{
        inputs_embeds: any;
        attention_mask: any;
    }>;
    forward({ input_ids, pixel_values, attention_mask, decoder_input_ids, decoder_attention_mask, encoder_outputs, past_key_values, inputs_embeds, decoder_inputs_embeds, }: {
        input_ids: any;
        pixel_values: any;
        attention_mask: any;
        decoder_input_ids: any;
        decoder_attention_mask: any;
        encoder_outputs: any;
        past_key_values: any;
        inputs_embeds: any;
        decoder_inputs_embeds: any;
    }): Promise<any>;
}
export class PaliGemmaPreTrainedModel extends PreTrainedModel {
}
export class PaliGemmaForConditionalGeneration extends PaliGemmaPreTrainedModel {
    _merge_input_ids_with_image_features(kwargs: any): {
        inputs_embeds: any;
        attention_mask: any;
    };
}
export class Idefics3PreTrainedModel extends PreTrainedModel {
}
/**
 * The Idefics3 model which consists of a vision backbone and a language model.
 */
export class Idefics3ForConditionalGeneration extends Idefics3PreTrainedModel {
    encode_image({ pixel_values, pixel_attention_mask }: {
        pixel_values: any;
        pixel_attention_mask: any;
    }): Promise<any>;
    _merge_input_ids_with_image_features(kwargs: any): {
        inputs_embeds: any;
        attention_mask: any;
    };
}
/**
 * The SmolVLM Model with a language modeling head.
 * It is made up a SigLIP vision encoder, with a language modeling head on top.
 */
export class SmolVLMForConditionalGeneration extends Idefics3ForConditionalGeneration {
}
export class Phi3VPreTrainedModel extends PreTrainedModel {
}
export class Phi3VForCausalLM extends Phi3VPreTrainedModel {
    forward({ input_ids, attention_mask, pixel_values, image_sizes, position_ids, inputs_embeds, past_key_values, generation_config, logits_processor, ...kwargs }: {
        [x: string]: any;
        input_ids?: any;
        attention_mask?: any;
        pixel_values?: any;
        image_sizes?: any;
        position_ids?: any;
        inputs_embeds?: any;
        past_key_values?: any;
        generation_config?: any;
        logits_processor?: any;
    }): Promise<any>;
}
export class CLIPPreTrainedModel extends PreTrainedModel {
}
/**
 * CLIP Text and Vision Model with a projection layers on top
 *
 * **Example:** Perform zero-shot image classification with a `CLIPModel`.
 *
 * ```javascript
 * import { AutoTokenizer, AutoProcessor, CLIPModel, RawImage } from '@huggingface/transformers';
 *
 * // Load tokenizer, processor, and model
 * let tokenizer = await AutoTokenizer.from_pretrained('Xenova/clip-vit-base-patch16');
 * let processor = await AutoProcessor.from_pretrained('Xenova/clip-vit-base-patch16');
 * let model = await CLIPModel.from_pretrained('Xenova/clip-vit-base-patch16');
 *
 * // Run tokenization
 * let texts = ['a photo of a car', 'a photo of a football match']
 * let text_inputs = tokenizer(texts, { padding: true, truncation: true });
 *
 * // Read image and run processor
 * let image = await RawImage.read('https://huggingface.co/datasets/Xenova/transformers.js-docs/resolve/main/football-match.jpg');
 * let image_inputs = await processor(image);
 *
 * // Run model with both text and pixel inputs
 * let output = await model({ ...text_inputs, ...image_inputs });
 * // {
 * //   logits_per_image: Tensor {
 * //     dims: [ 1, 2 ],
 * //     data: Float32Array(2) [ 18.579734802246094, 24.31830596923828 ],
 * //   },
 * //   logits_per_text: Tensor {
 * //     dims: [ 2, 1 ],
 * //     data: Float32Array(2) [ 18.579734802246094, 24.31830596923828 ],
 * //   },
 * //   text_embeds: Tensor {
 * //     dims: [ 2, 512 ],
 * //     data: Float32Array(1024) [ ... ],
 * //   },
 * //   image_embeds: Tensor {
 * //     dims: [ 1, 512 ],
 * //     data: Float32Array(512) [ ... ],
 * //   }
 * // }
 * ```
 */
export class CLIPModel extends CLIPPreTrainedModel {
}
/**
 * The text model from CLIP without any head or projection on top.
 */
export class CLIPTextModel extends CLIPPreTrainedModel {
}
/**
 * CLIP Text Model with a projection layer on top (a linear layer on top of the pooled output)
 *
 * **Example:** Compute text embeddings with `CLIPTextModelWithProjection`.
 *
 * ```javascript
 * import { AutoTokenizer, CLIPTextModelWithProjection } from '@huggingface/transformers';
 *
 * // Load tokenizer and text model
 * const tokenizer = await AutoTokenizer.from_pretrained('Xenova/clip-vit-base-patch16');
 * const text_model = await CLIPTextModelWithProjection.from_pretrained('Xenova/clip-vit-base-patch16');
 *
 * // Run tokenization
 * let texts = ['a photo of a car', 'a photo of a football match'];
 * let text_inputs = tokenizer(texts, { padding: true, truncation: true });
 *
 * // Compute embeddings
 * const { text_embeds } = await text_model(text_inputs);
 * // Tensor {
 * //   dims: [ 2, 512 ],
 * //   type: 'float32',
 * //   data: Float32Array(1024) [ ... ],
 * //   size: 1024
 * // }
 * ```
 */
export class CLIPTextModelWithProjection extends CLIPPreTrainedModel {
}
/**
 * The vision model from CLIP without any head or projection on top.
 */
export class CLIPVisionModel extends CLIPPreTrainedModel {
}
/**
 * CLIP Vision Model with a projection layer on top (a linear layer on top of the pooled output)
 *
 * **Example:** Compute vision embeddings with `CLIPVisionModelWithProjection`.
 *
 * ```javascript
 * import { AutoProcessor, CLIPVisionModelWithProjection, RawImage} from '@huggingface/transformers';
 *
 * // Load processor and vision model
 * const processor = await AutoProcessor.from_pretrained('Xenova/clip-vit-base-patch16');
 * const vision_model = await CLIPVisionModelWithProjection.from_pretrained('Xenova/clip-vit-base-patch16');
 *
 * // Read image and run processor
 * let image = await RawImage.read('https://huggingface.co/datasets/Xenova/transformers.js-docs/resolve/main/football-match.jpg');
 * let image_inputs = await processor(image);
 *
 * // Compute embeddings
 * const { image_embeds } = await vision_model(image_inputs);
 * // Tensor {
 * //   dims: [ 1, 512 ],
 * //   type: 'float32',
 * //   data: Float32Array(512) [ ... ],
 * //   size: 512
 * // }
 * ```
 */
export class CLIPVisionModelWithProjection extends CLIPPreTrainedModel {
}
export class SiglipPreTrainedModel extends PreTrainedModel {
}
/**
 * SigLIP Text and Vision Model with a projection layers on top
 *
 * **Example:** Perform zero-shot image classification with a `SiglipModel`.
 *
 * ```javascript
 * import { AutoTokenizer, AutoProcessor, SiglipModel, RawImage } from '@huggingface/transformers';
 *
 * // Load tokenizer, processor, and model
 * const tokenizer = await AutoTokenizer.from_pretrained('Xenova/siglip-base-patch16-224');
 * const processor = await AutoProcessor.from_pretrained('Xenova/siglip-base-patch16-224');
 * const model = await SiglipModel.from_pretrained('Xenova/siglip-base-patch16-224');
 *
 * // Run tokenization
 * const texts = ['a photo of 2 cats', 'a photo of 2 dogs'];
 * const text_inputs = tokenizer(texts, { padding: 'max_length', truncation: true });
 *
 * // Read image and run processor
 * const image = await RawImage.read('http://images.cocodataset.org/val2017/000000039769.jpg');
 * const image_inputs = await processor(image);
 *
 * // Run model with both text and pixel inputs
 * const output = await model({ ...text_inputs, ...image_inputs });
 * // {
 * //   logits_per_image: Tensor {
 * //     dims: [ 1, 2 ],
 * //     data: Float32Array(2) [ -1.6019744873046875, -10.720091819763184 ],
 * //   },
 * //   logits_per_text: Tensor {
 * //     dims: [ 2, 1 ],
 * //     data: Float32Array(2) [ -1.6019744873046875, -10.720091819763184 ],
 * //   },
 * //   text_embeds: Tensor {
 * //     dims: [ 2, 768 ],
 * //     data: Float32Array(1536) [ ... ],
 * //   },
 * //   image_embeds: Tensor {
 * //     dims: [ 1, 768 ],
 * //     data: Float32Array(768) [ ... ],
 * //   }
 * // }
 * ```
 */
export class SiglipModel extends SiglipPreTrainedModel {
}
/**
 * The text model from SigLIP without any head or projection on top.
 *
 * **Example:** Compute text embeddings with `SiglipTextModel`.
 *
 * ```javascript
 * import { AutoTokenizer, SiglipTextModel } from '@huggingface/transformers';
 *
 * // Load tokenizer and text model
 * const tokenizer = await AutoTokenizer.from_pretrained('Xenova/siglip-base-patch16-224');
 * const text_model = await SiglipTextModel.from_pretrained('Xenova/siglip-base-patch16-224');
 *
 * // Run tokenization
 * const texts = ['a photo of 2 cats', 'a photo of 2 dogs'];
 * const text_inputs = tokenizer(texts, { padding: 'max_length', truncation: true });
 *
 * // Compute embeddings
 * const { pooler_output } = await text_model(text_inputs);
 * // Tensor {
 * //   dims: [ 2, 768 ],
 * //   type: 'float32',
 * //   data: Float32Array(1536) [ ... ],
 * //   size: 1536
 * // }
 * ```
 */
export class SiglipTextModel extends SiglipPreTrainedModel {
}
/**
 * The vision model from SigLIP without any head or projection on top.
 *
 * **Example:** Compute vision embeddings with `SiglipVisionModel`.
 *
 * ```javascript
 * import { AutoProcessor, SiglipVisionModel, RawImage} from '@huggingface/transformers';
 *
 * // Load processor and vision model
 * const processor = await AutoProcessor.from_pretrained('Xenova/siglip-base-patch16-224');
 * const vision_model = await SiglipVisionModel.from_pretrained('Xenova/siglip-base-patch16-224');
 *
 * // Read image and run processor
 * const image = await RawImage.read('https://huggingface.co/datasets/Xenova/transformers.js-docs/resolve/main/football-match.jpg');
 * const image_inputs = await processor(image);
 *
 * // Compute embeddings
 * const { pooler_output } = await vision_model(image_inputs);
 * // Tensor {
 * //   dims: [ 1, 768 ],
 * //   type: 'float32',
 * //   data: Float32Array(768) [ ... ],
 * //   size: 768
 * // }
 * ```
 */
export class SiglipVisionModel extends CLIPPreTrainedModel {
}
export class ChineseCLIPPreTrainedModel extends PreTrainedModel {
}
export class ChineseCLIPModel extends ChineseCLIPPreTrainedModel {
}
export class JinaCLIPPreTrainedModel extends PreTrainedModel {
}
export class JinaCLIPModel extends JinaCLIPPreTrainedModel {
    forward(model_inputs: any): Promise<{
        text_embeddings: any;
        l2norm_text_embeddings: any;
        image_embeddings: any;
        l2norm_image_embeddings: any;
    }>;
}
export class JinaCLIPTextModel extends JinaCLIPPreTrainedModel {
}
export class JinaCLIPVisionModel extends JinaCLIPPreTrainedModel {
}
export class CLIPSegPreTrainedModel extends PreTrainedModel {
}
export class CLIPSegModel extends CLIPSegPreTrainedModel {
}
/**
 * CLIPSeg model with a Transformer-based decoder on top for zero-shot and one-shot image segmentation.
 *
 * **Example:** Perform zero-shot image segmentation with a `CLIPSegForImageSegmentation` model.
 *
 * ```javascript
 * import { AutoTokenizer, AutoProcessor, CLIPSegForImageSegmentation, RawImage } from '@huggingface/transformers';
 *
 * // Load tokenizer, processor, and model
 * const tokenizer = await AutoTokenizer.from_pretrained('Xenova/clipseg-rd64-refined');
 * const processor = await AutoProcessor.from_pretrained('Xenova/clipseg-rd64-refined');
 * const model = await CLIPSegForImageSegmentation.from_pretrained('Xenova/clipseg-rd64-refined');
 *
 * // Run tokenization
 * const texts = ['a glass', 'something to fill', 'wood', 'a jar'];
 * const text_inputs = tokenizer(texts, { padding: true, truncation: true });
 *
 * // Read image and run processor
 * const image = await RawImage.read('https://github.com/timojl/clipseg/blob/master/example_image.jpg?raw=true');
 * const image_inputs = await processor(image);
 *
 * // Run model with both text and pixel inputs
 * const { logits } = await model({ ...text_inputs, ...image_inputs });
 * // logits: Tensor {
 * //   dims: [4, 352, 352],
 * //   type: 'float32',
 * //   data: Float32Array(495616) [ ... ],
 * //   size: 495616
 * // }
 * ```
 *
 * You can visualize the predictions as follows:
 * ```javascript
 * const preds = logits
 *   .unsqueeze_(1)
 *   .sigmoid_()
 *   .mul_(255)
 *   .round_()
 *   .to('uint8');
 *
 * for (let i = 0; i < preds.dims[0]; ++i) {
 *   const img = RawImage.fromTensor(preds[i]);
 *   img.save(`prediction_${i}.png`);
 * }
 * ```
 */
export class CLIPSegForImageSegmentation extends CLIPSegPreTrainedModel {
}
export class GPT2PreTrainedModel extends PreTrainedModel {
}
export class GPT2Model extends GPT2PreTrainedModel {
}
/**
 * GPT-2 language model head on top of the GPT-2 base model. This model is suitable for text generation tasks.
 */
export class GPT2LMHeadModel extends GPT2PreTrainedModel {
}
export class JAISPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare JAIS Model transformer outputting raw hidden-states without any specific head on top.
 */
export class JAISModel extends JAISPreTrainedModel {
}
/**
 * The JAIS Model transformer with a language modeling head on top (linear layer with weights tied to the input embeddings).
 */
export class JAISLMHeadModel extends JAISPreTrainedModel {
}
export class GPTNeoPreTrainedModel extends PreTrainedModel {
}
export class GPTNeoModel extends GPTNeoPreTrainedModel {
}
export class GPTNeoForCausalLM extends GPTNeoPreTrainedModel {
}
export class GPTNeoXPreTrainedModel extends PreTrainedModel {
}
export class GPTNeoXModel extends GPTNeoXPreTrainedModel {
}
export class GPTNeoXForCausalLM extends GPTNeoXPreTrainedModel {
}
export class GPTJPreTrainedModel extends PreTrainedModel {
}
export class GPTJModel extends GPTJPreTrainedModel {
}
export class GPTJForCausalLM extends GPTJPreTrainedModel {
}
export class GPTBigCodePreTrainedModel extends PreTrainedModel {
}
export class GPTBigCodeModel extends GPTBigCodePreTrainedModel {
}
export class GPTBigCodeForCausalLM extends GPTBigCodePreTrainedModel {
}
export class CodeGenPreTrainedModel extends PreTrainedModel {
}
/**
 * CodeGenModel is a class representing a code generation model without a language model head.
 */
export class CodeGenModel extends CodeGenPreTrainedModel {
}
/**
 * CodeGenForCausalLM is a class that represents a code generation model based on the GPT-2 architecture. It extends the `CodeGenPreTrainedModel` class.
 */
export class CodeGenForCausalLM extends CodeGenPreTrainedModel {
}
/**
 * The bare LLama Model outputting raw hidden-states without any specific head on top.
 */
export class LlamaPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare LLaMA Model outputting raw hidden-states without any specific head on top.
 */
export class LlamaModel extends LlamaPreTrainedModel {
}
export class LlamaForCausalLM extends LlamaPreTrainedModel {
}
export class HeliumPreTrainedModel extends PreTrainedModel {
}
export class HeliumModel extends HeliumPreTrainedModel {
}
export class HeliumForCausalLM extends HeliumPreTrainedModel {
}
export class GlmPreTrainedModel extends PreTrainedModel {
}
export class GlmModel extends GlmPreTrainedModel {
}
export class GlmForCausalLM extends GlmPreTrainedModel {
}
export class ExaonePreTrainedModel extends PreTrainedModel {
}
export class ExaoneModel extends ExaonePreTrainedModel {
}
export class ExaoneForCausalLM extends ExaonePreTrainedModel {
}
export class MobileLLMPreTrainedModel extends PreTrainedModel {
}
export class MobileLLMModel extends MobileLLMPreTrainedModel {
}
export class MobileLLMForCausalLM extends MobileLLMPreTrainedModel {
}
export class OlmoPreTrainedModel extends PreTrainedModel {
}
export class OlmoModel extends OlmoPreTrainedModel {
}
export class OlmoForCausalLM extends OlmoPreTrainedModel {
}
export class Olmo2PreTrainedModel extends PreTrainedModel {
}
export class Olmo2Model extends Olmo2PreTrainedModel {
}
export class Olmo2ForCausalLM extends Olmo2PreTrainedModel {
}
export class GranitePreTrainedModel extends PreTrainedModel {
}
export class GraniteModel extends GranitePreTrainedModel {
}
export class GraniteForCausalLM extends GranitePreTrainedModel {
}
/**
 * The bare Cohere Model outputting raw hidden-states without any specific head on top.
 */
export class CoherePreTrainedModel extends PreTrainedModel {
}
export class CohereModel extends CoherePreTrainedModel {
}
export class CohereForCausalLM extends CoherePreTrainedModel {
}
/**
 * The bare Gemma Model outputting raw hidden-states without any specific head on top.
 */
export class GemmaPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare Gemma Model outputting raw hidden-states without any specific head on top.
 */
export class GemmaModel extends GemmaPreTrainedModel {
}
export class GemmaForCausalLM extends GemmaPreTrainedModel {
}
/**
 * The bare Gemma2 Model outputting raw hidden-states without any specific head on top.
 */
export class Gemma2PreTrainedModel extends PreTrainedModel {
}
/**
 * The bare Gemma2 Model outputting raw hidden-states without any specific head on top.
 */
export class Gemma2Model extends Gemma2PreTrainedModel {
}
export class Gemma2ForCausalLM extends Gemma2PreTrainedModel {
}
/**
 * The bare Gemma3 Model outputting raw hidden-states without any specific head on top.
 */
export class Gemma3PreTrainedModel extends PreTrainedModel {
}
/**
 * The bare Gemma3 Model outputting raw hidden-states without any specific head on top.
 */
export class Gemma3Model extends Gemma3PreTrainedModel {
}
export class Gemma3ForCausalLM extends Gemma3PreTrainedModel {
}
export class OpenELMPreTrainedModel extends PreTrainedModel {
}
export class OpenELMModel extends OpenELMPreTrainedModel {
}
export class OpenELMForCausalLM extends OpenELMPreTrainedModel {
}
/**
 * The bare Qwen2 Model outputting raw hidden-states without any specific head on top.
 */
export class Qwen2PreTrainedModel extends PreTrainedModel {
}
/**
 * The bare Qwen2 Model outputting raw hidden-states without any specific head on top.
 */
export class Qwen2Model extends Qwen2PreTrainedModel {
}
export class Qwen2ForCausalLM extends Qwen2PreTrainedModel {
}
/**
 * The bare Qwen3 Model outputting raw hidden-states without any specific head on top.
 */
export class Qwen3PreTrainedModel extends PreTrainedModel {
}
/**
 * The bare Qwen3 Model outputting raw hidden-states without any specific head on top.
 */
export class Qwen3Model extends Qwen3PreTrainedModel {
}
export class Qwen3ForCausalLM extends Qwen3PreTrainedModel {
}
export class Qwen2VLPreTrainedModel extends PreTrainedModel {
}
export class Qwen2VLForConditionalGeneration extends Qwen2VLPreTrainedModel {
    /**
     * Calculate the 3D rope index based on image and video's temporal, height and width in LLM.
     *
     * Explanation:
     *     Each embedding sequence contains vision embedding and text embedding or just contains text embedding.
     *
     *     For pure text embedding sequence, the rotary position embedding has no difference with mordern LLMs.
     *     Examples:
     *         input_ids: [T T T T T], here T is for text.
     *         temporal position_ids: [0, 1, 2, 3, 4]
     *         height position_ids: [0, 1, 2, 3, 4]
     *         width position_ids: [0, 1, 2, 3, 4]
     *
     *     For vision and text embedding sequence, we calculate 3D rotary position embedding for vision part
     *     and 1D rotary position embeddin for text part.
     *     Examples:
     *         Assume we have a video input with 3 temporal patches, 2 height patches and 2 width patches.
     *         input_ids: [V V V V V V V V V V V V T T T T T], here V is for vision.
     *         vision temporal position_ids: [0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2]
     *         vision height position_ids: [0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1]
     *         vision width position_ids: [0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1]
     *         text temporal position_ids: [3, 4, 5, 6, 7]
     *         text height position_ids: [3, 4, 5, 6, 7]
     *         text width position_ids: [3, 4, 5, 6, 7]
     *         Here we calculate the text start position_ids as the max vision position_ids plus 1.
     *
     * @param {Tensor} input_ids Indices of input sequence tokens in the vocabulary. Tensor of shape `(batch_size, sequence_length)`.
     * @param {Tensor} image_grid_thw (Optional) The temporal, height and width of feature shape of each image in LLM. Tensor of shape `(num_images, 3)`.
     * @param {Tensor} video_grid_thw (Optional) The temporal, height and width of feature shape of each video in LLM. Tensor of shape `(num_videos, 3)`.
     * @param {Tensor} attention_mask (Optional) Mask to avoid performing attention on padding token indices. Tensor of shape `(batch_size, sequence_length)`. Mask values selected in `[0, 1]`:
     * - 1 for tokens that are **not masked**,
     * - 0 for tokens that are **masked**.
     * @returns {[Tensor, Tensor]} [position_ids, mrope_position_deltas] with:
     * - position_ids: Tensor of shape `(3, batch_size, sequence_length)`.
     * - mrope_position_deltas: Tensor of shape `(batch_size)`.
     */
    get_rope_index(input_ids: Tensor, image_grid_thw: Tensor, video_grid_thw: Tensor, attention_mask: Tensor): [Tensor, Tensor];
    encode_image({ pixel_values, image_grid_thw }: {
        pixel_values: any;
        image_grid_thw: any;
    }): Promise<any>;
    _merge_input_ids_with_image_features(kwargs: any): {
        inputs_embeds: any;
        attention_mask: any;
    };
    prepare_inputs_for_generation(input_ids: any, model_inputs: any, generation_config: any): any;
}
export class PhiPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare Phi Model outputting raw hidden-states without any specific head on top.
 */
export class PhiModel extends PhiPreTrainedModel {
}
export class PhiForCausalLM extends PhiPreTrainedModel {
}
export class Phi3PreTrainedModel extends PreTrainedModel {
}
/**
 * The bare Phi3 Model outputting raw hidden-states without any specific head on top.
 */
export class Phi3Model extends Phi3PreTrainedModel {
}
export class Phi3ForCausalLM extends Phi3PreTrainedModel {
}
/**
 * The Bloom Model transformer with a language modeling head on top (linear layer with weights tied to the input embeddings).
 */
export class BloomPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare Bloom Model transformer outputting raw hidden-states without any specific head on top.
 */
export class BloomModel extends BloomPreTrainedModel {
}
/**
 * The Bloom Model transformer with a language modeling head on top (linear layer with weights tied to the input embeddings).
 */
export class BloomForCausalLM extends BloomPreTrainedModel {
}
export class MptPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare Mpt Model transformer outputting raw hidden-states without any specific head on top.
 */
export class MptModel extends MptPreTrainedModel {
}
/**
 * The MPT Model transformer with a language modeling head on top (linear layer with weights tied to the input embeddings).
 */
export class MptForCausalLM extends MptPreTrainedModel {
}
export class OPTPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare OPT Model outputting raw hidden-states without any specific head on top.
 */
export class OPTModel extends OPTPreTrainedModel {
}
/**
 * The OPT Model transformer with a language modeling head on top (linear layer with weights tied to the input embeddings).
 */
export class OPTForCausalLM extends OPTPreTrainedModel {
}
export class ViTPreTrainedModel extends PreTrainedModel {
}
export class ViTModel extends ViTPreTrainedModel {
}
export class ViTForImageClassification extends ViTPreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
export class IJepaPreTrainedModel extends PreTrainedModel {
}
export class IJepaModel extends IJepaPreTrainedModel {
}
export class IJepaForImageClassification extends IJepaPreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
export class VitPosePreTrainedModel extends PreTrainedModel {
}
/**
 * The VitPose model with a pose estimation head on top.
 */
export class VitPoseForPoseEstimation extends VitPosePreTrainedModel {
}
export class PvtPreTrainedModel extends PreTrainedModel {
}
export class PvtModel extends PvtPreTrainedModel {
}
export class PvtForImageClassification extends PvtPreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
export class ViTMAEPreTrainedModel extends PreTrainedModel {
}
export class ViTMAEModel extends ViTMAEPreTrainedModel {
}
export class ViTMSNPreTrainedModel extends PreTrainedModel {
}
export class ViTMSNModel extends ViTMSNPreTrainedModel {
}
export class ViTMSNForImageClassification extends ViTMSNPreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
export class GroupViTPreTrainedModel extends PreTrainedModel {
}
export class GroupViTModel extends GroupViTPreTrainedModel {
}
export class FastViTPreTrainedModel extends PreTrainedModel {
}
export class FastViTModel extends FastViTPreTrainedModel {
}
export class FastViTForImageClassification extends FastViTPreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
export class VitMattePreTrainedModel extends PreTrainedModel {
}
/**
 * ViTMatte framework leveraging any vision backbone e.g. for ADE20k, CityScapes.
 *
 * **Example:** Perform image matting with a `VitMatteForImageMatting` model.
 * ```javascript
 * import { AutoProcessor, VitMatteForImageMatting, RawImage } from '@huggingface/transformers';
 *
 * // Load processor and model
 * const processor = await AutoProcessor.from_pretrained('Xenova/vitmatte-small-distinctions-646');
 * const model = await VitMatteForImageMatting.from_pretrained('Xenova/vitmatte-small-distinctions-646');
 *
 * // Load image and trimap
 * const image = await RawImage.fromURL('https://huggingface.co/datasets/Xenova/transformers.js-docs/resolve/main/vitmatte_image.png');
 * const trimap = await RawImage.fromURL('https://huggingface.co/datasets/Xenova/transformers.js-docs/resolve/main/vitmatte_trimap.png');
 *
 * // Prepare image + trimap for the model
 * const inputs = await processor(image, trimap);
 *
 * // Predict alpha matte
 * const { alphas } = await model(inputs);
 * // Tensor {
 * //   dims: [ 1, 1, 640, 960 ],
 * //   type: 'float32',
 * //   size: 614400,
 * //   data: Float32Array(614400) [ 0.9894027709960938, 0.9970508813858032, ... ]
 * // }
 * ```
 *
 * You can visualize the alpha matte as follows:
 * ```javascript
 * import { Tensor, cat } from '@huggingface/transformers';
 *
 * // Visualize predicted alpha matte
 * const imageTensor = image.toTensor();
 *
 * // Convert float (0-1) alpha matte to uint8 (0-255)
 * const alphaChannel = alphas
 *   .squeeze(0)
 *   .mul_(255)
 *   .clamp_(0, 255)
 *   .round_()
 *   .to('uint8');
 *
 * // Concatenate original image with predicted alpha
 * const imageData = cat([imageTensor, alphaChannel], 0);
 *
 * // Save output image
 * const outputImage = RawImage.fromTensor(imageData);
 * outputImage.save('output.png');
 * ```
 */
export class VitMatteForImageMatting extends VitMattePreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<ImageMattingOutput>;
}
export class MobileViTPreTrainedModel extends PreTrainedModel {
}
export class MobileViTModel extends MobileViTPreTrainedModel {
}
export class MobileViTForImageClassification extends MobileViTPreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
export class MobileViTV2PreTrainedModel extends PreTrainedModel {
}
export class MobileViTV2Model extends MobileViTV2PreTrainedModel {
}
export class MobileViTV2ForImageClassification extends MobileViTV2PreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
export class OwlViTPreTrainedModel extends PreTrainedModel {
}
export class OwlViTModel extends OwlViTPreTrainedModel {
}
export class OwlViTForObjectDetection extends OwlViTPreTrainedModel {
}
export class Owlv2PreTrainedModel extends PreTrainedModel {
}
export class Owlv2Model extends Owlv2PreTrainedModel {
}
export class Owlv2ForObjectDetection extends Owlv2PreTrainedModel {
}
export class BeitPreTrainedModel extends PreTrainedModel {
}
export class BeitModel extends BeitPreTrainedModel {
}
export class BeitForImageClassification extends BeitPreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
export class DetrPreTrainedModel extends PreTrainedModel {
}
export class DetrModel extends DetrPreTrainedModel {
}
export class DetrForObjectDetection extends DetrPreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<DetrObjectDetectionOutput>;
}
export class DetrForSegmentation extends DetrPreTrainedModel {
    /**
     * Runs the model with the provided inputs
     * @param {Object} model_inputs Model inputs
     * @returns {Promise<DetrSegmentationOutput>} Object containing segmentation outputs
     */
    _call(model_inputs: any): Promise<DetrSegmentationOutput>;
}
export class DetrObjectDetectionOutput extends ModelOutput {
    /**
     * @param {Object} output The output of the model.
     * @param {Tensor} output.logits Classification logits (including no-object) for all queries.
     * @param {Tensor} output.pred_boxes Normalized boxes coordinates for all queries, represented as (center_x, center_y, width, height).
     * These values are normalized in [0, 1], relative to the size of each individual image in the batch (disregarding possible padding).
     */
    constructor({ logits, pred_boxes }: {
        logits: Tensor;
        pred_boxes: Tensor;
    });
    logits: Tensor;
    pred_boxes: Tensor;
}
export class DetrSegmentationOutput extends ModelOutput {
    /**
     * @param {Object} output The output of the model.
     * @param {Tensor} output.logits The output logits of the model.
     * @param {Tensor} output.pred_boxes Predicted boxes.
     * @param {Tensor} output.pred_masks Predicted masks.
     */
    constructor({ logits, pred_boxes, pred_masks }: {
        logits: Tensor;
        pred_boxes: Tensor;
        pred_masks: Tensor;
    });
    logits: Tensor;
    pred_boxes: Tensor;
    pred_masks: Tensor;
}
export class RTDetrPreTrainedModel extends PreTrainedModel {
}
export class RTDetrModel extends RTDetrPreTrainedModel {
}
export class RTDetrForObjectDetection extends RTDetrPreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<RTDetrObjectDetectionOutput>;
}
export class RTDetrObjectDetectionOutput extends ModelOutput {
    /**
     * @param {Object} output The output of the model.
     * @param {Tensor} output.logits Classification logits (including no-object) for all queries.
     * @param {Tensor} output.pred_boxes Normalized boxes coordinates for all queries, represented as (center_x, center_y, width, height).
     * These values are normalized in [0, 1], relative to the size of each individual image in the batch (disregarding possible padding).
     */
    constructor({ logits, pred_boxes }: {
        logits: Tensor;
        pred_boxes: Tensor;
    });
    logits: Tensor;
    pred_boxes: Tensor;
}
export class RTDetrV2PreTrainedModel extends PreTrainedModel {
}
export class RTDetrV2Model extends RTDetrV2PreTrainedModel {
}
export class RTDetrV2ForObjectDetection extends RTDetrV2PreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<RTDetrV2ObjectDetectionOutput>;
}
export class RTDetrV2ObjectDetectionOutput extends RTDetrObjectDetectionOutput {
}
export class RFDetrPreTrainedModel extends PreTrainedModel {
}
export class RFDetrModel extends RFDetrPreTrainedModel {
}
export class RFDetrForObjectDetection extends RFDetrPreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<RFDetrObjectDetectionOutput>;
}
export class RFDetrObjectDetectionOutput extends RTDetrObjectDetectionOutput {
}
export class DFinePreTrainedModel extends PreTrainedModel {
}
export class DFineModel extends DFinePreTrainedModel {
}
export class DFineForObjectDetection extends DFinePreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<RTDetrObjectDetectionOutput>;
}
export class TableTransformerPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare Table Transformer Model (consisting of a backbone and encoder-decoder Transformer)
 * outputting raw hidden-states without any specific head on top.
 */
export class TableTransformerModel extends TableTransformerPreTrainedModel {
}
/**
 * Table Transformer Model (consisting of a backbone and encoder-decoder Transformer)
 * with object detection heads on top, for tasks such as COCO detection.
 */
export class TableTransformerForObjectDetection extends TableTransformerPreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<TableTransformerObjectDetectionOutput>;
}
export class TableTransformerObjectDetectionOutput extends DetrObjectDetectionOutput {
}
export class DeiTPreTrainedModel extends PreTrainedModel {
}
export class DeiTModel extends DeiTPreTrainedModel {
}
export class DeiTForImageClassification extends DeiTPreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
export class HieraPreTrainedModel extends PreTrainedModel {
}
export class HieraModel extends HieraPreTrainedModel {
}
export class HieraForImageClassification extends HieraPreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
/**
 * An abstract class to handle weights initialization and a simple interface for downloading and loading pretrained models.
 */
export class ResNetPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare ResNet model outputting raw features without any specific head on top.
 */
export class ResNetModel extends ResNetPreTrainedModel {
}
/**
 * ResNet Model with an image classification head on top (a linear layer on top of the pooled features), e.g. for ImageNet.
 */
export class ResNetForImageClassification extends ResNetPreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
export class SwinPreTrainedModel extends PreTrainedModel {
}
export class SwinModel extends SwinPreTrainedModel {
}
export class SwinForImageClassification extends SwinPreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
export class SwinForSemanticSegmentation extends SwinPreTrainedModel {
}
export class Swin2SRPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare Swin2SR Model transformer outputting raw hidden-states without any specific head on top.
 */
export class Swin2SRModel extends Swin2SRPreTrainedModel {
}
/**
 * Swin2SR Model transformer with an upsampler head on top for image super resolution and restoration.
 *
 * **Example:** Super-resolution w/ `Xenova/swin2SR-classical-sr-x2-64`.
 *
 * ```javascript
 * import { AutoProcessor, Swin2SRForImageSuperResolution, RawImage } from '@huggingface/transformers';
 *
 * // Load processor and model
 * const model_id = 'Xenova/swin2SR-classical-sr-x2-64';
 * const processor = await AutoProcessor.from_pretrained(model_id);
 * const model = await Swin2SRForImageSuperResolution.from_pretrained(model_id);
 *
 * // Prepare model inputs
 * const url = 'https://huggingface.co/datasets/Xenova/transformers.js-docs/resolve/main/butterfly.jpg';
 * const image = await RawImage.fromURL(url);
 * const inputs = await processor(image);
 *
 * // Run model
 * const outputs = await model(inputs);
 *
 * // Convert Tensor to RawImage
 * const output = outputs.reconstruction.squeeze().clamp_(0, 1).mul_(255).round_().to('uint8');
 * const outputImage = RawImage.fromTensor(output);
 * // RawImage {
 * //   data: Uint8Array(786432) [ 41, 31, 24, ... ],
 * //   width: 512,
 * //   height: 512,
 * //   channels: 3
 * // }
 * ```
 */
export class Swin2SRForImageSuperResolution extends Swin2SRPreTrainedModel {
}
export class DPTPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare DPT Model transformer outputting raw hidden-states without any specific head on top.
 */
export class DPTModel extends DPTPreTrainedModel {
}
/**
 * DPT Model with a depth estimation head on top (consisting of 3 convolutional layers) e.g. for KITTI, NYUv2.
 *
 * **Example:** Depth estimation w/ `Xenova/dpt-hybrid-midas`.
 * ```javascript
 * import { DPTForDepthEstimation, AutoProcessor, RawImage, interpolate_4d } from '@huggingface/transformers';
 *
 * // Load model and processor
 * const model_id = 'Xenova/dpt-hybrid-midas';
 * const model = await DPTForDepthEstimation.from_pretrained(model_id);
 * const processor = await AutoProcessor.from_pretrained(model_id);
 *
 * // Load image from URL
 * const url = 'http://images.cocodataset.org/val2017/000000039769.jpg';
 * const image = await RawImage.read(url);
 *
 * // Prepare image for the model
 * const inputs = await processor(image);
 *
 * // Run model
 * const { predicted_depth } = await model(inputs);
 *
 * // Interpolate to original size
 * const prediction = (await interpolate_4d(predicted_depth.unsqueeze(1), {
     * size: image.size.reverse(),
     * mode: 'bilinear',
 * })).squeeze(1);
 *
 * // Visualize the prediction
 * const min = prediction.min().item();
 * const max = prediction.max().item();
 * const formatted = prediction.sub_(min).div_(max - min).mul_(255).to('uint8');
 * const depth = RawImage.fromTensor(formatted);
 * // RawImage {
 * //   data: Uint8Array(307200) [ 85, 85, 84, ... ],
 * //   width: 640,
 * //   height: 480,
 * //   channels: 1
 * // }
 * ```
 */
export class DPTForDepthEstimation extends DPTPreTrainedModel {
}
export class DepthAnythingPreTrainedModel extends PreTrainedModel {
}
/**
 * Depth Anything Model with a depth estimation head on top (consisting of 3 convolutional layers) e.g. for KITTI, NYUv2.
 */
export class DepthAnythingForDepthEstimation extends DepthAnythingPreTrainedModel {
}
export class SapiensPreTrainedModel extends PreTrainedModel {
}
export class SapiensForSemanticSegmentation extends SapiensPreTrainedModel {
}
export class SapiensForDepthEstimation extends SapiensPreTrainedModel {
}
export class SapiensForNormalEstimation extends SapiensPreTrainedModel {
}
export class DepthProPreTrainedModel extends PreTrainedModel {
}
export class DepthProForDepthEstimation extends DepthProPreTrainedModel {
}
export class Metric3DPreTrainedModel extends PreTrainedModel {
}
export class Metric3DForDepthEstimation extends Metric3DPreTrainedModel {
}
export class Metric3Dv2PreTrainedModel extends PreTrainedModel {
}
export class Metric3Dv2ForDepthEstimation extends Metric3Dv2PreTrainedModel {
}
export class MaskFormerPreTrainedModel extends PreTrainedModel {
}
export class MaskFormerModel extends MaskFormerPreTrainedModel {
}
export class MaskFormerForInstanceSegmentation extends MaskFormerPreTrainedModel {
}
export class GLPNPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare GLPN encoder (Mix-Transformer) outputting raw hidden-states without any specific head on top.
 */
export class GLPNModel extends GLPNPreTrainedModel {
}
/**
 * import { GLPNForDepthEstimation, AutoProcessor, RawImage, interpolate_4d } from '@huggingface/transformers';
 *
 * // Load model and processor
 * const model_id = 'Xenova/glpn-kitti';
 * const model = await GLPNForDepthEstimation.from_pretrained(model_id);
 * const processor = await AutoProcessor.from_pretrained(model_id);
 *
 * // Load image from URL
 * const url = 'http://images.cocodataset.org/val2017/000000039769.jpg';
 * const image = await RawImage.read(url);
 *
 * // Prepare image for the model
 * const inputs = await processor(image);
 *
 * // Run model
 * const { predicted_depth } = await model(inputs);
 *
 * // Interpolate to original size
 * const prediction = (await interpolate_4d(predicted_depth.unsqueeze(1), {
     * size: image.size.reverse(),
     * mode: 'bilinear',
 * })).squeeze(1);
 *
 * // Visualize the prediction
 * const min = prediction.min().item();
 * const max = prediction.max().item();
 * const formatted = prediction.sub_(min).div_(max - min).mul_(255).to('uint8');
 * const depth = RawImage.fromTensor(formatted);
 * // RawImage {
 * //   data: Uint8Array(307200) [ 85, 85, 84, ... ],
 * //   width: 640,
 * //   height: 480,
 * //   channels: 1
 * // }
 * ```
 */
export class GLPNForDepthEstimation extends GLPNPreTrainedModel {
}
export class DonutSwinPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare Donut Swin Model transformer outputting raw hidden-states without any specific head on top.
 *
 * **Example:** Step-by-step Document Parsing.
 *
 * ```javascript
 * import { AutoProcessor, AutoTokenizer, AutoModelForVision2Seq, RawImage } from '@huggingface/transformers';
 *
 * // Choose model to use
 * const model_id = 'Xenova/donut-base-finetuned-cord-v2';
 *
 * // Prepare image inputs
 * const processor = await AutoProcessor.from_pretrained(model_id);
 * const url = 'https://huggingface.co/datasets/Xenova/transformers.js-docs/resolve/main/receipt.png';
 * const image = await RawImage.read(url);
 * const image_inputs = await processor(image);
 *
 * // Prepare decoder inputs
 * const tokenizer = await AutoTokenizer.from_pretrained(model_id);
 * const task_prompt = '<s_cord-v2>';
 * const decoder_input_ids = tokenizer(task_prompt, {
 *   add_special_tokens: false,
 * }).input_ids;
 *
 * // Create the model
 * const model = await AutoModelForVision2Seq.from_pretrained(model_id);
 *
 * // Run inference
 * const output = await model.generate(image_inputs.pixel_values, {
 *   decoder_input_ids,
 *   max_length: model.config.decoder.max_position_embeddings,
 * });
 *
 * // Decode output
 * const decoded = tokenizer.batch_decode(output)[0];
 * // <s_cord-v2><s_menu><s_nm> CINNAMON SUGAR</s_nm><s_unitprice> 17,000</s_unitprice><s_cnt> 1 x</s_cnt><s_price> 17,000</s_price></s_menu><s_sub_total><s_subtotal_price> 17,000</s_subtotal_price></s_sub_total><s_total><s_total_price> 17,000</s_total_price><s_cashprice> 20,000</s_cashprice><s_changeprice> 3,000</s_changeprice></s_total></s>
 * ```
 *
 * **Example:** Step-by-step Document Visual Question Answering (DocVQA)
 *
 * ```javascript
 * import { AutoProcessor, AutoTokenizer, AutoModelForVision2Seq, RawImage } from '@huggingface/transformers';
 *
 * // Choose model to use
 * const model_id = 'Xenova/donut-base-finetuned-docvqa';
 *
 * // Prepare image inputs
 * const processor = await AutoProcessor.from_pretrained(model_id);
 * const url = 'https://huggingface.co/datasets/Xenova/transformers.js-docs/resolve/main/invoice.png';
 * const image = await RawImage.read(url);
 * const image_inputs = await processor(image);
 *
 * // Prepare decoder inputs
 * const tokenizer = await AutoTokenizer.from_pretrained(model_id);
 * const question = 'What is the invoice number?';
 * const task_prompt = `<s_docvqa><s_question>${question}</s_question><s_answer>`;
 * const decoder_input_ids = tokenizer(task_prompt, {
 *   add_special_tokens: false,
 * }).input_ids;
 *
 * // Create the model
 * const model = await AutoModelForVision2Seq.from_pretrained(model_id);
 *
 * // Run inference
 * const output = await model.generate(image_inputs.pixel_values, {
 *   decoder_input_ids,
 *   max_length: model.config.decoder.max_position_embeddings,
 * });
 *
 * // Decode output
 * const decoded = tokenizer.batch_decode(output)[0];
 * // <s_docvqa><s_question> What is the invoice number?</s_question><s_answer> us-001</s_answer></s>
 * ```
 */
export class DonutSwinModel extends DonutSwinPreTrainedModel {
}
export class ConvNextPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare ConvNext model outputting raw features without any specific head on top.
 */
export class ConvNextModel extends ConvNextPreTrainedModel {
}
/**
 * ConvNext Model with an image classification head on top (a linear layer on top of the pooled features), e.g. for ImageNet.
 */
export class ConvNextForImageClassification extends ConvNextPreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
export class ConvNextV2PreTrainedModel extends PreTrainedModel {
}
/**
 * The bare ConvNextV2 model outputting raw features without any specific head on top.
 */
export class ConvNextV2Model extends ConvNextV2PreTrainedModel {
}
/**
 * ConvNextV2 Model with an image classification head on top (a linear layer on top of the pooled features), e.g. for ImageNet.
 */
export class ConvNextV2ForImageClassification extends ConvNextV2PreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
export class Dinov2PreTrainedModel extends PreTrainedModel {
}
/**
 * The bare DINOv2 Model transformer outputting raw hidden-states without any specific head on top.
 */
export class Dinov2Model extends Dinov2PreTrainedModel {
}
/**
 * Dinov2 Model transformer with an image classification head on top (a linear layer on top of the final hidden state of the [CLS] token) e.g. for ImageNet.
 */
export class Dinov2ForImageClassification extends Dinov2PreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
export class Dinov2WithRegistersPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare Dinov2WithRegisters Model transformer outputting raw hidden-states without any specific head on top.
 */
export class Dinov2WithRegistersModel extends Dinov2WithRegistersPreTrainedModel {
}
/**
 * Dinov2WithRegisters Model transformer with an image classification head on top (a linear layer on top of the final hidden state of the [CLS] token) e.g. for ImageNet.
 */
export class Dinov2WithRegistersForImageClassification extends Dinov2WithRegistersPreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
export class GroundingDinoPreTrainedModel extends PreTrainedModel {
}
export class GroundingDinoForObjectDetection extends GroundingDinoPreTrainedModel {
}
export class YolosPreTrainedModel extends PreTrainedModel {
}
export class YolosModel extends YolosPreTrainedModel {
}
export class YolosForObjectDetection extends YolosPreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<YolosObjectDetectionOutput>;
}
export class YolosObjectDetectionOutput extends ModelOutput {
    /**
     * @param {Object} output The output of the model.
     * @param {Tensor} output.logits Classification logits (including no-object) for all queries.
     * @param {Tensor} output.pred_boxes Normalized boxes coordinates for all queries, represented as (center_x, center_y, width, height).
     * These values are normalized in [0, 1], relative to the size of each individual image in the batch (disregarding possible padding).
     */
    constructor({ logits, pred_boxes }: {
        logits: Tensor;
        pred_boxes: Tensor;
    });
    logits: Tensor;
    pred_boxes: Tensor;
}
export class SamPreTrainedModel extends PreTrainedModel {
}
/**
 * Segment Anything Model (SAM) for generating segmentation masks, given an input image
 * and optional 2D location and bounding boxes.
 *
 * **Example:** Perform mask generation w/ `Xenova/sam-vit-base`.
 * ```javascript
 * import { SamModel, AutoProcessor, RawImage } from '@huggingface/transformers';
 *
 * const model = await SamModel.from_pretrained('Xenova/sam-vit-base');
 * const processor = await AutoProcessor.from_pretrained('Xenova/sam-vit-base');
 *
 * const img_url = 'https://huggingface.co/ybelkada/segment-anything/resolve/main/assets/car.png';
 * const raw_image = await RawImage.read(img_url);
 * const input_points = [[[450, 600]]] // 2D localization of a window
 *
 * const inputs = await processor(raw_image, { input_points });
 * const outputs = await model(inputs);
 *
 * const masks = await processor.post_process_masks(outputs.pred_masks, inputs.original_sizes, inputs.reshaped_input_sizes);
 * // [
 * //   Tensor {
 * //     dims: [ 1, 3, 1764, 2646 ],
 * //     type: 'bool',
 * //     data: Uint8Array(14002632) [ ... ],
 * //     size: 14002632
 * //   }
 * // ]
 * const scores = outputs.iou_scores;
 * // Tensor {
 * //   dims: [ 1, 1, 3 ],
 * //   type: 'float32',
 * //   data: Float32Array(3) [
 * //     0.8892380595207214,
 * //     0.9311248064041138,
 * //     0.983696699142456
 * //   ],
 * //   size: 3
 * // }
 * ```
 */
export class SamModel extends SamPreTrainedModel {
    /**
     * Compute image embeddings and positional image embeddings, given the pixel values of an image.
     * @param {Object} model_inputs Object containing the model inputs.
     * @param {Tensor} model_inputs.pixel_values Pixel values obtained using a `SamProcessor`.
     * @returns {Promise<{ image_embeddings: Tensor, image_positional_embeddings: Tensor }>} The image embeddings and positional image embeddings.
     */
    get_image_embeddings({ pixel_values }: {
        pixel_values: Tensor;
    }): Promise<{
        image_embeddings: Tensor;
        image_positional_embeddings: Tensor;
    }>;
    /**
     * @typedef {Object} SamModelInputs Object containing the model inputs.
     * @property {Tensor} pixel_values Pixel values as a Tensor with shape `(batch_size, num_channels, height, width)`.
     * These can be obtained using a `SamProcessor`.
     * @property {Tensor} [input_points] Input 2D spatial points with shape `(batch_size, num_points, 2)`.
     * This is used by the prompt encoder to encode the prompt.
     * @property {Tensor} [input_labels] Input labels for the points, as a Tensor of shape `(batch_size, point_batch_size, num_points)`.
     * This is used by the prompt encoder to encode the prompt. There are 4 types of labels:
     *  - `1`: the point is a point that contains the object of interest
     *  - `0`: the point is a point that does not contain the object of interest
     *  - `-1`: the point corresponds to the background
     *  - `-10`: the point is a padding point, thus should be ignored by the prompt encoder
     * @property {Tensor} [input_boxes] Input bounding boxes with shape `(batch_size, num_boxes, 4)`.
     * @property {Tensor} [image_embeddings] Image embeddings used by the mask decoder.
     * @property {Tensor} [image_positional_embeddings] Image positional embeddings used by the mask decoder.
     */
    /**
     * @param {SamModelInputs} model_inputs Object containing the model inputs.
     * @returns {Promise<Object>} The output of the model.
     */
    forward(model_inputs: {
        /**
         * Pixel values as a Tensor with shape `(batch_size, num_channels, height, width)`.
         * These can be obtained using a `SamProcessor`.
         */
        pixel_values: Tensor;
        /**
         * Input 2D spatial points with shape `(batch_size, num_points, 2)`.
         * This is used by the prompt encoder to encode the prompt.
         */
        input_points?: Tensor;
        /**
         * Input labels for the points, as a Tensor of shape `(batch_size, point_batch_size, num_points)`.
         * This is used by the prompt encoder to encode the prompt. There are 4 types of labels:
         * - `1`: the point is a point that contains the object of interest
         * - `0`: the point is a point that does not contain the object of interest
         * - `-1`: the point corresponds to the background
         * - `-10`: the point is a padding point, thus should be ignored by the prompt encoder
         */
        input_labels?: Tensor;
        /**
         * Input bounding boxes with shape `(batch_size, num_boxes, 4)`.
         */
        input_boxes?: Tensor;
        /**
         * Image embeddings used by the mask decoder.
         */
        image_embeddings?: Tensor;
        /**
         * Image positional embeddings used by the mask decoder.
         */
        image_positional_embeddings?: Tensor;
    }): Promise<any>;
    /**
     * Runs the model with the provided inputs
     * @param {Object} model_inputs Model inputs
     * @returns {Promise<SamImageSegmentationOutput>} Object containing segmentation outputs
     */
    _call(model_inputs: any): Promise<SamImageSegmentationOutput>;
}
/**
 * Base class for Segment-Anything model's output.
 */
export class SamImageSegmentationOutput extends ModelOutput {
    /**
     * @param {Object} output The output of the model.
     * @param {Tensor} output.iou_scores The output logits of the model.
     * @param {Tensor} output.pred_masks Predicted boxes.
     */
    constructor({ iou_scores, pred_masks }: {
        iou_scores: Tensor;
        pred_masks: Tensor;
    });
    iou_scores: Tensor;
    pred_masks: Tensor;
}
export class MarianPreTrainedModel extends PreTrainedModel {
}
export class MarianModel extends MarianPreTrainedModel {
}
export class MarianMTModel extends MarianPreTrainedModel {
}
export class M2M100PreTrainedModel extends PreTrainedModel {
}
export class M2M100Model extends M2M100PreTrainedModel {
}
export class M2M100ForConditionalGeneration extends M2M100PreTrainedModel {
}
export class Wav2Vec2PreTrainedModel extends PreTrainedModel {
}
/**
 * The bare Wav2Vec2 Model transformer outputting raw hidden-states without any specific head on top.
 *
 * **Example:** Load and run a `Wav2Vec2Model` for feature extraction.
 *
 * ```javascript
 * import { AutoProcessor, AutoModel, read_audio } from '@huggingface/transformers';
 *
 * // Read and preprocess audio
 * const processor = await AutoProcessor.from_pretrained('Xenova/mms-300m');
 * const audio = await read_audio('https://huggingface.co/datasets/Narsil/asr_dummy/resolve/main/mlk.flac', 16000);
 * const inputs = await processor(audio);
 *
 * // Run model with inputs
 * const model = await AutoModel.from_pretrained('Xenova/mms-300m');
 * const output = await model(inputs);
 * // {
 * //   last_hidden_state: Tensor {
 * //     dims: [ 1, 1144, 1024 ],
 * //     type: 'float32',
 * //     data: Float32Array(1171456) [ ... ],
 * //     size: 1171456
 * //   }
 * // }
 * ```
 */
export class Wav2Vec2Model extends Wav2Vec2PreTrainedModel {
}
export class Wav2Vec2ForCTC extends Wav2Vec2PreTrainedModel {
    /**
     * @param {Object} model_inputs
     * @param {Tensor} model_inputs.input_values Float values of input raw speech waveform.
     * @param {Tensor} model_inputs.attention_mask Mask to avoid performing convolution and attention on padding token indices. Mask values selected in [0, 1]
     */
    _call(model_inputs: {
        input_values: Tensor;
        attention_mask: Tensor;
    }): Promise<CausalLMOutput>;
}
export class Wav2Vec2ForSequenceClassification extends Wav2Vec2PreTrainedModel {
    /**
     * Calls the model on new inputs.
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<SequenceClassifierOutput>} An object containing the model's output logits for sequence classification.
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
/**
 * Wav2Vec2 Model with a frame classification head on top for tasks like Speaker Diarization.
 */
export class Wav2Vec2ForAudioFrameClassification extends Wav2Vec2PreTrainedModel {
    /**
     * Calls the model on new inputs.
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<TokenClassifierOutput>} An object containing the model's output logits for sequence classification.
     */
    _call(model_inputs: any): Promise<TokenClassifierOutput>;
}
export class PyAnnotePreTrainedModel extends PreTrainedModel {
}
/**
 * The bare PyAnnote Model transformer outputting raw hidden-states without any specific head on top.
 */
export class PyAnnoteModel extends PyAnnotePreTrainedModel {
}
/**
 * PyAnnote Model with a frame classification head on top for tasks like Speaker Diarization.
 *
 * **Example:** Load and run a `PyAnnoteForAudioFrameClassification` for speaker diarization.
 *
 * ```javascript
 * import { AutoProcessor, AutoModelForAudioFrameClassification, read_audio } from '@huggingface/transformers';
 *
 * // Load model and processor
 * const model_id = 'onnx-community/pyannote-segmentation-3.0';
 * const model = await AutoModelForAudioFrameClassification.from_pretrained(model_id);
 * const processor = await AutoProcessor.from_pretrained(model_id);
 *
 * // Read and preprocess audio
 * const url = 'https://huggingface.co/datasets/Xenova/transformers.js-docs/resolve/main/mlk.wav';
 * const audio = await read_audio(url, processor.feature_extractor.config.sampling_rate);
 * const inputs = await processor(audio);
 *
 * // Run model with inputs
 * const { logits } = await model(inputs);
 * // {
 * //   logits: Tensor {
 * //     dims: [ 1, 767, 7 ],  // [batch_size, num_frames, num_classes]
 * //     type: 'float32',
 * //     data: Float32Array(5369) [ ... ],
 * //     size: 5369
 * //   }
 * // }
 *
 * const result = processor.post_process_speaker_diarization(logits, audio.length);
 * // [
 * //   [
 * //     { id: 0, start: 0, end: 1.0512535626298245, confidence: 0.8220156481664611 },
 * //     { id: 2, start: 1.0512535626298245, end: 2.3398869619825127, confidence: 0.9008811707860472 },
 * //     ...
 * //   ]
 * // ]
 *
 * // Display result
 * console.table(result[0], ['start', 'end', 'id', 'confidence']);
 * // ┌─────────┬────────────────────┬────────────────────┬────┬─────────────────────┐
 * // │ (index) │ start              │ end                │ id │ confidence          │
 * // ├─────────┼────────────────────┼────────────────────┼────┼─────────────────────┤
 * // │ 0       │ 0                  │ 1.0512535626298245 │ 0  │ 0.8220156481664611  │
 * // │ 1       │ 1.0512535626298245 │ 2.3398869619825127 │ 2  │ 0.9008811707860472  │
 * // │ 2       │ 2.3398869619825127 │ 3.5946089560890773 │ 0  │ 0.7521651315796233  │
 * // │ 3       │ 3.5946089560890773 │ 4.578039708226655  │ 2  │ 0.8491978128022479  │
 * // │ 4       │ 4.578039708226655  │ 4.594995410849717  │ 0  │ 0.2935352600416393  │
 * // │ 5       │ 4.594995410849717  │ 6.121008646925269  │ 3  │ 0.6788051309866024  │
 * // │ 6       │ 6.121008646925269  │ 6.256654267909762  │ 0  │ 0.37125512393851134 │
 * // │ 7       │ 6.256654267909762  │ 8.630452635138397  │ 2  │ 0.7467035186353542  │
 * // │ 8       │ 8.630452635138397  │ 10.088643060721703 │ 0  │ 0.7689364814666032  │
 * // │ 9       │ 10.088643060721703 │ 12.58113134631177  │ 2  │ 0.9123324509131324  │
 * // │ 10      │ 12.58113134631177  │ 13.005023911888312 │ 0  │ 0.4828358177572041  │
 * // └─────────┴────────────────────┴────────────────────┴────┴─────────────────────┘
 * ```
 */
export class PyAnnoteForAudioFrameClassification extends PyAnnotePreTrainedModel {
    /**
     * Calls the model on new inputs.
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<TokenClassifierOutput>} An object containing the model's output logits for sequence classification.
     */
    _call(model_inputs: any): Promise<TokenClassifierOutput>;
}
export class WeSpeakerResNetPreTrainedModel extends PreTrainedModel {
}
export class WeSpeakerResNetModel extends WeSpeakerResNetPreTrainedModel {
}
export class UniSpeechPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare UniSpeech Model transformer outputting raw hidden-states without any specific head on top.
 */
export class UniSpeechModel extends UniSpeechPreTrainedModel {
}
/**
 * UniSpeech Model with a `language modeling` head on top for Connectionist Temporal Classification (CTC).
 */
export class UniSpeechForCTC extends UniSpeechPreTrainedModel {
    /**
     * @param {Object} model_inputs
     * @param {Tensor} model_inputs.input_values Float values of input raw speech waveform.
     * @param {Tensor} model_inputs.attention_mask Mask to avoid performing convolution and attention on padding token indices. Mask values selected in [0, 1]
     */
    _call(model_inputs: {
        input_values: Tensor;
        attention_mask: Tensor;
    }): Promise<CausalLMOutput>;
}
/**
 * UniSpeech Model with a sequence classification head on top (a linear layer over the pooled output).
 */
export class UniSpeechForSequenceClassification extends UniSpeechPreTrainedModel {
    /**
     * Calls the model on new inputs.
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<SequenceClassifierOutput>} An object containing the model's output logits for sequence classification.
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
export class UniSpeechSatPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare UniSpeechSat Model transformer outputting raw hidden-states without any specific head on top.
 */
export class UniSpeechSatModel extends UniSpeechSatPreTrainedModel {
}
/**
 * UniSpeechSat Model with a `language modeling` head on top for Connectionist Temporal Classification (CTC).
 */
export class UniSpeechSatForCTC extends UniSpeechSatPreTrainedModel {
    /**
     * @param {Object} model_inputs
     * @param {Tensor} model_inputs.input_values Float values of input raw speech waveform.
     * @param {Tensor} model_inputs.attention_mask Mask to avoid performing convolution and attention on padding token indices. Mask values selected in [0, 1]
     */
    _call(model_inputs: {
        input_values: Tensor;
        attention_mask: Tensor;
    }): Promise<CausalLMOutput>;
}
/**
 * UniSpeechSat Model with a sequence classification head on top (a linear layer over the pooled output).
 */
export class UniSpeechSatForSequenceClassification extends UniSpeechSatPreTrainedModel {
    /**
     * Calls the model on new inputs.
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<SequenceClassifierOutput>} An object containing the model's output logits for sequence classification.
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
/**
 * UniSpeechSat Model with a frame classification head on top for tasks like Speaker Diarization.
 */
export class UniSpeechSatForAudioFrameClassification extends UniSpeechSatPreTrainedModel {
    /**
     * Calls the model on new inputs.
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<TokenClassifierOutput>} An object containing the model's output logits for sequence classification.
     */
    _call(model_inputs: any): Promise<TokenClassifierOutput>;
}
export class Wav2Vec2BertPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare Wav2Vec2Bert Model transformer outputting raw hidden-states without any specific head on top.
 */
export class Wav2Vec2BertModel extends Wav2Vec2BertPreTrainedModel {
}
/**
 * Wav2Vec2Bert Model with a `language modeling` head on top for Connectionist Temporal Classification (CTC).
 */
export class Wav2Vec2BertForCTC extends Wav2Vec2BertPreTrainedModel {
    /**
     * @param {Object} model_inputs
     * @param {Tensor} model_inputs.input_features Float values of input mel-spectrogram.
     * @param {Tensor} model_inputs.attention_mask Mask to avoid performing convolution and attention on padding token indices. Mask values selected in [0, 1]
     */
    _call(model_inputs: {
        input_features: Tensor;
        attention_mask: Tensor;
    }): Promise<CausalLMOutput>;
}
/**
 * Wav2Vec2Bert Model with a sequence classification head on top (a linear layer over the pooled output).
 */
export class Wav2Vec2BertForSequenceClassification extends Wav2Vec2BertPreTrainedModel {
    /**
     * Calls the model on new inputs.
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<SequenceClassifierOutput>} An object containing the model's output logits for sequence classification.
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
export class HubertPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare Hubert Model transformer outputting raw hidden-states without any specific head on top.
 *
 * **Example:** Load and run a `HubertModel` for feature extraction.
 *
 * ```javascript
 * import { AutoProcessor, AutoModel, read_audio } from '@huggingface/transformers';
 *
 * // Read and preprocess audio
 * const processor = await AutoProcessor.from_pretrained('Xenova/hubert-base-ls960');
 * const audio = await read_audio('https://huggingface.co/datasets/Xenova/transformers.js-docs/resolve/main/jfk.wav', 16000);
 * const inputs = await processor(audio);
 *
 * // Load and run model with inputs
 * const model = await AutoModel.from_pretrained('Xenova/hubert-base-ls960');
 * const output = await model(inputs);
 * // {
 * //   last_hidden_state: Tensor {
 * //     dims: [ 1, 549, 768 ],
 * //     type: 'float32',
 * //     data: Float32Array(421632) [0.0682469978928566, 0.08104046434164047, -0.4975186586380005, ...],
 * //     size: 421632
 * //   }
 * // }
 * ```
 */
export class HubertModel extends Wav2Vec2PreTrainedModel {
}
/**
 * Hubert Model with a `language modeling` head on top for Connectionist Temporal Classification (CTC).
 */
export class HubertForCTC extends Wav2Vec2PreTrainedModel {
    /**
     * @param {Object} model_inputs
     * @param {Tensor} model_inputs.input_values Float values of input raw speech waveform.
     * @param {Tensor} model_inputs.attention_mask Mask to avoid performing convolution and attention on padding token indices. Mask values selected in [0, 1]
     */
    _call(model_inputs: {
        input_values: Tensor;
        attention_mask: Tensor;
    }): Promise<CausalLMOutput>;
}
/**
 * Hubert Model with a sequence classification head on top (a linear layer over the pooled output) for tasks like SUPERB Keyword Spotting.
 */
export class HubertForSequenceClassification extends Wav2Vec2PreTrainedModel {
    /**
     * Calls the model on new inputs.
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<SequenceClassifierOutput>} An object containing the model's output logits for sequence classification.
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
/**
 * An abstract class to handle weights initialization and a simple interface for downloading and loading pretrained models.
 */
export class WavLMPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare WavLM Model transformer outputting raw hidden-states without any specific head on top.
 *
 * **Example:** Load and run a `WavLMModel` for feature extraction.
 *
 * ```javascript
 * import { AutoProcessor, AutoModel, read_audio } from '@huggingface/transformers';
 *
 * // Read and preprocess audio
 * const processor = await AutoProcessor.from_pretrained('Xenova/wavlm-base');
 * const audio = await read_audio('https://huggingface.co/datasets/Xenova/transformers.js-docs/resolve/main/jfk.wav', 16000);
 * const inputs = await processor(audio);
 *
 * // Run model with inputs
 * const model = await AutoModel.from_pretrained('Xenova/wavlm-base');
 * const output = await model(inputs);
 * // {
 * //   last_hidden_state: Tensor {
 * //     dims: [ 1, 549, 768 ],
 * //     type: 'float32',
 * //     data: Float32Array(421632) [-0.349443256855011, -0.39341306686401367,  0.022836603224277496, ...],
 * //     size: 421632
 * //   }
 * // }
 * ```
 */
export class WavLMModel extends WavLMPreTrainedModel {
}
/**
 * WavLM Model with a `language modeling` head on top for Connectionist Temporal Classification (CTC).
 */
export class WavLMForCTC extends WavLMPreTrainedModel {
    /**
     * @param {Object} model_inputs
     * @param {Tensor} model_inputs.input_values Float values of input raw speech waveform.
     * @param {Tensor} model_inputs.attention_mask Mask to avoid performing convolution and attention on padding token indices. Mask values selected in [0, 1]
     */
    _call(model_inputs: {
        input_values: Tensor;
        attention_mask: Tensor;
    }): Promise<CausalLMOutput>;
}
/**
 * WavLM Model with a sequence classification head on top (a linear layer over the pooled output).
 */
export class WavLMForSequenceClassification extends WavLMPreTrainedModel {
    /**
     * Calls the model on new inputs.
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<SequenceClassifierOutput>} An object containing the model's output logits for sequence classification.
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
/**
 * WavLM Model with an XVector feature extraction head on top for tasks like Speaker Verification.
 *
 * **Example:** Extract speaker embeddings with `WavLMForXVector`.
 * ```javascript
 * import { AutoProcessor, AutoModel, read_audio } from '@huggingface/transformers';
 *
 * // Read and preprocess audio
 * const processor = await AutoProcessor.from_pretrained('Xenova/wavlm-base-plus-sv');
 * const url = 'https://huggingface.co/datasets/Xenova/transformers.js-docs/resolve/main/jfk.wav';
 * const audio = await read_audio(url, 16000);
 * const inputs = await processor(audio);
 *
 * // Run model with inputs
 * const model = await AutoModel.from_pretrained('Xenova/wavlm-base-plus-sv');
 * const outputs = await model(inputs);
 * // {
 * //   logits: Tensor {
 * //     dims: [ 1, 512 ],
 * //     type: 'float32',
 * //     data: Float32Array(512) [0.5847219228744507, ...],
 * //     size: 512
 * //   },
 * //   embeddings: Tensor {
 * //     dims: [ 1, 512 ],
 * //     type: 'float32',
 * //     data: Float32Array(512) [-0.09079201519489288, ...],
 * //     size: 512
 * //   }
 * // }
 * ```
 */
export class WavLMForXVector extends WavLMPreTrainedModel {
    /**
     * Calls the model on new inputs.
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<XVectorOutput>} An object containing the model's output logits and speaker embeddings.
     */
    _call(model_inputs: any): Promise<XVectorOutput>;
}
/**
 * WavLM Model with a frame classification head on top for tasks like Speaker Diarization.
 *
 * **Example:** Perform speaker diarization with `WavLMForAudioFrameClassification`.
 * ```javascript
 * import { AutoProcessor, AutoModelForAudioFrameClassification, read_audio } from '@huggingface/transformers';
 *
 * // Read and preprocess audio
 * const processor = await AutoProcessor.from_pretrained('Xenova/wavlm-base-plus-sd');
 * const url = 'https://huggingface.co/datasets/Xenova/transformers.js-docs/resolve/main/jfk.wav';
 * const audio = await read_audio(url, 16000);
 * const inputs = await processor(audio);
 *
 * // Run model with inputs
 * const model = await AutoModelForAudioFrameClassification.from_pretrained('Xenova/wavlm-base-plus-sd');
 * const { logits } = await model(inputs);
 * // {
 * //   logits: Tensor {
 * //     dims: [ 1, 549, 2 ],  // [batch_size, num_frames, num_speakers]
 * //     type: 'float32',
 * //     data: Float32Array(1098) [-3.5301010608673096, ...],
 * //     size: 1098
 * //   }
 * // }
 *
 * const labels = logits[0].sigmoid().tolist().map(
 *     frames => frames.map(speaker => speaker > 0.5 ? 1 : 0)
 * );
 * console.log(labels); // labels is a one-hot array of shape (num_frames, num_speakers)
 * // [
 * //     [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0],
 * //     [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0],
 * //     [0, 0], [0, 1], [0, 1], [0, 1], [0, 1], [0, 1],
 * //     ...
 * // ]
 * ```
 */
export class WavLMForAudioFrameClassification extends WavLMPreTrainedModel {
    /**
     * Calls the model on new inputs.
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<TokenClassifierOutput>} An object containing the model's output logits for sequence classification.
     */
    _call(model_inputs: any): Promise<TokenClassifierOutput>;
}
export class StyleTextToSpeech2PreTrainedModel extends PreTrainedModel {
}
export class StyleTextToSpeech2Model extends StyleTextToSpeech2PreTrainedModel {
}
/**
 * An abstract class to handle weights initialization and a simple interface for downloading and loading pretrained models.
 */
export class SpeechT5PreTrainedModel extends PreTrainedModel {
}
/**
 * The bare SpeechT5 Encoder-Decoder Model outputting raw hidden-states without any specific pre- or post-nets.
 */
export class SpeechT5Model extends SpeechT5PreTrainedModel {
}
/**
 * SpeechT5 Model with a speech encoder and a text decoder.
 *
 * **Example:** Generate speech from text with `SpeechT5ForSpeechToText`.
 * ```javascript
 * import { AutoTokenizer, AutoProcessor, SpeechT5ForTextToSpeech, SpeechT5HifiGan, Tensor } from '@huggingface/transformers';
 *
 * // Load the tokenizer and processor
 * const tokenizer = await AutoTokenizer.from_pretrained('Xenova/speecht5_tts');
 * const processor = await AutoProcessor.from_pretrained('Xenova/speecht5_tts');
 *
 * // Load the models
 * // NOTE: We use the full-precision versions as they are more accurate
 * const model = await SpeechT5ForTextToSpeech.from_pretrained('Xenova/speecht5_tts', { dtype: 'fp32' });
 * const vocoder = await SpeechT5HifiGan.from_pretrained('Xenova/speecht5_hifigan', { dtype: 'fp32' });
 *
 * // Load speaker embeddings from URL
 * const speaker_embeddings_data = new Float32Array(
 *     await (await fetch('https://huggingface.co/datasets/Xenova/transformers.js-docs/resolve/main/speaker_embeddings.bin')).arrayBuffer()
 * );
 * const speaker_embeddings = new Tensor(
 *     'float32',
 *     speaker_embeddings_data,
 *     [1, speaker_embeddings_data.length]
 * )
 *
 * // Run tokenization
 * const { input_ids } = tokenizer('Hello, my dog is cute');
 *
 * // Generate waveform
 * const { waveform } = await model.generate_speech(input_ids, speaker_embeddings, { vocoder });
 * console.log(waveform)
 * // Tensor {
 * //   dims: [ 26112 ],
 * //   type: 'float32',
 * //   size: 26112,
 * //   data: Float32Array(26112) [ -0.00043630177970044315, -0.00018082228780258447, ... ],
 * // }
 * ```
 */
export class SpeechT5ForSpeechToText extends SpeechT5PreTrainedModel {
}
/**
 * SpeechT5 Model with a text encoder and a speech decoder.
 */
export class SpeechT5ForTextToSpeech extends SpeechT5PreTrainedModel {
    /**
     * @typedef {Object} SpeechOutput
     * @property {Tensor} [spectrogram] The predicted log-mel spectrogram of shape
     * `(output_sequence_length, config.num_mel_bins)`. Returned when no `vocoder` is provided
     * @property {Tensor} [waveform] The predicted waveform of shape `(num_frames,)`. Returned when a `vocoder` is provided.
     * @property {Tensor} [cross_attentions] The outputs of the decoder's cross-attention layers of shape
     * `(config.decoder_layers, config.decoder_attention_heads, output_sequence_length, input_sequence_length)`. returned when `output_cross_attentions` is `true`.
     */
    /**
     * Converts a sequence of input tokens into a sequence of mel spectrograms, which are subsequently turned into a speech waveform using a vocoder.
     * @param {Tensor} input_values Indices of input sequence tokens in the vocabulary.
     * @param {Tensor} speaker_embeddings Tensor containing the speaker embeddings.
     * @param {Object} options Optional parameters for generating speech.
     * @param {number} [options.threshold=0.5] The generated sequence ends when the predicted stop token probability exceeds this value.
     * @param {number} [options.minlenratio=0.0] Used to calculate the minimum required length for the output sequence.
     * @param {number} [options.maxlenratio=20.0] Used to calculate the maximum allowed length for the output sequence.
     * @param {Object} [options.vocoder=null] The vocoder that converts the mel spectrogram into a speech waveform. If `null`, the output is the mel spectrogram.
     * @param {boolean} [options.output_cross_attentions=false] Whether or not to return the attentions tensors of the decoder's cross-attention layers.
     * @returns {Promise<SpeechOutput>} A promise which resolves to an object containing the spectrogram, waveform, and cross-attention tensors.
     */
    generate_speech(input_values: Tensor, speaker_embeddings: Tensor, { threshold, minlenratio, maxlenratio, vocoder, }?: {
        threshold?: number;
        minlenratio?: number;
        maxlenratio?: number;
        vocoder?: any;
        output_cross_attentions?: boolean;
    }): Promise<{
        /**
         * The predicted log-mel spectrogram of shape
         * `(output_sequence_length, config.num_mel_bins)`. Returned when no `vocoder` is provided
         */
        spectrogram?: Tensor;
        /**
         * The predicted waveform of shape `(num_frames,)`. Returned when a `vocoder` is provided.
         */
        waveform?: Tensor;
        /**
         * The outputs of the decoder's cross-attention layers of shape
         * `(config.decoder_layers, config.decoder_attention_heads, output_sequence_length, input_sequence_length)`. returned when `output_cross_attentions` is `true`.
         */
        cross_attentions?: Tensor;
    }>;
}
/**
 * HiFi-GAN vocoder.
 *
 * See [SpeechT5ForSpeechToText](./models#module_models.SpeechT5ForSpeechToText) for example usage.
 */
export class SpeechT5HifiGan extends PreTrainedModel {
}
export class TrOCRPreTrainedModel extends PreTrainedModel {
}
/**
 * The TrOCR Decoder with a language modeling head.
 */
export class TrOCRForCausalLM extends TrOCRPreTrainedModel {
}
/**
 * The bare Mistral Model outputting raw hidden-states without any specific head on top.
 */
export class MistralPreTrainedModel extends PreTrainedModel {
}
export class MistralModel extends MistralPreTrainedModel {
}
export class MistralForCausalLM extends MistralPreTrainedModel {
}
/**
 * The bare Starcoder2 Model outputting raw hidden-states without any specific head on top.
 */
export class Starcoder2PreTrainedModel extends PreTrainedModel {
}
export class Starcoder2Model extends Starcoder2PreTrainedModel {
}
export class Starcoder2ForCausalLM extends Starcoder2PreTrainedModel {
}
/**
 * The bare Falcon Model outputting raw hidden-states without any specific head on top.
 */
export class FalconPreTrainedModel extends PreTrainedModel {
}
export class FalconModel extends FalconPreTrainedModel {
}
export class FalconForCausalLM extends FalconPreTrainedModel {
}
export class ClapPreTrainedModel extends PreTrainedModel {
}
export class ClapModel extends ClapPreTrainedModel {
}
/**
 * CLAP Text Model with a projection layer on top (a linear layer on top of the pooled output).
 *
 * **Example:** Compute text embeddings with `ClapTextModelWithProjection`.
 *
 * ```javascript
 * import { AutoTokenizer, ClapTextModelWithProjection } from '@huggingface/transformers';
 *
 * // Load tokenizer and text model
 * const tokenizer = await AutoTokenizer.from_pretrained('Xenova/clap-htsat-unfused');
 * const text_model = await ClapTextModelWithProjection.from_pretrained('Xenova/clap-htsat-unfused');
 *
 * // Run tokenization
 * const texts = ['a sound of a cat', 'a sound of a dog'];
 * const text_inputs = tokenizer(texts, { padding: true, truncation: true });
 *
 * // Compute embeddings
 * const { text_embeds } = await text_model(text_inputs);
 * // Tensor {
 * //   dims: [ 2, 512 ],
 * //   type: 'float32',
 * //   data: Float32Array(1024) [ ... ],
 * //   size: 1024
 * // }
 * ```
 */
export class ClapTextModelWithProjection extends ClapPreTrainedModel {
}
/**
 * CLAP Audio Model with a projection layer on top (a linear layer on top of the pooled output).
 *
 * **Example:** Compute audio embeddings with `ClapAudioModelWithProjection`.
 *
 * ```javascript
 * import { AutoProcessor, ClapAudioModelWithProjection, read_audio } from '@huggingface/transformers';
 *
 * // Load processor and audio model
 * const processor = await AutoProcessor.from_pretrained('Xenova/clap-htsat-unfused');
 * const audio_model = await ClapAudioModelWithProjection.from_pretrained('Xenova/clap-htsat-unfused');
 *
 * // Read audio and run processor
 * const audio = await read_audio('https://huggingface.co/datasets/Xenova/transformers.js-docs/resolve/main/cat_meow.wav');
 * const audio_inputs = await processor(audio);
 *
 * // Compute embeddings
 * const { audio_embeds } = await audio_model(audio_inputs);
 * // Tensor {
 * //   dims: [ 1, 512 ],
 * //   type: 'float32',
 * //   data: Float32Array(512) [ ... ],
 * //   size: 512
 * // }
 * ```
 */
export class ClapAudioModelWithProjection extends ClapPreTrainedModel {
}
export class VitsPreTrainedModel extends PreTrainedModel {
}
/**
 * The complete VITS model, for text-to-speech synthesis.
 *
 * **Example:** Generate speech from text with `VitsModel`.
 * ```javascript
 * import { AutoTokenizer, VitsModel } from '@huggingface/transformers';
 *
 * // Load the tokenizer and model
 * const tokenizer = await AutoTokenizer.from_pretrained('Xenova/mms-tts-eng');
 * const model = await VitsModel.from_pretrained('Xenova/mms-tts-eng');
 *
 * // Run tokenization
 * const inputs = tokenizer('I love transformers');
 *
 * // Generate waveform
 * const { waveform } = await model(inputs);
 * // Tensor {
 * //   dims: [ 1, 35328 ],
 * //   type: 'float32',
 * //   data: Float32Array(35328) [ ... ],
 * //   size: 35328,
 * // }
 * ```
 */
export class VitsModel extends VitsPreTrainedModel {
    /**
     * Calls the model on new inputs.
     * @param {Object} model_inputs The inputs to the model.
     * @returns {Promise<VitsModelOutput>} The outputs for the VITS model.
     */
    _call(model_inputs: any): Promise<VitsModelOutput>;
}
export class SegformerPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare SegFormer encoder (Mix-Transformer) outputting raw hidden-states without any specific head on top.
 */
export class SegformerModel extends SegformerPreTrainedModel {
}
/**
 * SegFormer Model transformer with an image classification head on top (a linear layer on top of the final hidden states) e.g. for ImageNet.
 */
export class SegformerForImageClassification extends SegformerPreTrainedModel {
}
/**
 * SegFormer Model transformer with an all-MLP decode head on top e.g. for ADE20k, CityScapes.
 */
export class SegformerForSemanticSegmentation extends SegformerPreTrainedModel {
}
export class StableLmPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare StableLm Model transformer outputting raw hidden-states without any specific head on top.
 */
export class StableLmModel extends StableLmPreTrainedModel {
}
/**
 * StableLm Model with a `language modeling` head on top for Causal Language Modeling (with past).
 */
export class StableLmForCausalLM extends StableLmPreTrainedModel {
}
export class EfficientNetPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare EfficientNet model outputting raw features without any specific head on top.
 */
export class EfficientNetModel extends EfficientNetPreTrainedModel {
}
/**
 * EfficientNet Model with an image classification head on top (a linear layer on top of the pooled features).
 */
export class EfficientNetForImageClassification extends EfficientNetPreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
export class MusicgenPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare Musicgen decoder model outputting raw hidden-states without any specific head on top.
 */
export class MusicgenModel extends MusicgenPreTrainedModel {
}
/**
 * The MusicGen decoder model with a language modelling head on top.
 */
export class MusicgenForCausalLM extends MusicgenPreTrainedModel {
}
/**
 * The composite MusicGen model with a text encoder, audio encoder and Musicgen decoder,
 * for music generation tasks with one or both of text and audio prompts.
 *
 * **Example:** Generate music from text with `Xenova/musicgen-small`.
 * ```javascript
 * import { AutoTokenizer, MusicgenForConditionalGeneration } from '@huggingface/transformers';
 *
 * // Load tokenizer and model
 * const tokenizer = await AutoTokenizer.from_pretrained('Xenova/musicgen-small');
 * const model = await MusicgenForConditionalGeneration.from_pretrained(
 *   'Xenova/musicgen-small', { dtype: 'fp32' }
 * );
 *
 * // Prepare text input
 * const prompt = '80s pop track with bassy drums and synth';
 * const inputs = tokenizer(prompt);
 *
 * // Generate audio
 * const audio_values = await model.generate({
 *   ...inputs,
 *   max_new_tokens: 512,
 *   do_sample: true,
 *   guidance_scale: 3,
 * });
 *
 * // (Optional) Write the output to a WAV file
 * import wavefile from 'wavefile';
 * import fs from 'fs';
 *
 * const wav = new wavefile.WaveFile();
 * wav.fromScratch(1, model.config.audio_encoder.sampling_rate, '32f', audio_values.data);
 * fs.writeFileSync('musicgen_out.wav', wav.toBuffer());
 * ```
 */
export class MusicgenForConditionalGeneration extends PreTrainedModel {
    /**
     * Apply the pattern mask to the final ids,
     * then revert the pattern delay mask by filtering the pad token id in a single step.
     * @param {Tensor} outputs The output tensor from the model.
     * @returns {Tensor} The filtered output tensor.
     */
    _apply_and_filter_by_delay_pattern_mask(outputs: Tensor): Tensor;
    prepare_inputs_for_generation(input_ids: any, model_inputs: any, generation_config: any): any;
}
export class MobileNetV1PreTrainedModel extends PreTrainedModel {
}
/**
 * The bare MobileNetV1 model outputting raw hidden-states without any specific head on top.
 */
export class MobileNetV1Model extends MobileNetV1PreTrainedModel {
}
/**
 * MobileNetV1 model with an image classification head on top (a linear layer on top of the pooled features),
 * e.g. for ImageNet.
 */
export class MobileNetV1ForImageClassification extends MobileNetV1PreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
export class MobileNetV1ForSemanticSegmentation extends MobileNetV1PreTrainedModel {
}
export class MobileNetV2PreTrainedModel extends PreTrainedModel {
}
/**
 * The bare MobileNetV2 model outputting raw hidden-states without any specific head on top.
 */
export class MobileNetV2Model extends MobileNetV2PreTrainedModel {
}
/**
 * MobileNetV2 model with an image classification head on top (a linear layer on top of the pooled features),
 * e.g. for ImageNet.
 */
export class MobileNetV2ForImageClassification extends MobileNetV2PreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
export class MobileNetV2ForSemanticSegmentation extends MobileNetV2PreTrainedModel {
}
export class MobileNetV3PreTrainedModel extends PreTrainedModel {
}
/**
 * The bare MobileNetV3 model outputting raw hidden-states without any specific head on top.
 */
export class MobileNetV3Model extends MobileNetV3PreTrainedModel {
}
/**
 * MobileNetV3 model with an image classification head on top (a linear layer on top of the pooled features),
 * e.g. for ImageNet.
 */
export class MobileNetV3ForImageClassification extends MobileNetV3PreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
export class MobileNetV3ForSemanticSegmentation extends MobileNetV3PreTrainedModel {
}
export class MobileNetV4PreTrainedModel extends PreTrainedModel {
}
/**
 * The bare MobileNetV4 model outputting raw hidden-states without any specific head on top.
 */
export class MobileNetV4Model extends MobileNetV4PreTrainedModel {
}
/**
 * MobileNetV4 model with an image classification head on top (a linear layer on top of the pooled features),
 * e.g. for ImageNet.
 */
export class MobileNetV4ForImageClassification extends MobileNetV4PreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<SequenceClassifierOutput>;
}
export class MobileNetV4ForSemanticSegmentation extends MobileNetV4PreTrainedModel {
}
export class DecisionTransformerPreTrainedModel extends PreTrainedModel {
}
/**
 * The model builds upon the GPT2 architecture to perform autoregressive prediction of actions in an offline RL setting.
 * Refer to the paper for more details: https://arxiv.org/abs/2106.01345
 */
export class DecisionTransformerModel extends DecisionTransformerPreTrainedModel {
}
export class MultiModalityPreTrainedModel extends PreTrainedModel {
}
export class MultiModalityCausalLM extends MultiModalityPreTrainedModel {
    _generation_mode: string;
    forward(model_inputs: any): Promise<any>;
    /**
     * @param {import('./generation/parameters.js').GenerationFunctionParameters} options
     */
    generate_images(options: import("./generation/parameters.js").GenerationFunctionParameters): Promise<RawImage[]>;
}
export class MgpstrModelOutput extends ModelOutput {
    constructor({ char_logits, bpe_logits, wp_logits }: {
        char_logits: any;
        bpe_logits: any;
        wp_logits: any;
    });
    char_logits: any;
    bpe_logits: any;
    wp_logits: any;
    get logits(): any[];
}
export class MgpstrPreTrainedModel extends PreTrainedModel {
}
/**
 * MGP-STR Model transformer with three classification heads on top
 * (three A^3 modules and three linear layer on top of the transformer encoder output) for scene text recognition (STR).
 */
export class MgpstrForSceneTextRecognition extends MgpstrPreTrainedModel {
    /**
     * @param {any} model_inputs
     */
    _call(model_inputs: any): Promise<MgpstrModelOutput>;
}
export class PatchTSTPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare PatchTST Model outputting raw hidden-states without any specific head.
 */
export class PatchTSTModel extends PatchTSTPreTrainedModel {
}
/**
 * The PatchTST for prediction model.
 */
export class PatchTSTForPrediction extends PatchTSTPreTrainedModel {
}
export class PatchTSMixerPreTrainedModel extends PreTrainedModel {
}
/**
 * The bare PatchTSMixer Model outputting raw hidden-states without any specific head.
 */
export class PatchTSMixerModel extends PatchTSMixerPreTrainedModel {
}
/**
 * The PatchTSMixer for prediction model.
 */
export class PatchTSMixerForPrediction extends PatchTSMixerPreTrainedModel {
}
export class UltravoxPreTrainedModel extends PreTrainedModel {
}
export class UltravoxModel extends UltravoxPreTrainedModel {
    _merge_input_ids_with_audio_features(kwargs: any): {
        inputs_embeds: any;
        attention_mask: any;
    };
}
export class MimiPreTrainedModel extends PreTrainedModel {
}
export class MimiEncoderOutput extends ModelOutput {
    /**
     * @param {Object} output The output of the model.
     * @param {Tensor} output.audio_codes Discrete code embeddings, of shape `(batch_size, num_quantizers, codes_length)`.
     */
    constructor({ audio_codes }: {
        audio_codes: Tensor;
    });
    audio_codes: Tensor;
}
export class MimiDecoderOutput extends ModelOutput {
    /**
     * @param {Object} output The output of the model.
     * @param {Tensor} output.audio_values Decoded audio values, of shape `(batch_size, num_channels, sequence_length)`.
     */
    constructor({ audio_values }: {
        audio_values: Tensor;
    });
    audio_values: Tensor;
}
/**
 * The Mimi neural audio codec model.
 */
export class MimiModel extends MimiPreTrainedModel {
    /**
     * Encodes the input audio waveform into discrete codes.
     * @param {Object} inputs Model inputs
     * @param {Tensor} [inputs.input_values] Float values of the input audio waveform, of shape `(batch_size, channels, sequence_length)`).
     * @returns {Promise<MimiEncoderOutput>} The output tensor of shape `(batch_size, num_codebooks, sequence_length)`.
     */
    encode(inputs: {
        input_values?: Tensor;
    }): Promise<MimiEncoderOutput>;
    /**
     * Decodes the given frames into an output audio waveform.
     * @param {MimiEncoderOutput} inputs The encoded audio codes.
     * @returns {Promise<MimiDecoderOutput>} The output tensor of shape `(batch_size, num_channels, sequence_length)`.
     */
    decode(inputs: MimiEncoderOutput): Promise<MimiDecoderOutput>;
}
export class MimiEncoderModel extends MimiPreTrainedModel {
}
export class MimiDecoderModel extends MimiPreTrainedModel {
}
export class DacPreTrainedModel extends PreTrainedModel {
}
export class DacEncoderOutput extends ModelOutput {
    /**
     * @param {Object} output The output of the model.
     * @param {Tensor} output.audio_codes Discrete code embeddings, of shape `(batch_size, num_quantizers, codes_length)`.
     */
    constructor({ audio_codes }: {
        audio_codes: Tensor;
    });
    audio_codes: Tensor;
}
export class DacDecoderOutput extends ModelOutput {
    /**
     * @param {Object} output The output of the model.
     * @param {Tensor} output.audio_values Decoded audio values, of shape `(batch_size, num_channels, sequence_length)`.
     */
    constructor({ audio_values }: {
        audio_values: Tensor;
    });
    audio_values: Tensor;
}
/**
 * The DAC (Descript Audio Codec) model.
 */
export class DacModel extends DacPreTrainedModel {
    /**
     * Encodes the input audio waveform into discrete codes.
     * @param {Object} inputs Model inputs
     * @param {Tensor} [inputs.input_values] Float values of the input audio waveform, of shape `(batch_size, channels, sequence_length)`).
     * @returns {Promise<DacEncoderOutput>} The output tensor of shape `(batch_size, num_codebooks, sequence_length)`.
     */
    encode(inputs: {
        input_values?: Tensor;
    }): Promise<DacEncoderOutput>;
    /**
     * Decodes the given frames into an output audio waveform.
     * @param {DacEncoderOutput} inputs The encoded audio codes.
     * @returns {Promise<DacDecoderOutput>} The output tensor of shape `(batch_size, num_channels, sequence_length)`.
     */
    decode(inputs: DacEncoderOutput): Promise<DacDecoderOutput>;
}
export class DacEncoderModel extends DacPreTrainedModel {
}
export class DacDecoderModel extends DacPreTrainedModel {
}
export class SnacPreTrainedModel extends PreTrainedModel {
}
/**
 * The SNAC (Multi-Scale Neural Audio Codec) model.
 */
export class SnacModel extends SnacPreTrainedModel {
    /**
     * Encodes the input audio waveform into discrete codes.
     * @param {Object} inputs Model inputs
     * @param {Tensor} [inputs.input_values] Float values of the input audio waveform, of shape `(batch_size, channels, sequence_length)`).
     * @returns {Promise<Record<string, Tensor>>} The output tensors of shape `(batch_size, num_codebooks, sequence_length)`.
     */
    encode(inputs: {
        input_values?: Tensor;
    }): Promise<Record<string, Tensor>>;
    /**
     * Decodes the given frames into an output audio waveform.
     * @param {Record<string, Tensor>} inputs The encoded audio codes.
     * @returns {Promise<{audio_values: Tensor}>} The output tensor of shape `(batch_size, num_channels, sequence_length)`.
     */
    decode(inputs: Record<string, Tensor>): Promise<{
        audio_values: Tensor;
    }>;
}
export class SnacEncoderModel extends SnacPreTrainedModel {
}
export class SnacDecoderModel extends SnacPreTrainedModel {
}
/**
 * Base class of all AutoModels. Contains the `from_pretrained` function
 * which is used to instantiate pretrained models.
 */
export class PretrainedMixin {
    /**
     * Mapping from model type to model class.
     * @type {Map<string, Object>[]}
     */
    static MODEL_CLASS_MAPPINGS: Map<string, any>[];
    /**
     * Whether to attempt to instantiate the base class (`PretrainedModel`) if
     * the model type is not found in the mapping.
     */
    static BASE_IF_FAIL: boolean;
    /**
     * Instantiate one of the model classes of the library from a pretrained model.
     *
     * The model class to instantiate is selected based on the `model_type` property of the config object
     * (either passed as an argument or loaded from `pretrained_model_name_or_path` if possible)
     *
     * @param {string} pretrained_model_name_or_path The name or path of the pretrained model. Can be either:
     * - A string, the *model id* of a pretrained model hosted inside a model repo on huggingface.co.
     *   Valid model ids can be located at the root-level, like `bert-base-uncased`, or namespaced under a
     *   user or organization name, like `dbmdz/bert-base-german-cased`.
     * - A path to a *directory* containing model weights, e.g., `./my_model_directory/`.
     * @param {import('./utils/hub.js').PretrainedModelOptions} options Additional options for loading the model.
     *
     * @returns {Promise<PreTrainedModel>} A new instance of the `PreTrainedModel` class.
     */
    static from_pretrained(pretrained_model_name_or_path: string, { progress_callback, config, cache_dir, local_files_only, revision, model_file_name, subfolder, device, dtype, use_external_data_format, session_options, }?: import("./utils/hub.js").PretrainedModelOptions): Promise<PreTrainedModel>;
}
/**
 * Helper class which is used to instantiate pretrained models with the `from_pretrained` function.
 * The chosen model class is determined by the type specified in the model config.
 *
 * @example
 * let model = await AutoModel.from_pretrained('Xenova/bert-base-uncased');
 */
export class AutoModel extends PretrainedMixin {
}
/**
 * Helper class which is used to instantiate pretrained sequence classification models with the `from_pretrained` function.
 * The chosen model class is determined by the type specified in the model config.
 *
 * @example
 * let model = await AutoModelForSequenceClassification.from_pretrained('Xenova/distilbert-base-uncased-finetuned-sst-2-english');
 */
export class AutoModelForSequenceClassification extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof BertForSequenceClassification)[]>[];
}
/**
 * Helper class which is used to instantiate pretrained token classification models with the `from_pretrained` function.
 * The chosen model class is determined by the type specified in the model config.
 *
 * @example
 * let model = await AutoModelForTokenClassification.from_pretrained('Xenova/distilbert-base-multilingual-cased-ner-hrl');
 */
export class AutoModelForTokenClassification extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof BertForTokenClassification)[]>[];
}
/**
 * Helper class which is used to instantiate pretrained sequence-to-sequence models with the `from_pretrained` function.
 * The chosen model class is determined by the type specified in the model config.
 *
 * @example
 * let model = await AutoModelForSeq2SeqLM.from_pretrained('Xenova/t5-small');
 */
export class AutoModelForSeq2SeqLM extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof T5ForConditionalGeneration)[]>[];
}
/**
 * Helper class which is used to instantiate pretrained sequence-to-sequence speech-to-text models with the `from_pretrained` function.
 * The chosen model class is determined by the type specified in the model config.
 *
 * @example
 * let model = await AutoModelForSpeechSeq2Seq.from_pretrained('openai/whisper-tiny.en');
 */
export class AutoModelForSpeechSeq2Seq extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof SpeechT5ForSpeechToText)[] | (string | typeof WhisperForConditionalGeneration)[]>[];
}
/**
 * Helper class which is used to instantiate pretrained sequence-to-sequence text-to-spectrogram models with the `from_pretrained` function.
 * The chosen model class is determined by the type specified in the model config.
 *
 * @example
 * let model = await AutoModelForTextToSpectrogram.from_pretrained('microsoft/speecht5_tts');
 */
export class AutoModelForTextToSpectrogram extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof SpeechT5ForTextToSpeech)[]>[];
}
/**
 * Helper class which is used to instantiate pretrained text-to-waveform models with the `from_pretrained` function.
 * The chosen model class is determined by the type specified in the model config.
 *
 * @example
 * let model = await AutoModelForTextToSpectrogram.from_pretrained('facebook/mms-tts-eng');
 */
export class AutoModelForTextToWaveform extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof VitsModel)[] | (string | typeof MusicgenForConditionalGeneration)[]>[];
}
/**
 * Helper class which is used to instantiate pretrained causal language models with the `from_pretrained` function.
 * The chosen model class is determined by the type specified in the model config.
 *
 * @example
 * let model = await AutoModelForCausalLM.from_pretrained('Xenova/gpt2');
 */
export class AutoModelForCausalLM extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof Phi3VForCausalLM)[]>[];
}
/**
 * Helper class which is used to instantiate pretrained masked language models with the `from_pretrained` function.
 * The chosen model class is determined by the type specified in the model config.
 *
 * @example
 * let model = await AutoModelForMaskedLM.from_pretrained('Xenova/bert-base-uncased');
 */
export class AutoModelForMaskedLM extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof BertForMaskedLM)[]>[];
}
/**
 * Helper class which is used to instantiate pretrained question answering models with the `from_pretrained` function.
 * The chosen model class is determined by the type specified in the model config.
 *
 * @example
 * let model = await AutoModelForQuestionAnswering.from_pretrained('Xenova/distilbert-base-cased-distilled-squad');
 */
export class AutoModelForQuestionAnswering extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof BertForQuestionAnswering)[]>[];
}
/**
 * Helper class which is used to instantiate pretrained vision-to-sequence models with the `from_pretrained` function.
 * The chosen model class is determined by the type specified in the model config.
 *
 * @example
 * let model = await AutoModelForVision2Seq.from_pretrained('Xenova/vit-gpt2-image-captioning');
 */
export class AutoModelForVision2Seq extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof VisionEncoderDecoderModel)[]>[];
}
/**
 * Helper class which is used to instantiate pretrained image classification models with the `from_pretrained` function.
 * The chosen model class is determined by the type specified in the model config.
 *
 * @example
 * let model = await AutoModelForImageClassification.from_pretrained('Xenova/vit-base-patch16-224');
 */
export class AutoModelForImageClassification extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof SegformerForImageClassification)[]>[];
}
/**
 * Helper class which is used to instantiate pretrained image segmentation models with the `from_pretrained` function.
 * The chosen model class is determined by the type specified in the model config.
 *
 * @example
 * let model = await AutoModelForImageSegmentation.from_pretrained('Xenova/detr-resnet-50-panoptic');
 */
export class AutoModelForImageSegmentation extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof CLIPSegForImageSegmentation)[]>[];
}
/**
 * Helper class which is used to instantiate pretrained image segmentation models with the `from_pretrained` function.
 * The chosen model class is determined by the type specified in the model config.
 *
 * @example
 * let model = await AutoModelForSemanticSegmentation.from_pretrained('nvidia/segformer-b3-finetuned-cityscapes-1024-1024');
 */
export class AutoModelForSemanticSegmentation extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof SegformerForSemanticSegmentation)[]>[];
}
/**
 * Helper class which is used to instantiate pretrained universal image segmentation models with the `from_pretrained` function.
 * The chosen model class is determined by the type specified in the model config.
 *
 * @example
 * let model = await AutoModelForUniversalSegmentation.from_pretrained('hf-internal-testing/tiny-random-MaskFormerForInstanceSegmentation');
 */
export class AutoModelForUniversalSegmentation extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof MaskFormerForInstanceSegmentation)[]>[];
}
/**
 * Helper class which is used to instantiate pretrained object detection models with the `from_pretrained` function.
 * The chosen model class is determined by the type specified in the model config.
 *
 * @example
 * let model = await AutoModelForObjectDetection.from_pretrained('Xenova/detr-resnet-50');
 */
export class AutoModelForObjectDetection extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof DetrForObjectDetection)[]>[];
}
export class AutoModelForZeroShotObjectDetection extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof OwlViTForObjectDetection)[]>[];
}
/**
 * Helper class which is used to instantiate pretrained mask generation models with the `from_pretrained` function.
 * The chosen model class is determined by the type specified in the model config.
 *
 * @example
 * let model = await AutoModelForMaskGeneration.from_pretrained('Xenova/sam-vit-base');
 */
export class AutoModelForMaskGeneration extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof SamModel)[]>[];
}
export class AutoModelForCTC extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof Wav2Vec2ForCTC)[] | (string | typeof Wav2Vec2BertForCTC)[]>[];
}
export class AutoModelForAudioClassification extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof ASTForAudioClassification)[]>[];
}
export class AutoModelForXVector extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof WavLMForXVector)[]>[];
}
export class AutoModelForAudioFrameClassification extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof UniSpeechSatForAudioFrameClassification)[]>[];
}
export class AutoModelForDocumentQuestionAnswering extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof VisionEncoderDecoderModel)[]>[];
}
export class AutoModelForImageMatting extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof VitMatteForImageMatting)[]>[];
}
export class AutoModelForImageToImage extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof Swin2SRForImageSuperResolution)[]>[];
}
export class AutoModelForDepthEstimation extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof DPTForDepthEstimation)[]>[];
}
export class AutoModelForNormalEstimation extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof SapiensForNormalEstimation)[]>[];
}
export class AutoModelForPoseEstimation extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof VitPoseForPoseEstimation)[]>[];
}
export class AutoModelForImageFeatureExtraction extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof CLIPVisionModelWithProjection)[]>[];
}
export class AutoModelForImageTextToText extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof Idefics3ForConditionalGeneration)[] | (string | typeof Florence2ForConditionalGeneration)[]>[];
}
export class AutoModelForAudioTextToText extends PretrainedMixin {
    static MODEL_CLASS_MAPPINGS: Map<string, (string | typeof UltravoxModel)[]>[];
}
export class Seq2SeqLMOutput extends ModelOutput {
    /**
     * @param {Object} output The output of the model.
     * @param {Tensor} output.logits The output logits of the model.
     * @param {Tensor} output.past_key_values An tensor of key/value pairs that represent the previous state of the model.
     * @param {Tensor} output.encoder_outputs The output of the encoder in a sequence-to-sequence model.
     * @param {Tensor} [output.decoder_attentions] Attentions weights of the decoder, after the attention softmax, used to compute the weighted average in the self-attention heads.
     * @param {Tensor} [output.cross_attentions] Attentions weights of the decoder's cross-attention layer, after the attention softmax, used to compute the weighted average in the cross-attention heads.
     */
    constructor({ logits, past_key_values, encoder_outputs, decoder_attentions, cross_attentions }: {
        logits: Tensor;
        past_key_values: Tensor;
        encoder_outputs: Tensor;
        decoder_attentions?: Tensor;
        cross_attentions?: Tensor;
    });
    logits: Tensor;
    past_key_values: Tensor;
    encoder_outputs: Tensor;
    decoder_attentions: Tensor;
    cross_attentions: Tensor;
}
/**
 * Base class for outputs of sentence classification models.
 */
export class SequenceClassifierOutput extends ModelOutput {
    /**
     * @param {Object} output The output of the model.
     * @param {Tensor} output.logits classification (or regression if config.num_labels==1) scores (before SoftMax).
     * @param {Record<string, Tensor>} [output.attentions] Object of `torch.FloatTensor` (one for each layer) of shape `(batch_size, num_heads, sequence_length, sequence_length)`.
     * Attentions weights after the attention softmax, used to compute the weighted average in the self-attention heads.
     */
    constructor({ logits, ...attentions }: {
        logits: Tensor;
        attentions?: Record<string, Tensor>;
    });
    logits: Tensor;
    attentions: Record<string, Tensor>[];
}
/**
 * Base class for outputs of XVector models.
 */
export class XVectorOutput extends ModelOutput {
    /**
     * @param {Object} output The output of the model.
     * @param {Tensor} output.logits Classification hidden states before AMSoftmax, of shape `(batch_size, config.xvector_output_dim)`.
     * @param {Tensor} output.embeddings Utterance embeddings used for vector similarity-based retrieval, of shape `(batch_size, config.xvector_output_dim)`.
     */
    constructor({ logits, embeddings }: {
        logits: Tensor;
        embeddings: Tensor;
    });
    logits: Tensor;
    embeddings: Tensor;
}
/**
 * Base class for outputs of token classification models.
 */
export class TokenClassifierOutput extends ModelOutput {
    /**
     * @param {Object} output The output of the model.
     * @param {Tensor} output.logits Classification scores (before SoftMax).
     */
    constructor({ logits }: {
        logits: Tensor;
    });
    logits: Tensor;
}
/**
 * Base class for masked language models outputs.
 */
export class MaskedLMOutput extends ModelOutput {
    /**
     * @param {Object} output The output of the model.
     * @param {Tensor} output.logits Prediction scores of the language modeling head (scores for each vocabulary token before SoftMax).
     */
    constructor({ logits }: {
        logits: Tensor;
    });
    logits: Tensor;
}
/**
 * Base class for outputs of question answering models.
 */
export class QuestionAnsweringModelOutput extends ModelOutput {
    /**
     * @param {Object} output The output of the model.
     * @param {Tensor} output.start_logits Span-start scores (before SoftMax).
     * @param {Tensor} output.end_logits Span-end scores (before SoftMax).
     */
    constructor({ start_logits, end_logits }: {
        start_logits: Tensor;
        end_logits: Tensor;
    });
    start_logits: Tensor;
    end_logits: Tensor;
}
/**
 * Base class for causal language model (or autoregressive) outputs.
 */
export class CausalLMOutput extends ModelOutput {
    /**
     * @param {Object} output The output of the model.
     * @param {Tensor} output.logits Prediction scores of the language modeling head (scores for each vocabulary token before softmax).
     */
    constructor({ logits }: {
        logits: Tensor;
    });
    logits: Tensor;
}
/**
 * Base class for causal language model (or autoregressive) outputs.
 */
export class CausalLMOutputWithPast extends ModelOutput {
    /**
     * @param {Object} output The output of the model.
     * @param {Tensor} output.logits Prediction scores of the language modeling head (scores for each vocabulary token before softmax).
     * @param {Tensor} output.past_key_values Contains pre-computed hidden-states (key and values in the self-attention blocks)
     * that can be used (see `past_key_values` input) to speed up sequential decoding.
     */
    constructor({ logits, past_key_values }: {
        logits: Tensor;
        past_key_values: Tensor;
    });
    logits: Tensor;
    past_key_values: Tensor;
}
export class ImageMattingOutput extends ModelOutput {
    /**
     * @param {Object} output The output of the model.
     * @param {Tensor} output.alphas Estimated alpha values, of shape `(batch_size, num_channels, height, width)`.
     */
    constructor({ alphas }: {
        alphas: Tensor;
    });
    alphas: Tensor;
}
/**
 * Describes the outputs for the VITS model.
 */
export class VitsModelOutput extends ModelOutput {
    /**
     * @param {Object} output The output of the model.
     * @param {Tensor} output.waveform The final audio waveform predicted by the model, of shape `(batch_size, sequence_length)`.
     * @param {Tensor} output.spectrogram The log-mel spectrogram predicted at the output of the flow model.
     * This spectrogram is passed to the Hi-Fi GAN decoder model to obtain the final audio waveform.
     */
    constructor({ waveform, spectrogram }: {
        waveform: Tensor;
        spectrogram: Tensor;
    });
    waveform: Tensor;
    spectrogram: Tensor;
}
/**
 * Forward pass of a decoder model.
 * @param {Object} self The decoder model.
 * @param {Object} model_inputs The input data to be used for the forward pass.
 * @returns {Promise<Object>} The logits and past key values.
 * @private
 */
declare function decoderForward(self: any, model_inputs: any, is_encoder_decoder?: boolean): Promise<any>;
declare function autoEncoderForward(self: any, model_inputs: any): Promise<any>;
declare function multimodal_text_to_text_prepare_inputs_for_generation(self: any, ...args: any[]): any;
import { GenerationConfig } from './generation/configuration_utils.js';
import { LogitsProcessorList } from './generation/logits_process.js';
import { StoppingCriteriaList } from './generation/stopping_criteria.js';
import { Tensor } from './utils/tensor.js';
import { WhisperGenerationConfig } from './models/whisper/generation_whisper.js';
import { RawImage } from './utils/image.js';
export {};
//# sourceMappingURL=models.d.ts.map