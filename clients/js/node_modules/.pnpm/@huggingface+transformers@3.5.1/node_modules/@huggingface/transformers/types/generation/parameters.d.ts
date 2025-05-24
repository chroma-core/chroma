export type GenerationFunctionParameters = {
    /**
     * (`Tensor` of varying shape depending on the modality, *optional*):
     * The sequence used as a prompt for the generation or as model inputs to the encoder. If `null` the
     * method initializes it with `bos_token_id` and a batch size of 1. For decoder-only models `inputs`
     * should be in the format of `input_ids`. For encoder-decoder models *inputs* can represent any of
     * `input_ids`, `input_values`, `input_features`, or `pixel_values`.
     */
    inputs?: import("../utils/tensor.js").Tensor;
    /**
     * (`GenerationConfig`, *optional*):
     * The generation configuration to be used as base parametrization for the generation call.
     * `**kwargs` passed to generate matching the attributes of `generation_config` will override them.
     * If `generation_config` is not provided, the default will be used, which has the following loading
     * priority:
     * - (1) from the `generation_config.json` model file, if it exists;
     * - (2) from the model configuration. Please note that unspecified parameters will inherit [`GenerationConfig`]'s
     * default values, whose documentation should be checked to parameterize generation.
     */
    generation_config?: import("./configuration_utils.js").GenerationConfig;
    /**
     * (`LogitsProcessorList`, *optional*):
     * Custom logits processors that complement the default logits processors built from arguments and
     * generation config. If a logit processor is passed that is already created with the arguments or a
     * generation config an error is thrown. This feature is intended for advanced users.
     */
    logits_processor?: import("./logits_process.js").LogitsProcessorList;
    /**
     * (`StoppingCriteriaList`, *optional*):
     * Custom stopping criteria that complements the default stopping criteria built from arguments and a
     * generation config. If a stopping criteria is passed that is already created with the arguments or a
     * generation config an error is thrown. This feature is intended for advanced users.
     */
    stopping_criteria?: import("./stopping_criteria.js").StoppingCriteriaList;
    /**
     * (`BaseStreamer`, *optional*):
     * Streamer object that will be used to stream the generated sequences. Generated tokens are passed
     * through `streamer.put(token_ids)` and the streamer is responsible for any further processing.
     */
    streamer?: import("./streamers.js").BaseStreamer;
    /**
     * (`number[]`, *optional*):
     * If the model is an encoder-decoder model, this argument is used to pass the `decoder_input_ids`.
     */
    decoder_input_ids?: number[];
};
//# sourceMappingURL=parameters.d.ts.map