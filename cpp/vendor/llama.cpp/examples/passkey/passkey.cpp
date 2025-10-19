#include "common.h"
#include "llama.h"

#include <cmath>
#include <cstdio>
#include <string>
#include <vector>

static void print_usage(int argc, char ** argv, const gpt_params & params) {
    gpt_params_print_usage(argc, argv, params);

    LOG_TEE("\nexample usage:\n");
    LOG_TEE("\n    %s -m model.gguf --junk 250 --pos 90 --keep 32 --grp-attn-n 2 [--seed 1234]\n", argv[0]);
    LOG_TEE("\n");
}

int main(int argc, char ** argv) {
    gpt_params params;

    params.n_junk = 250;
    params.n_keep = 32;
    params.i_pos  = -1;

    if (!gpt_params_parse(argc, argv, params)) {
        print_usage(argc, argv, params);
        return 1;
    }

    srand(params.seed == LLAMA_DEFAULT_SEED ? time(NULL) : params.seed);

    int n_junk = params.n_junk;
    int n_keep = params.n_keep;
    int n_grp  = params.grp_attn_n;
    int i_pos  = params.i_pos;

    if (i_pos == -1) {
        i_pos = rand() % n_junk;
    }

    const std::string prompt_prefix = "There is an important info hidden inside a lot of irrelevant text. Find it and memorize them. I will quiz you about the important information there.";
    const std::string prompt_suffix = " What is the pass key? The pass key is";

    // generate junk text
    params.prompt = prompt_prefix;

    const int passkey = rand() % 50000 + 1;

    for (int i = 0; i < n_junk; i++) {
        if (i % n_junk == i_pos) {
            params.prompt += " The pass key is " + std::to_string(passkey) + ". Remember it. " + std::to_string(passkey) + " is the pass key.";
        }

        params.prompt += " The grass is green. The sky is blue. The sun is yellow. Here we go. There and back again.";
    }

    params.prompt += prompt_suffix;

    // init LLM

    llama_backend_init();
    llama_numa_init(params.numa);

    // initialize the model

    llama_model_params model_params = llama_model_params_from_gpt_params(params);

    llama_model * model = llama_load_model_from_file(params.model.c_str(), model_params);

    if (model == NULL) {
        fprintf(stderr , "%s: error: unable to load model\n" , __func__);
        return 1;
    }

    // initialize the context

    llama_context_params ctx_params = llama_context_params_from_gpt_params(params);

    ctx_params.n_ctx = llama_n_ctx_train(model)*n_grp + n_keep;

    GGML_ASSERT(ctx_params.n_batch % n_grp == 0 && "n_batch must be divisible by n_grp");

    llama_context * ctx = llama_new_context_with_model(model, ctx_params);

    if (ctx == NULL) {
        fprintf(stderr , "%s: error: failed to create the llama_context\n" , __func__);
        return 1;
    }

    // tokenize the prompt
    std::vector<llama_token> tokens_list;
    tokens_list = ::llama_tokenize(ctx, params.prompt, true);

    // tokenize the prefix and use it as a sink
    const int n_tokens_prefix = ::llama_tokenize(ctx, prompt_prefix, true).size();

    const int n_tokens_all = tokens_list.size();

    // we leave a margin of 16 tokens for the generated text - it should contain just the passkey
    const int n_predict = 16;

    // total length of the sequences including the prompt
    const int n_len = n_tokens_all + n_predict;

    const int n_ctx       = llama_n_ctx(ctx) - n_keep;
    const int n_kv_req    = llama_n_ctx(ctx);
    const int n_batch     = ctx_params.n_batch;
    const int n_batch_grp = ctx_params.n_batch/n_grp;

    LOG_TEE("\n%s: n_len = %d, n_ctx = %d, n_kv_req = %d, n_grp = %d, n_batch = %d, n_junk = %d, i_pos = %d\n", __func__, n_len, n_ctx, n_kv_req, n_grp, n_batch, n_junk, i_pos);

    // print the prompt token-by-token

    LOG_TEE("\n");
    LOG_TEE("prefix tokens: %d\n", n_tokens_prefix);
    LOG_TEE("prompt tokens: %d\n", n_tokens_all);
    //LOG_TEE("prompt: %s\n", params.prompt.c_str());

    llama_batch batch = llama_batch_init(params.n_batch, 0, 1);

    int n_past = 0;

    // fill the KV cache
    for (int i = 0; i < n_ctx; i += n_batch) {
        if (i > 0 && n_grp > 1) {
            // if SelfExtend is enabled, we compress the position from the last batch by a factor of n_grp
            const int ib = i/n_batch - 1;
            const int bd = n_batch_grp*(n_grp - 1);

            llama_kv_cache_seq_add (ctx, 0, n_past - n_batch,         n_past,         ib*bd);
            llama_kv_cache_seq_div (ctx, 0, n_past - n_batch + ib*bd, n_past + ib*bd, n_grp);
            llama_kv_cache_update  (ctx);

            n_past = llama_kv_cache_seq_pos_max(ctx, 0) + 1;
        }

        llama_batch_clear(batch);

        for (int j = 0; j < n_batch && i + j < n_tokens_all; j++) {
            llama_batch_add(batch, tokens_list[i + j], n_past++, { 0 }, false);
        }

        if (i + n_batch >= n_tokens_all) {
            batch.logits[batch.n_tokens - 1] = true;
        }

        if (llama_decode(ctx, batch) != 0) {
            LOG_TEE("%s: llama_decode() failed\n", __func__);
            return 1;
        }

        LOG_TEE("%s: processed: [%6d, %6d)\n", __func__, i, std::min(i + n_batch, n_tokens_all));

        if (i + n_batch >= n_tokens_all) {
            break;
        }
    }

    for (int i = n_ctx; i < n_tokens_all; i += n_batch) {
        const int n_discard = n_batch;

        LOG_TEE("%s: shifting KV cache with %d\n", __func__, n_discard);

        llama_kv_cache_seq_rm (ctx, 0, n_keep            , n_keep + n_discard);
        llama_kv_cache_seq_add(ctx, 0, n_keep + n_discard, n_ctx,  -n_discard);
      //llama_kv_cache_defrag (ctx);
        llama_kv_cache_update (ctx);

        n_past = llama_kv_cache_seq_pos_max(ctx, 0) + 1;

        llama_batch_clear(batch);

        for (int j = 0; j < n_batch && i + j < n_tokens_all; j++) {
            llama_batch_add(batch, tokens_list[i + j], n_past++, { 0 }, false);
        }

        if (i + n_batch >= n_tokens_all) {
            batch.logits[batch.n_tokens - 1] = true;
        }

        if (llama_decode(ctx, batch) != 0) {
            LOG_TEE("%s: llama_decode() failed\n", __func__);
            return 1;
        }

        LOG_TEE("%s: processed: [%6d, %6d)\n", __func__, i, std::min(i + n_batch, n_tokens_all));
    }

    {
        const int n_discard = n_past - n_ctx + n_predict;

        if (n_discard > 0) {
            LOG_TEE("%s: shifting KV cache with %d to free space for the answer\n", __func__, n_discard);

            llama_kv_cache_seq_rm (ctx, 0, n_keep            , n_keep + n_discard);
            llama_kv_cache_seq_add(ctx, 0, n_keep + n_discard, n_ctx,  -n_discard);
          //llama_kv_cache_defrag (ctx);
            llama_kv_cache_update (ctx);

            n_past = llama_kv_cache_seq_pos_max(ctx, 0) + 1;
        }
    }

    LOG_TEE("\n");
    LOG_TEE("%s: passkey = %d, inserted at position %d / %d (token pos: ~%d)\n", __func__, passkey, i_pos, n_junk, (i_pos * n_tokens_all) / n_junk);
    LOG_TEE("\n");

    // main loop

    int n_cur    = n_tokens_all;
    int n_decode = 0;

    LOG_TEE("%s", prompt_suffix.c_str());
    fflush(stdout);

    const auto t_main_start = ggml_time_us();

    while (n_cur <= n_len) {
        // sample the next token
        {
            auto   n_vocab = llama_n_vocab(model);
            auto * logits  = llama_get_logits_ith(ctx, batch.n_tokens - 1);

            std::vector<llama_token_data> candidates;
            candidates.reserve(n_vocab);

            for (llama_token token_id = 0; token_id < n_vocab; token_id++) {
                candidates.emplace_back(llama_token_data{ token_id, logits[token_id], 0.0f });
            }

            llama_token_data_array candidates_p = { candidates.data(), candidates.size(), false };

            // sample the most likely token
            const llama_token new_token_id = llama_sample_token_greedy(ctx, &candidates_p);

            // is it an end of generation?
            if (llama_token_is_eog(model, new_token_id) || n_cur == n_len) {
                LOG_TEE("\n");

                break;
            }

            LOG_TEE("%s", llama_token_to_piece(ctx, new_token_id).c_str());
            fflush(stdout);

            n_decode += 1;

            // prepare the next batch
            llama_batch_clear(batch);

            // push this new token for next evaluation
            llama_batch_add(batch, new_token_id, n_past++, { 0 }, true);
        }

        n_cur += 1;

        // evaluate the current batch with the transformer model
        if (llama_decode(ctx, batch)) {
            fprintf(stderr, "%s : failed to eval, return code %d\n", __func__, 1);
            return 1;
        }
    }

    LOG_TEE("\n");

    const auto t_main_end = ggml_time_us();

    LOG_TEE("%s: decoded %d tokens in %.2f s, speed: %.2f t/s\n",
            __func__, n_decode, (t_main_end - t_main_start) / 1000000.0f, n_decode / ((t_main_end - t_main_start) / 1000000.0f));

    llama_print_timings(ctx);

    fprintf(stderr, "\n");

    llama_batch_free(batch);

    llama_free(ctx);
    llama_free_model(model);

    llama_backend_free();

    return 0;
}
