#include <cstdint>
#include <cstdio>
#include <vector>
#include <string>
#include <thread>

#include "common.h"
#include "llama.h"
#include "log.h"

#include <ctime>

#if defined(_MSC_VER)
#pragma warning(disable : 4244 4267) // possible loss of data
#endif

static void batch_add_seq(llama_batch &batch, const std::vector<int32_t> &tokens, llama_seq_id seq_id)
{
    size_t n_tokens = tokens.size();
    for (size_t i = 0; i < n_tokens; i++)
    {
        llama_batch_add(batch, tokens[i], i, {seq_id}, true);
    }
}

static void batch_decode(llama_context *ctx, llama_batch &batch, float *output, int n_seq, int n_embd, int embd_norm)
{
    const enum llama_pooling_type pooling_type = llama_pooling_type(ctx);
    const struct llama_model *model = llama_get_model(ctx);

    // clear previous kv_cache values (irrelevant for embeddings)
    llama_kv_cache_clear(ctx);

    // run model
    if (llama_model_has_encoder(model) && !llama_model_has_decoder(model))
    {
        // encoder-only model
        if (llama_encode(ctx, batch) < 0)
        {
            fprintf(stderr, "%s : failed to encode\n", __func__);
        }
    }
    else if (!llama_model_has_encoder(model) && llama_model_has_decoder(model))
    {
        // decoder-only model
        if (llama_decode(ctx, batch) < 0)
        {
            fprintf(stderr, "%s : failed to decode\n", __func__);
        }
    }

    for (int i = 0; i < batch.n_tokens; i++)
    {
        if (!batch.logits[i])
        {
            continue;
        }

        const float *embd = nullptr;
        int embd_pos = 0;

        if (pooling_type == LLAMA_POOLING_TYPE_NONE)
        {
            // try to get token embeddings
            embd = llama_get_embeddings_ith(ctx, i);
            embd_pos = i;
            GGML_ASSERT(embd != NULL && "failed to get token embeddings");
        }
        else
        {
            // try to get sequence embeddings - supported only when pooling_type is not NONE
            embd = llama_get_embeddings_seq(ctx, batch.seq_id[i][0]);
            embd_pos = batch.seq_id[i][0];
            GGML_ASSERT(embd != NULL && "failed to get sequence embeddings");
        }

        float *out = output + embd_pos * n_embd;

        llama_embd_normalize(embd, out, n_embd, embd_norm);
    }
}

class EmbeddingModel
{

public:
    EmbeddingModel(const std::string &model_path)
    {
        params.embedding = true;
        params.embd_out = "array";

        // For non-causal models, batch size must be equal to ubatch size
        params.n_ubatch = params.n_batch;

        std::mt19937 rng(params.seed);

        llama_backend_init();
        llama_numa_init(params.numa);

        // load the model
        params.model = model_path;
        llama_init_result llama_init = llama_init_from_gpt_params(params);

        model = std::unique_ptr<llama_model, decltype(&llama_free_model)>(llama_init.model, &llama_free_model);
        ctx = std::unique_ptr<llama_context, decltype(&llama_free)>(llama_init.context, &llama_free);

        if (model == NULL)
        {
            fprintf(stderr, "%s: error: unable to load model\n", __func__);
            throw std::runtime_error("Unable to load model");
        }

        const int n_ctx_train = llama_n_ctx_train(model.get());
        const int n_ctx = llama_n_ctx(ctx.get());

        if (llama_model_has_encoder(model.get()) && llama_model_has_decoder(model.get()))
        {
            fprintf(stderr, "%s: error: computing embeddings in encoder-decoder models is not supported\n", __func__);
            throw std::runtime_error("Computing embeddings in encoder-decoder models is not supported");
        }

        if (n_ctx > n_ctx_train)
        {
            fprintf(stderr, "%s: warning: model was trained on only %d context tokens (%d specified)\n",
                    __func__, n_ctx_train, n_ctx);
        }

        // max batch size
        GGML_ASSERT(params.n_batch >= params.n_ctx);
    }

    ~EmbeddingModel()
    {
        llama_backend_free();
    }

    void embed(const std::vector<std::string> &prompts, float *embeddings)
    {
        // max batch size
        const uint64_t n_batch = params.n_batch;
        GGML_ASSERT(params.n_batch >= params.n_ctx);

        // tokenize the prompts and trim
        std::vector<std::vector<int32_t>> inputs;
        for (const auto &prompt : prompts)
        {
            auto inp = ::llama_tokenize(model.get(), prompt, true, false);
            if (inp.size() > n_batch)
            {
                fprintf(stderr, "%s: error: number of tokens in input line (%lld) exceeds batch size (%lld), increase batch size and re-run\n",
                        __func__, (long long int)inp.size(), (long long int)n_batch);
                throw std::runtime_error("Number of tokens in input line exceeds batch size");
            }
            inputs.push_back(inp);
        }

        // check if the last token is SEP
        // it should be automatically added by the tokenizer when 'tokenizer.ggml.add_eos_token' is set to 'true'
        for (auto &inp : inputs)
        {
            if (inp.empty() || inp.back() != llama_token_sep(model.get()))
            {
                fprintf(stderr, "%s: warning: last token in the prompt is not SEP\n", __func__);
                fprintf(stderr, "%s:          'tokenizer.ggml.add_eos_token' should be set to 'true' in the GGUF header\n", __func__);
            }
        }

        // initialize batch
        const int n_prompts = prompts.size();
        struct llama_batch batch = llama_batch_init(n_batch, 0, 1);

        const enum llama_pooling_type pooling_type = llama_pooling_type(ctx.get());

        // count number of embeddings
        int n_embd_count = 0;
        if (pooling_type == LLAMA_POOLING_TYPE_NONE)
        {
            for (int k = 0; k < n_prompts; k++)
            {
                n_embd_count += inputs[k].size();
            }
        }
        else
        {
            n_embd_count = n_prompts;
        }

        // allocate output
        const int n_embd = llama_n_embd(model.get());

        // break into batches
        int e = 0; // number of embeddings already stored
        int s = 0; // number of prompts in current batch
        for (int k = 0; k < n_prompts; k++)
        {
            // clamp to n_batch tokens
            auto &inp = inputs[k];

            const uint64_t n_toks = inp.size();

            // encode if at capacity
            if (batch.n_tokens + n_toks > n_batch)
            {
                float *output = embeddings + e * n_embd;
                batch_decode(ctx.get(), batch, output, s, n_embd, params.embd_normalize);
                e += pooling_type == LLAMA_POOLING_TYPE_NONE ? batch.n_tokens : s;
                s = 0;
                llama_batch_clear(batch);
            }

            // add to batch
            batch_add_seq(batch, inp, s);
            s += 1;
        }

        // final batch
        float *output = embeddings + e * n_embd;
        batch_decode(ctx.get(), batch, output, s, n_embd, params.embd_normalize);

        llama_batch_free(batch);
    }

    const std::size_t get_emb_dim()
    {
        return llama_n_embd(model.get());
    }

private:
    std::unique_ptr<llama_model, decltype(&llama_free_model)> model{nullptr, &llama_free_model};
    std::unique_ptr<llama_context, decltype(&llama_free)> ctx{nullptr, &llama_free};
    ;
    gpt_params params = gpt_params();
};

// Wrap the main function in a C-style function to avoid name mangling
extern "C"
{

    EmbeddingModel *EmbeddingModel_new(const char *model_path)
    {
        std::string model_str(model_path);
        return new EmbeddingModel(model_str);
    }

    void EmbeddingModel_free(EmbeddingModel *embedding_model)
    {
        delete embedding_model;
    }

    std::size_t EmbeddingModel_get_emb_dim(EmbeddingModel *embedding_model)
    {
        return embedding_model->get_emb_dim();
    }

    void EmbeddingModel_embed(EmbeddingModel *embedding_model, const char **prompts, const int n_prompts, float *output)
    {
        std::vector<std::string> prompts_vec;
        for (int i = 0; i < n_prompts; i++)
        {
            prompts_vec.emplace_back(std::string(prompts[i]));
        }

        embedding_model->embed(prompts_vec, output);
    }
}
