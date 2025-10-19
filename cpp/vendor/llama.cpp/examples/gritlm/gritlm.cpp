#include "common.h"
#include "llama.h"

#include <string>
#include <vector>

// #define GRIT_DEBUG

static std::vector<std::vector<float>> encode(llama_context * ctx, const std::vector<std::string> & sentences, const std::string & instruction) {
    std::vector<std::vector<float>> result;

    const llama_model * mdl = llama_get_model(ctx);

    llama_batch batch = llama_batch_init(llama_n_batch(ctx), 0, 1);

    for (uint64_t i = 0; i < sentences.size(); i++) {
        llama_batch_clear(batch);

        const std::string input_string = instruction + sentences[i];

        std::vector<llama_token> inputs = llama_tokenize(mdl, input_string, true, false);

        const int32_t n_toks = inputs.size();

        // GritLM seems to have EOS = ""
        // https://github.com/ContextualAI/gritlm/blob/92025b16534712b31b3c4aaaf069350e222bd5f8/gritlm/gritlm.py#L18
        // inputs.push_back(llama_token_eos(mdl));

        // we want to ignore instruction tokens for mean pooling
        const int32_t n_inst = llama_tokenize(mdl, instruction, true, false).size();

#ifdef GRIT_DEBUG
        // debug tokens - should be matching as referenced in the GritLM sample
        std::for_each(inputs.begin(), inputs.end(), [&ctx](llama_token t) {
            std::printf("[%u:%s]", t, llama_token_to_piece(ctx, t).c_str());
        });
        std::printf("\n");
#endif

        // add input to batch (this increments n_tokens)
        for (int32_t j = 0; j < n_toks; j++) {
            llama_batch_add(batch, inputs[j], j, { 0 }, j >= n_inst);
        }

        // clear previous kv_cache values (irrelevant for embeddings)
        llama_kv_cache_clear(ctx);
        llama_set_embeddings(ctx, true);
        llama_set_causal_attn(ctx, false);

        // run model
        llama_decode(ctx, batch);

        // get embedding dimensions
        uint64_t n_embd = llama_n_embd(mdl);

        // allocate embedding output
        std::vector<float> emb_unorm(n_embd, 0.0f);

        // sum up all token embeddings
        for (int32_t k = n_inst; k < n_toks; k++) {
            float * emb = llama_get_embeddings_ith(ctx, k);
            for (uint64_t j = 0; j < n_embd; j++) {
                emb_unorm[j] += emb[j];
            }
        }

        // divide by number of tokens (mean pooling)
        {
            const uint64_t n_sent = n_toks - n_inst;

            for (uint64_t j = 0; j < n_embd; j++) {
                emb_unorm[j] /= n_sent;
            }
        }

        std::vector<float> emb_norm(emb_unorm.size());
        llama_embd_normalize(emb_unorm.data(), emb_norm.data(), n_embd);
        result.push_back(emb_norm);

#ifdef GRIT_DEBUG
        // print out emb_norm
        std::printf("embedding %ld: ", i);
        for (uint64_t j = 0; j < n_embd; j++) {
            std::printf("%.5f ", emb_norm[j]);
        }
        std::printf("\n\n");
#endif
    }

    llama_batch_free(batch);

    return result;
}

static std::string generate(llama_context * ctx, const std::string & prompt, bool stream) {
    std::string result;

    const llama_model * mdl = llama_get_model(ctx);
    llama_token eos_token = llama_token_eos(mdl);

    llama_kv_cache_clear(ctx);
    llama_set_embeddings(ctx, false);
    llama_set_causal_attn(ctx, true);

    llama_batch bat = llama_batch_init(llama_n_batch(ctx), 0, 1);

    std::vector<llama_token> inputs = llama_tokenize(mdl, prompt, false, true);
    int32_t i_current_token = 0;

    while (true) {
        llama_batch_clear(bat);
        auto n_inputs = (int32_t)inputs.size();
        for (int32_t i = 0; i < n_inputs; i++) {
            llama_batch_add(bat, inputs[i], i_current_token++, { 0 }, i == n_inputs - 1);
        }
        inputs.clear();

        llama_decode(ctx, bat);
        auto logits = llama_get_logits_ith(ctx, bat.n_tokens - 1);

        auto candidates = std::vector<llama_token_data>(llama_n_vocab(mdl));
        auto n_candidates = (int32_t)candidates.size();
        for (int32_t token = 0; token < n_candidates; token++) {
            candidates[token] = llama_token_data{ token, logits[token], 0.0f };
        }
        auto candidates_p = llama_token_data_array{ candidates.data(), candidates.size(), false };

        llama_token token = llama_sample_token_greedy(ctx, &candidates_p);
        if (token == eos_token) {
            break;
        }

        std::string piece = llama_token_to_piece(ctx, token);
        if (stream) {
            std::printf("%s", piece.c_str());
            std::fflush(stdout);
        }

        inputs.push_back(token);

        result += piece;
    }

    if (stream) {
        std::printf("\n");
    }

    llama_batch_free(bat);

    return result;
}

static std::string gritlm_instruction(const std::string & instruction) {
    return !instruction.empty() ? "<|user|>\n" + instruction + "\n<|embed|>\n" : "<|embed|>\n";
}

int main(int argc, char * argv[]) {
    gpt_params params;

    if (!gpt_params_parse(argc, argv, params)) {
        gpt_params_print_usage(argc, argv, params);
        return 1;
    }

    llama_model_params mparams = llama_model_params_from_gpt_params(params);
    llama_context_params cparams = llama_context_params_from_gpt_params(params);

    llama_backend_init();

    llama_model * mdl = llama_load_model_from_file(params.model.c_str(), mparams);

    // create generation context
    llama_context * ctx = llama_new_context_with_model(mdl, cparams);

    // ### Embedding/Representation ###
    // samples taken from: https://github.com/ContextualAI/gritlm#basic
    {
        const std::string instruction = "Given a scientific paper title, retrieve the paper's abstract";

        const std::vector<std::string> queries = {
            "Bitcoin: A Peer-to-Peer Electronic Cash System",
            "Generative Representational Instruction Tuning",
        };

        const std::vector<std::string> documents = {
            "A purely peer-to-peer version of electronic cash would allow online payments to be sent directly from one party to another without going through a financial institution. Digital signatures provide part of the solution, but the main benefits are lost if a trusted third party is still required to prevent double-spending. We propose a solution to the double-spending problem using a peer-to-peer network. The network timestamps transactions by hashing them into an ongoing chain of hash-based proof-of-work, forming a record that cannot be changed without redoing the proof-of-work. The longest chain not only serves as proof of the sequence of events witnessed, but proof that it came from the largest pool of CPU power. As long as a majority of CPU power is controlled by nodes that are not cooperating to attack the network, they'll generate the longest chain and outpace attackers. The network itself requires minimal structure. Messages are broadcast on a best effort basis, and nodes can leave and rejoin the network at will, accepting the longest proof-of-work chain as proof of what happened while they were gone.",
            "All text-based language problems can be reduced to either generation or embedding. Current models only perform well at one or the other. We introduce generative representational instruction tuning (GRIT) whereby a large language model is trained to handle both generative and embedding tasks by distinguishing between them through instructions. Compared to other open models, our resulting GritLM 7B sets a new state of the art on the Massive Text Embedding Benchmark (MTEB) and outperforms all models up to its size on a range of generative tasks. By scaling up further, GritLM 8X7B outperforms all open generative language models that we tried while still being among the best embedding models. Notably, we find that GRIT matches training on only generative or embedding data, thus we can unify both at no performance loss. Among other benefits, the unification via GRIT speeds up Retrieval-Augmented Generation (RAG) by > 60% for long documents, by no longer requiring separate retrieval and generation models. Models, code, etc. are freely available at https://github.com/ContextualAI/gritlm.",
        };

        // No need to add instruction for retrieval documents
        const std::vector<std::vector<float>> d_rep = encode(ctx, documents, gritlm_instruction(""));
        const std::vector<std::vector<float>> q_rep = encode(ctx, queries,   gritlm_instruction(instruction));

        const int n_embd = llama_n_embd(mdl);

        const float cosine_sim_q0_d0 = llama_embd_similarity_cos(q_rep[0].data(), d_rep[0].data(), n_embd);
        const float cosine_sim_q0_d1 = llama_embd_similarity_cos(q_rep[0].data(), d_rep[1].data(), n_embd);
        const float cosine_sim_q1_d0 = llama_embd_similarity_cos(q_rep[1].data(), d_rep[0].data(), n_embd);
        const float cosine_sim_q1_d1 = llama_embd_similarity_cos(q_rep[1].data(), d_rep[1].data(), n_embd);

        std::printf("Cosine similarity between \"%.50s\" and \"%.50s\" is: %.3f\n", queries[0].c_str(), documents[0].c_str(), cosine_sim_q0_d0);
        std::printf("Cosine similarity between \"%.50s\" and \"%.50s\" is: %.3f\n", queries[0].c_str(), documents[1].c_str(), cosine_sim_q0_d1);
        std::printf("Cosine similarity between \"%.50s\" and \"%.50s\" is: %.3f\n", queries[1].c_str(), documents[0].c_str(), cosine_sim_q1_d0);
        std::printf("Cosine similarity between \"%.50s\" and \"%.50s\" is: %.3f\n", queries[1].c_str(), documents[1].c_str(), cosine_sim_q1_d1);
    }

    // ### Generation ###
    // GritLM models are not finetuned with system prompts, as you can just include system-like instructions together with your user instruction
    {
        const std::string prompt = "<|user|>\nPlease write me a poem about my recent hike of Mt. Fuji at midnight in the style of Shakespeare.\n<|assistant|>\n";
        std::string response = generate(ctx, prompt, true);
    }

    llama_free(ctx);
    llama_free_model(mdl);
    llama_backend_free();

    return 0;
}
