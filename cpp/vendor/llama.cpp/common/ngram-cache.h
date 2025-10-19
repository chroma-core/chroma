#pragma once

#include "llama.h"

#include <unordered_map>
#include <string>
#include <vector>

#define LLAMA_NGRAM_MIN    1
#define LLAMA_NGRAM_MAX    4
#define LLAMA_NGRAM_STATIC 2

// Data structures to map n-grams to empirical token probabilities:

struct llama_ngram {
    llama_token tokens[LLAMA_NGRAM_MAX];

    llama_ngram() {
        for (int i = 0; i < LLAMA_NGRAM_MAX; ++i) {
            tokens[i] = -1;
        }
    }

    llama_ngram(const llama_token * input, const int ngram_size) {
        for (int i = 0; i < LLAMA_NGRAM_MAX; ++i) {
            tokens[i] = i < ngram_size ? input[i] : -1;
        }
    }

    bool operator==(const llama_ngram & other) const {
        for (int i = 0; i < LLAMA_NGRAM_MAX; ++i) {
            if (tokens[i] != other.tokens[i]) {
                return false;
            }
        }
        return true;
    }
};

struct llama_token_hash_function {
    size_t operator()(const llama_token token) const {
        // see https://probablydance.com/2018/06/16/fibonacci-hashing-the-optimization-that-the-world-forgot-or-a-better-alternative-to-integer-modulo/
        return token * 11400714819323198485llu;
    }
};

struct llama_ngram_hash_function {
    size_t operator()(const llama_ngram & ngram) const {
        size_t hash = llama_token_hash_function{}(ngram.tokens[0]);
        for (int i = 1; i < LLAMA_NGRAM_MAX; ++i) {
            hash ^= llama_token_hash_function{}(ngram.tokens[i]);
        }
        return hash;
    }
};

// token -> number of times token has been seen
typedef std::unordered_map<llama_token, int32_t> llama_ngram_cache_part;

// n-gram -> empirical distribution of following tokens
typedef std::unordered_map<llama_ngram, llama_ngram_cache_part, llama_ngram_hash_function> llama_ngram_cache;


// Update an ngram cache with tokens.
// ngram_cache:         the cache to modify.
// ngram_min/ngram_max: the min/max size of the ngrams to extract from inp_data.
// inp_data:            the token sequence with which to update ngram_cache.
// nnew:                how many new tokens have been appended to inp_data since the last call to this function.
// print_progress:      whether to print progress to stderr.
//
// In order to get correct results inp_data can ONLY BE APPENDED TO.
// Changes in the middle need a complete rebuild.
void llama_ngram_cache_update(
    llama_ngram_cache & ngram_cache, int ngram_min, int ngram_max, std::vector<llama_token> & inp_data, int nnew, bool print_progress);

// Try to draft tokens from ngram caches.
// inp:                the tokens generated so far.
// draft:              the token sequence to draft. Expected to initially contain the previously sampled token.
// n_draft:            maximum number of tokens to add to draft.
// ngram_min/gram_max: the min/max size of the ngrams in nc_context and nc_dynamic.
// nc_context:         ngram cache based on current context.
// nc_dynamic:         ngram cache based on previous user generations.
// nc_static:          ngram cache generated from a large text corpus, used for validation.
void llama_ngram_cache_draft(
    std::vector<llama_token> & inp, std::vector<llama_token> & draft, int n_draft, int ngram_min, int ngram_max,
    llama_ngram_cache & nc_context, llama_ngram_cache & nc_dynamic, llama_ngram_cache & nc_static);

// Save an ngram cache to a file.
// ngram_cache: the ngram cache to save.
// filename:    the path under which to save the ngram cache.
void llama_ngram_cache_save(llama_ngram_cache & ngram_cache, std::string & filename);

// Load an ngram cache saved with llama_ngram_cache_save.
// filename: the path from which to load the ngram cache.
// returns:  an ngram cache containing the information saved to filename.
llama_ngram_cache llama_ngram_cache_load(std::string & filename);

// Merge two ngram caches.
// ngram_cache_target: the ngram cache to which to add the information from ngram_cache_add.
// ngram_cache_add:    the ngram cache to add to ngram_cache_target.
void llama_ngram_cache_merge(llama_ngram_cache & ngram_cache_target, llama_ngram_cache & ngram_cache_add);
