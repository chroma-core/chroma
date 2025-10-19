#include "llama-grammar.h"

#include "llama-vocab.h"
#include "llama-sampling.h"

#include <algorithm>

// Decodes a UTF-8 string which may end in an incomplete sequence. Adds a terminating 0 for use as
// pointer. If an invalid sequence is encountered, returns `llama_partial_utf8.n_remain == -1`.
std::pair<std::vector<uint32_t>, llama_partial_utf8> decode_utf8(
        const std::string & src,
        llama_partial_utf8 partial_start) {
    static const int      lookup[] = { 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 2, 2, 3, 4 };
    const char          * pos      = src.c_str();
    std::vector<uint32_t> code_points;

    // common english strings have the same number of codepoints and bytes. `+ 1` for the terminating 0.
    code_points.reserve(src.size() + 1);
    uint32_t value    = partial_start.value;
    int      n_remain = partial_start.n_remain;

    // continue previous decode, if applicable
    while (*pos != 0 && n_remain > 0) {
        uint8_t next_byte = static_cast<uint8_t>(*pos);
        if ((next_byte >> 6) != 2) {
            // invalid sequence, abort
            code_points.push_back(0);
            return std::make_pair(std::move(code_points), llama_partial_utf8{ 0, -1 });
        }
        value = (value << 6) + (next_byte & 0x3F);
        ++pos;
        --n_remain;
    }

    if (partial_start.n_remain > 0 && n_remain == 0) {
        code_points.push_back(value);
    }

    // decode any subsequent utf-8 sequences, which may end in an incomplete one
    while (*pos != 0) {
        uint8_t first_byte = static_cast<uint8_t>(*pos);
        uint8_t highbits   = first_byte >> 4;
                n_remain   = lookup[highbits] - 1;

        if (n_remain < 0) {
            // invalid sequence, abort
            code_points.clear();
            code_points.push_back(0);
            return std::make_pair(std::move(code_points), llama_partial_utf8{ 0, n_remain });
        }

        uint8_t mask  = (1 << (7 - n_remain)) - 1;
                value = first_byte & mask;

        ++pos;
        while (*pos != 0 && n_remain > 0) {
            value = (value << 6) + (static_cast<uint8_t>(*pos) & 0x3F);
            ++pos;
            --n_remain;
        }
        if (n_remain == 0) {
            code_points.push_back(value);
        }
    }
    code_points.push_back(0);

    return std::make_pair(std::move(code_points), llama_partial_utf8{ value, n_remain });
}

const llama_grammar_rules & llama_grammar_get_rules(const struct llama_grammar * grammar) {
    return grammar->rules;
}

llama_grammar_stacks & llama_grammar_get_stacks(struct llama_grammar * grammar) {
    return grammar->stacks;
}

// returns true iff pos points to the end of one of the definitions of a rule
static bool llama_grammar_is_end_of_sequence(const llama_grammar_element * pos) {
    switch (pos->type) {
        case LLAMA_GRETYPE_END: return true;  // NOLINT
        case LLAMA_GRETYPE_ALT: return true;  // NOLINT
        default:                return false;
    }
}

// returns true iff chr satisfies the char range at pos (regular or inverse range)
// asserts that pos is pointing to a char range element
static std::pair<bool, const llama_grammar_element *> llama_grammar_match_char(
        const llama_grammar_element * pos,
        const uint32_t                chr) {

    bool found            = false;
    bool is_positive_char = pos->type == LLAMA_GRETYPE_CHAR || pos->type == LLAMA_GRETYPE_CHAR_ANY;

    GGML_ASSERT(is_positive_char || pos->type == LLAMA_GRETYPE_CHAR_NOT); // NOLINT

    do {
        if (pos[1].type == LLAMA_GRETYPE_CHAR_RNG_UPPER) {
            // inclusive range, e.g. [a-z]
            found = found || (pos->value <= chr && chr <= pos[1].value);
            pos += 2;
        } else if (pos->type == LLAMA_GRETYPE_CHAR_ANY) {
            // Any character matches "."
            found = true;
            pos += 1;
        } else {
            // exact char match, e.g. [a] or "a"
            found = found || pos->value == chr;
            pos += 1;
        }
    } while (pos->type == LLAMA_GRETYPE_CHAR_ALT);

    return std::make_pair(found == is_positive_char, pos);
}

// returns true iff some continuation of the given partial UTF-8 sequence could satisfy the char
// range at pos (regular or inverse range)
// asserts that pos is pointing to a char range element
static bool llama_grammar_match_partial_char(
        const llama_grammar_element * pos,
        const llama_partial_utf8      partial_utf8) {
    bool is_positive_char = pos->type == LLAMA_GRETYPE_CHAR || pos->type == LLAMA_GRETYPE_CHAR_ANY;
    GGML_ASSERT(is_positive_char || pos->type == LLAMA_GRETYPE_CHAR_NOT);

    uint32_t partial_value = partial_utf8.value;
    int      n_remain      = partial_utf8.n_remain;

    // invalid sequence or 7-bit char split across 2 bytes (overlong)
    if (n_remain < 0 || (n_remain == 1 && partial_value < 2)) {
        return false;
    }

    // range of possible code points this partial UTF-8 sequence could complete to
    uint32_t low  = partial_value << (n_remain * 6);
    uint32_t high = low | ((1 << (n_remain * 6)) - 1);

    if (low == 0) {
        if (n_remain == 2) {
            low = 1 << 11;
        } else if (n_remain == 3) {
            low = 1 << 16;
        }
    }

    do {
        if (pos[1].type == LLAMA_GRETYPE_CHAR_RNG_UPPER) {
            // inclusive range, e.g. [a-z]
            if (pos->value <= high && low <= pos[1].value) {
                return is_positive_char;
            }
            pos += 2;
        } else if (pos->type == LLAMA_GRETYPE_CHAR_ANY) {
            // Any character matches "."
            return true;
        } else {
            // exact char match, e.g. [a] or "a"
            if (low <= pos->value && pos->value <= high) {
                return is_positive_char;
            }
            pos += 1;
        }
    } while (pos->type == LLAMA_GRETYPE_CHAR_ALT);

    return !is_positive_char;
}

// transforms a grammar pushdown stack into N possible stacks, all ending
// at a character range (terminal element)
static void llama_grammar_advance_stack(
        const llama_grammar_rules  & rules,
        const llama_grammar_stack  & stack,
              llama_grammar_stacks & new_stacks) {
    if (stack.empty()) {
        if (std::find(new_stacks.begin(), new_stacks.end(), stack) == new_stacks.end()) {
            new_stacks.emplace_back(stack);
        }
        return;
    }

    const llama_grammar_element * pos = stack.back();

    switch (pos->type) {
        case LLAMA_GRETYPE_RULE_REF: {
            const size_t                  rule_id = static_cast<size_t>(pos->value);
            const llama_grammar_element * subpos  = rules[rule_id].data();
            do {
                // init new stack without the top (pos)
                llama_grammar_stack new_stack(stack.begin(), stack.end() - 1);
                if (!llama_grammar_is_end_of_sequence(pos + 1)) {
                    // if this rule ref is followed by another element, add that to stack
                    new_stack.push_back(pos + 1);
                }
                if (!llama_grammar_is_end_of_sequence(subpos)) {
                    // if alternate is nonempty, add to stack
                    new_stack.push_back(subpos);
                }
                llama_grammar_advance_stack(rules, new_stack, new_stacks);
                while (!llama_grammar_is_end_of_sequence(subpos)) {
                    // scan to end of alternate def
                    subpos++;
                }
                if (subpos->type == LLAMA_GRETYPE_ALT) {
                    // there's another alternate def of this rule to process
                    subpos++;
                } else {
                    break;
                }
            } while (true);
            break;
        }
        case LLAMA_GRETYPE_CHAR:
        case LLAMA_GRETYPE_CHAR_NOT:
        case LLAMA_GRETYPE_CHAR_ANY:
            if (std::find(new_stacks.begin(), new_stacks.end(), stack) == new_stacks.end()) {
                // only add the stack if it's not a duplicate of one we already have
                new_stacks.emplace_back(stack);
            }
            break;
        default:
            // end of alternate (LLAMA_GRETYPE_END, LLAMA_GRETYPE_ALT) or middle of char range
            // (LLAMA_GRETYPE_CHAR_ALT, LLAMA_GRETYPE_CHAR_RNG_UPPER); stack should never be left on
            // those
            GGML_ABORT("fatal error");
    }
}

// takes a set of possible pushdown stacks on a grammar, which are required to
// be positioned at a character range (see `llama_grammar_advance_stack`), and
// produces the N possible stacks if the given char is accepted at those
// positions
void llama_grammar_accept(
        const llama_grammar_rules  & rules,
        const llama_grammar_stacks & stacks,
        const uint32_t               chr,
              llama_grammar_stacks & new_stacks) {
    new_stacks.clear();

    for (const auto & stack : stacks) {
        if (stack.empty()) {
            continue;
        }

        auto match = llama_grammar_match_char(stack.back(), chr);
        if (match.first) {
            const llama_grammar_element * pos = match.second;

            // update top of stack to next element, if any
            llama_grammar_stack new_stack(stack.begin(), stack.end() - 1);
            if (!llama_grammar_is_end_of_sequence(pos)) {
                new_stack.push_back(pos);
            }
            llama_grammar_advance_stack(rules, new_stack, new_stacks);
        }
    }
}

static llama_grammar_candidates llama_grammar_reject_candidates(
        const llama_grammar_rules  & rules,
        const llama_grammar_stacks & stacks,
        const llama_grammar_candidates & candidates) {
    GGML_ASSERT(!stacks.empty()); // REVIEW

    if (candidates.empty()) {
        return {};
    }

    auto rejects = llama_grammar_reject_candidates_for_stack(rules, stacks.front(), candidates);

    for (size_t i = 1, size = stacks.size(); i < size; ++i) {
        rejects = llama_grammar_reject_candidates_for_stack(rules, stacks[i], rejects);
    }
    return rejects;
}

llama_grammar_candidates llama_grammar_reject_candidates_for_stack(
        const llama_grammar_rules      & rules,
        const llama_grammar_stack      & stack,
        const llama_grammar_candidates & candidates) {

    llama_grammar_candidates rejects;
    rejects.reserve(candidates.size());

    if (stack.empty()) {
        for (const auto & tok : candidates) {
            if (*tok.code_points != 0 || tok.partial_utf8.n_remain != 0) {
                rejects.push_back(tok);
            }
        }
        return rejects;
    }

    const llama_grammar_element * stack_pos = stack.back();

    llama_grammar_candidates next_candidates;
    next_candidates.reserve(candidates.size());

    for (const auto & tok : candidates) {
        if (*tok.code_points == 0) {
            // reached end of full codepoints in token, reject iff it ended in a partial sequence
            // that cannot satisfy this position in grammar
            if (tok.partial_utf8.n_remain != 0 &&
                    !llama_grammar_match_partial_char(stack_pos, tok.partial_utf8)) {
                rejects.push_back(tok);
            }
        } else if (llama_grammar_match_char(stack_pos, *tok.code_points).first) {
            next_candidates.push_back({ tok.index, tok.code_points + 1, tok.partial_utf8 });
        } else {
            rejects.push_back(tok);
        }
    }

    const auto * stack_pos_after = llama_grammar_match_char(stack_pos, 0).second;

    // update top of stack to next element, if any
    llama_grammar_stack stack_after(stack.begin(), stack.end() - 1);
    if (!llama_grammar_is_end_of_sequence(stack_pos_after)) {
        stack_after.push_back(stack_pos_after);
    }
    llama_grammar_stacks next_stacks;
    llama_grammar_advance_stack(rules, stack_after, next_stacks);

    auto next_rejects = llama_grammar_reject_candidates(rules, next_stacks, next_candidates);
    for (const auto & tok : next_rejects) {
        rejects.push_back({ tok.index, tok.code_points - 1, tok.partial_utf8 });
    }

    return rejects;
}

static bool llama_grammar_detect_left_recursion(
        const llama_grammar_rules & rules,
        size_t rule_index,
        std::vector<bool> * rules_visited,
        std::vector<bool> * rules_in_progress,
        std::vector<bool> * rules_may_be_empty) {
    if ((*rules_in_progress)[rule_index]) {
        return true;
    }

    (*rules_in_progress)[rule_index] = true;

    const llama_grammar_rule & rule = rules[rule_index];

    // First check if the rule might produce the empty string. This could be done combined with the second
    // step but it's more readable as two steps.
    bool at_rule_start = true;
    for (size_t i = 0; i < rule.size(); i++) {
        if (llama_grammar_is_end_of_sequence(&rule[i])) {
            if (at_rule_start) {
                (*rules_may_be_empty)[rule_index] = true;
                break;
            }
            at_rule_start = true;
        } else {
            at_rule_start = false;
        }
    }

    // Second, recurse into leftmost nonterminals (or next-leftmost as long as the previous nonterminal may
    // be empty)
    bool recurse_into_nonterminal = true;
    for (size_t i = 0; i < rule.size(); i++) {
        if (rule[i].type == LLAMA_GRETYPE_RULE_REF && recurse_into_nonterminal) {
            if (llama_grammar_detect_left_recursion(rules, (size_t)rule[i].value, rules_visited, rules_in_progress, rules_may_be_empty)) {
                return true;
            }
            if (!((*rules_may_be_empty)[(size_t)rule[i].value])) {
                recurse_into_nonterminal = false;
            }
        } else if (llama_grammar_is_end_of_sequence(&rule[i])) {
            recurse_into_nonterminal = true;
        } else {
            recurse_into_nonterminal = false;
        }
    }

    (*rules_in_progress)[rule_index] = false;
    (*rules_visited)[rule_index] = true;
    return false;
}

//
// grammar - external
//

struct llama_grammar * llama_grammar_init_impl(
            const llama_grammar_element ** rules,
                                 size_t    n_rules,
                                 size_t    start_rule_index) {
    const llama_grammar_element * pos;

    // copy rule definitions into vectors
    llama_grammar_rules vec_rules(n_rules);
    for (size_t i = 0; i < n_rules; i++) {
        for (pos = rules[i]; pos->type != LLAMA_GRETYPE_END; pos++) {
            vec_rules[i].push_back(*pos);
        }
        vec_rules[i].push_back({LLAMA_GRETYPE_END, 0});
    }

    // Check for left recursion
    std::vector<bool> rules_visited(n_rules);
    std::vector<bool> rules_in_progress(n_rules);
    std::vector<bool> rules_may_be_empty(n_rules);
    for (size_t i = 0; i < n_rules; i++) {
        if (rules_visited[i]) {
            continue;
        }
        if (llama_grammar_detect_left_recursion(vec_rules, i, &rules_visited, &rules_in_progress, &rules_may_be_empty)) {
            LLAMA_LOG_ERROR("unsupported grammar, left recursion detected for nonterminal at index %zu", i);
            return nullptr;
        }
    }

    // loop over alternates of start rule to build initial stacks
    llama_grammar_stacks stacks;
    pos = vec_rules[start_rule_index].data();
    do {
        llama_grammar_stack stack;
        if (!llama_grammar_is_end_of_sequence(pos)) {
            // if alternate is nonempty, add to stack
            stack.push_back(pos);
        }
        llama_grammar_advance_stack(vec_rules, stack, stacks);
        while (!llama_grammar_is_end_of_sequence(pos)) {
            // scan to end of alternate def
            pos++;
        }
        if (pos->type == LLAMA_GRETYPE_ALT) {
            // there's another alternate def of this rule to process
            pos++;
        } else {
            break;
        }
    } while (true);

    // Important: vec_rules has to be moved here, not copied, because stacks contains
    // pointers to elements of vec_rules. If vec_rules were copied into llama_grammar
    // then the pointers would be invalidated when the local vec_rules goes out of scope.
    return new llama_grammar{ std::move(vec_rules), std::move(stacks), {} };
}

void llama_grammar_free_impl(struct llama_grammar * grammar) {
    delete grammar;
}

struct llama_grammar * llama_grammar_copy_impl(const struct llama_grammar * grammar) {
    llama_grammar * result = new llama_grammar{ grammar->rules, grammar->stacks, grammar->partial_utf8 };

    // redirect elements in stacks to point to new rules
    for (size_t is = 0; is < result->stacks.size(); is++) {
        for (size_t ie = 0; ie < result->stacks[is].size(); ie++) {
            for (size_t ir0 = 0; ir0 < grammar->rules.size(); ir0++) {
                for (size_t ir1 = 0; ir1 < grammar->rules[ir0].size(); ir1++) {
                    if (grammar->stacks[is][ie] == &grammar->rules[ir0][ir1]) {
                         result->stacks[is][ie]  =  &result->rules[ir0][ir1];
                    }
                }
            }
        }
    }

    return result;
}

void llama_grammar_sample_impl(const struct llama_grammar * grammar, const struct llama_vocab * vocab, const struct llama_sampling * smpl, llama_token_data_array * candidates) {
    GGML_ASSERT(grammar);
    GGML_ASSERT(vocab);

    int64_t t_start_sample_us = ggml_time_us();

    bool allow_eog = false;
    for (const auto & stack : grammar->stacks) {
        if (stack.empty()) {
            allow_eog = true;
            break;
        }
    }

    std::vector<std::pair<std::vector<uint32_t>, llama_partial_utf8>> candidates_decoded;
    candidates_decoded.reserve(candidates->size);

    llama_grammar_candidates candidates_grammar;
    candidates_grammar.reserve(candidates->size);

    for (size_t i = 0; i < candidates->size; ++i) {
        const llama_token id      = candidates->data[i].id;
        const std::string & piece = vocab->cache_token_to_piece.at(id);

        if (llama_token_is_eog_impl(*vocab, id)) {
            if (!allow_eog) {
                candidates->data[i].logit = -INFINITY;
            }
        } else if (piece.empty() || piece[0] == 0) {
            candidates->data[i].logit = -INFINITY;
        } else {
            candidates_decoded.push_back(decode_utf8(piece, grammar->partial_utf8));
            candidates_grammar.push_back({ i, candidates_decoded.back().first.data(), candidates_decoded.back().second });
        }
    }

    const auto rejects = llama_grammar_reject_candidates(grammar->rules, grammar->stacks, candidates_grammar);
    for (const auto & reject : rejects) {
        candidates->data[reject.index].logit = -INFINITY;
    }

    smpl->t_sample_us += ggml_time_us() - t_start_sample_us;
}

void llama_grammar_accept_token_impl(struct llama_grammar * grammar, const struct llama_vocab * vocab, const struct llama_sampling * smpl, llama_token token) {
    const int64_t t_start_sample_us = ggml_time_us();

    if (llama_token_is_eog_impl(*vocab, token)) {
        for (const auto & stack : grammar->stacks) {
            if (stack.empty()) {
                return;
            }
        }
        GGML_ABORT("fatal error");
    }

    const std::string & piece = vocab->cache_token_to_piece.at(token);

    // Note terminating 0 in decoded string
    const auto   decoded     = decode_utf8(piece, grammar->partial_utf8);
    const auto & code_points = decoded.first;

    llama_grammar_stacks tmp_new_stacks;
    for (auto it = code_points.begin(), end = code_points.end() - 1; it != end; ++it) {
        llama_grammar_accept(grammar->rules, grammar->stacks, *it, tmp_new_stacks);
        grammar->stacks = tmp_new_stacks;
    }

    grammar->partial_utf8 = decoded.second;
    GGML_ASSERT(!grammar->stacks.empty());

    smpl->t_sample_us += ggml_time_us() - t_start_sample_us;
}
