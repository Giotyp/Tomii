#include "wordcount.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* -------------------------------------------------------------------------- *
 * Fixed vocabulary
 * -------------------------------------------------------------------------- */

#define VOCAB_SIZE 8

static const char* WORDS[VOCAB_SIZE] = {
    "apple", "banana", "cherry", "date",
    "elderberry", "fig", "grape", "honeydew",
};

/* -------------------------------------------------------------------------- *
 * Internal types
 * -------------------------------------------------------------------------- */

typedef struct { size_t size; }                        Vocab;
typedef struct { size_t* tokens; size_t len; }         Shard;
typedef struct { uint32_t* counts; size_t vocab_size; } TokenCounts;

/* -------------------------------------------------------------------------- *
 * make_vocabulary
 * -------------------------------------------------------------------------- */

void* make_vocabulary(void) {
    Vocab* v = (Vocab*)malloc(sizeof(Vocab));
    if (!v) return NULL;
    v->size = VOCAB_SIZE;
    return (void*)v;
}

/* -------------------------------------------------------------------------- *
 * get_out_file
 * -------------------------------------------------------------------------- */

char* get_out_file(const char* env_var, const char* out_file) {
    const char* dir = getenv(env_var);
    if (!dir) {
        fprintf(stderr, "get_out_file: env var '%s' not set\n", env_var);
        return NULL;
    }
    size_t len = strlen(dir) + 1 + strlen(out_file) + 1;
    char* path = (char*)malloc(len);
    if (!path) return NULL;
    snprintf(path, len, "%s/%s", dir, out_file);
    FILE* f = fopen(path, "w");
    if (f) fclose(f);
    return path;
}

/* -------------------------------------------------------------------------- *
 * generate_shard
 *
 * Each parallel instance XORs base_seed with an atomic counter so that all
 * N shards are distinct even though they receive the same base_seed argument.
 * LCG parameters from Knuth.
 * -------------------------------------------------------------------------- */

static _Atomic uint64_t g_shard_counter = 0;

void* generate_shard(void* vocab_unused, uint64_t base_seed, size_t num_tokens) {
    (void)vocab_unused;

    uint64_t seed = base_seed
        ^ atomic_fetch_add_explicit(&g_shard_counter, 1, memory_order_relaxed);

    Shard* s = (Shard*)malloc(sizeof(Shard));
    if (!s) return NULL;
    s->tokens = (size_t*)malloc(num_tokens * sizeof(size_t));
    if (!s->tokens) { free(s); return NULL; }
    s->len = num_tokens;

    for (size_t i = 0; i < num_tokens; i++) {
        seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
        s->tokens[i] = (size_t)((seed >> 33) % VOCAB_SIZE);
    }
    return (void*)s;
}

/* -------------------------------------------------------------------------- *
 * map_tokens
 * -------------------------------------------------------------------------- */

void* map_tokens(void* shard_handle, size_t vocab_size) {
    Shard* s = (Shard*)shard_handle;

    TokenCounts* tc = (TokenCounts*)malloc(sizeof(TokenCounts));
    if (!tc) return NULL;
    tc->counts = (uint32_t*)calloc(vocab_size, sizeof(uint32_t));
    if (!tc->counts) { free(tc); return NULL; }
    tc->vocab_size = vocab_size;

    for (size_t i = 0; i < s->len; i++) {
        if (s->tokens[i] < vocab_size)
            tc->counts[s->tokens[i]]++;
    }
    return (void*)tc;
}

/* -------------------------------------------------------------------------- *
 * reduce_all
 *
 * Receives all N TokenCounts* tables (one per map_tokens instance) as a
 * variadic void** array.  Sums counts element-wise and writes one line per
 * word to the result file.
 * -------------------------------------------------------------------------- */

void reduce_all(const char* path, void* vocab_handle,
                void** parts, size_t num_parts) {
    Vocab* vocab = (Vocab*)vocab_handle;
    size_t vs = vocab->size;

    uint64_t* totals = (uint64_t*)calloc(vs, sizeof(uint64_t));
    if (!totals) return;

    for (size_t p = 0; p < num_parts; p++) {
        TokenCounts* tc = (TokenCounts*)parts[p];
        size_t limit = tc->vocab_size < vs ? tc->vocab_size : vs;
        for (size_t w = 0; w < limit; w++)
            totals[w] += tc->counts[w];
    }

    FILE* f = fopen(path, "w");
    if (!f) { free(totals); return; }
    for (size_t w = 0; w < vs; w++)
        fprintf(f, "%s %llu\n", WORDS[w], (unsigned long long)totals[w]);
    fclose(f);
    free(totals);
}

/* -------------------------------------------------------------------------- *
 * Memory management helpers
 * -------------------------------------------------------------------------- */

void free_string(void* s) { free(s); }
