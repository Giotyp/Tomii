#ifndef WORDCOUNT_H
#define WORDCOUNT_H

#include <stddef.h>
#include <stdint.h>

/*
 * Word-count MapReduce plugin for the Τομί mapreduce example.
 *
 * Pipeline:
 *   generate_shard (×N) -- map phase source
 *       ↓ 1:1 $res
 *   map_tokens      (×N) -- map: shard → local count table
 *       ↓ variadic $res (all N results)
 *   reduce_all      (×1) -- reduce: sum tables, write word totals
 *
 * Opaque handles: Vocab*, Shard*, TokenCounts* are all exposed as void*.
 */

/* -----------------------------------------------------------------------
 * Τομί-exported functions (annotated for the converter)
 * ----------------------------------------------------------------------- */

/* Build a fixed vocabulary of VOCAB_SIZE words. Used as a shared $ref. */
// @tomii_export
void* make_vocabulary(void);

/* Resolve and truncate the output file; returns a malloc'd C string. */
// @tomii_export(free=free_string)
char* get_out_file(const char* env_var, const char* out_file);

/* Generate a shard of num_tokens tokens drawn from the vocabulary.
 * Each call gets a unique per-shard seed (base_seed XOR monotonic counter)
 * so parallel instances produce different distributions. */
// @tomii_export
void* generate_shard(void* vocab, uint64_t base_seed, size_t num_tokens);

/* Map a shard to a local word-count table (uint32_t[vocab_size]). */
// @tomii_export
void* map_tokens(void* shard, size_t vocab_size);

/* Variadic reduce: sum all local count tables and write "word total\n"
 * lines to the file at `path'. */
// @tomii_export(variadic)
void reduce_all(const char* path, void* vocab, void** parts, size_t num_parts);

/* -----------------------------------------------------------------------
 * Memory management helpers (loaded by generated wrappers, not graph nodes)
 * ----------------------------------------------------------------------- */

void free_string(void* s);

#endif /* WORDCOUNT_H */
