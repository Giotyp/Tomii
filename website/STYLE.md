# Tomii documentation style guide

The target reader is a systems researcher or streaming-pipeline engineer
deciding whether Tomii fits their workload, or stuck mid-task and trying to
get unstuck. Write for them.

## Prose

- One idea per sentence. Short sentences beat long ones.
- Second person, present tense: "you define a graph", not "a graph may be
  defined".
- No marketing vocabulary: "powerful", "seamless", "blazing", "leverage",
  "simply", "easily", "obviously" are banned. If a sentence sounds like vendor
  marketing when read aloud, rewrite it.
- Name the mechanism, not the impression: "dispatches FFT tasks as each UDP
  packet arrives" beats "processes data with ultra-low latency".

## Claims

- Every quantitative claim cites its measurement source (a `bench/` path, an
  example README, or "measured in our evaluation" for paper-only numbers).
- Numbers not reproducible from the repository are labeled as such.
- Losses are stated as plainly as wins. "Tomii does not win this benchmark"
  is a complete, publishable sentence.
- Do not extend a measured claim beyond its workload. A MIMO result is a MIMO
  result.

## Code

- Code snippets come from working examples in the repository, trimmed but not
  invented. If a snippet cannot run, say so.
- Show the command and its expected output together when the output matters.

## Formatting

- Sentence case for headings.
- `code font` for flags, file paths, function names, and JSON keys.
- Tables for enumerable facts; prose for explanations. Never explain inside a
  table cell.
