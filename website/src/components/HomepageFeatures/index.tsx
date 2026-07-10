import React from 'react';
import Link from '@docusaurus/Link';
import CodeBlock from '@theme/CodeBlock';

import styles from './styles.module.css';

const GRAPH_SNIPPET = `{
  "name": "vec_mat",
  "function": "vec_to_mat",
  "factor": "num_nodes",
  "args": [
    { "type": "$res",
      "predecessor": { "name": "gen_vec" } },
    { "type": "$barrier",
      "predecessor": { "name": "compute_fft" } }
  ]
}`;

const RUST_SNIPPET = `#[tomii_export]
pub fn generate_vector(n: usize) -> Vec<Complex32> {
    functions::generate_vector(n)
}`;

const C_SNIPPET = `// @tomii_export(out_len=n, free=free_vector)
complex_f32* generate_vector(size_t n);`;

const PYTHON_SNIPPET = `@tomii.export
def compute_fft(v: np.ndarray) -> np.ndarray:
    return np.fft.fft(v)`;

const KNOB_SNIPPET = `{
  "name": "workers",
  "cli": "--workers",
  "role": "perf",
  "description": "Rayon worker threads (match physical cores)",
  "search_hint": "unimodal; binary search 1–physical_cores",
  "domain": { "kind": "int", "min": 1, "max": 128,
              "scale": "pow2" }
}`;

type Pillar = {
  number: string;
  title: string;
  body: React.ReactNode;
  link: {to: string; label: string};
  code: React.ReactNode;
};

const PILLARS: Pillar[] = [
  {
    number: '01',
    title: 'Graphs are data',
    body: (
      <>
        The topology is pure JSON: nodes, data dependencies (<code>$res</code>),
        barriers, and network sources (<code>$network</code>) as first-class
        argument types. The same compiled graph replays across up to 64
        concurrent frame slots, with O(1) generational reset between frames;
        no per-frame graph reconstruction.
      </>
    ),
    link: {to: '/docs/guide/graphs/nodes-and-vars', label: 'Building graphs'},
    code: <CodeBlock language="json">{GRAPH_SNIPPET}</CodeBlock>,
  },
  {
    number: '02',
    title: 'Kernels are polyglot',
    body: (
      <>
        Annotate a function in Rust, C, or Python and the build step generates
        the wrapper and registry entry. All three languages compose in one
        graph, referenced by name; the runtime never knows the kernel language.
      </>
    ),
    link: {to: '/docs/guide/plugins/polyglot', label: 'One DAG, three languages'},
    code: (
      <>
        <CodeBlock language="rust">{RUST_SNIPPET}</CodeBlock>
        <CodeBlock language="c">{C_SNIPPET}</CodeBlock>
        <CodeBlock language="python">{PYTHON_SNIPPET}</CodeBlock>
      </>
    ),
  },
  {
    number: '03',
    title: 'The runtime is machine-readable',
    body: (
      <>
        Every tuning knob ships with a type, a domain, and a search hint
        (<code>--list-knobs-json</code>); every graph validates against a
        published schema. An optimizer — random search, Bayesian, or an LLM —
        can enumerate, evaluate, and iterate without recompilation. In our
        4-arm tuning benchmark over a 14-million-cell knob space, random
        search found 1 valid configuration in 50 trials; the verifier-gated
        agent stayed valid in 41 and won best-trial on all three workloads.
      </>
    ),
    link: {to: '/docs/guide/tuning/agent-tuning', label: 'Agent-driven tuning'},
    code: <CodeBlock language="json">{KNOB_SNIPPET}</CodeBlock>,
  },
];

function PillarBlock({pillar, flipped}: {pillar: Pillar; flipped: boolean}) {
  return (
    <div className={flipped ? styles.pillarFlipped : styles.pillar}>
      <div className={styles.pillarText}>
        <span className={styles.pillarNumber} aria-hidden="true">
          {pillar.number}
        </span>
        <h2 className={styles.pillarTitle}>{pillar.title}</h2>
        <p className={styles.pillarBody}>{pillar.body}</p>
        <Link className={styles.pillarLink} to={pillar.link.to}>
          {pillar.link.label} →
        </Link>
      </div>
      <div className={styles.pillarCode}>{pillar.code}</div>
    </div>
  );
}

export default function HomepageFeatures(): React.ReactNode {
  return (
    <section className={styles.pillars}>
      <div className="container">
        {PILLARS.map((pillar, i) => (
          <PillarBlock key={pillar.number} pillar={pillar} flipped={i % 2 === 1} />
        ))}
      </div>
    </section>
  );
}
