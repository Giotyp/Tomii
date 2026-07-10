import React, {useState} from 'react';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import CodeBlock from '@theme/CodeBlock';
import HomepageFeatures from '@site/src/components/HomepageFeatures';

import styles from './index.module.css';

const PYTHON_SNIPPET = `import tomii as tm

app = tm.Graph()

buf  = app.var("buf_size", 100)
plan = app.var("fft_planner", func="fft_planner", args=[buf])

gen  = app.node("gen_vec", func="generate_vector",
                factor=200, args=[buf])
fft  = app.node("compute_fft", func="compute_fft",
                factor=200, args=[plan, gen.out()])

app.build(func_path="plugin/src/lib.rs",
          plugin_manifest="plugin/Cargo.toml")
app.run(workers=4, slots=2)`;

const JSON_SNIPPET = `{
  "initializations": [
    { "name": "buf_size",
      "args": [{ "type": "usize", "value": "100" }] },
    { "name": "fft_planner", "function": "fft_planner",
      "args": [{ "type": "$ref", "value": "buf_size" }] }
  ],
  "nodes": [
    { "name": "gen_vec", "function": "generate_vector",
      "factor": "200",
      "args": [{ "type": "$ref", "value": "buf_size" }] },
    { "name": "compute_fft", "function": "compute_fft",
      "factor": "200",
      "args": [
        { "type": "$ref", "value": "fft_planner" },
        { "type": "$res",
          "predecessor": { "name": "gen_vec" } }
      ] }
  ]
}`;

function HeroCode() {
  const [tab, setTab] = useState<'python' | 'json'>('python');
  return (
    <div className={styles.heroCode}>
      <div className={styles.heroCodeTabs}>
        <button
          type="button"
          className={tab === 'python' ? styles.heroCodeTabActive : styles.heroCodeTab}
          onClick={() => setTab('python')}>
          you write
        </button>
        <button
          type="button"
          className={tab === 'json' ? styles.heroCodeTabActive : styles.heroCodeTab}
          onClick={() => setTab('json')}>
          the graph it emits
        </button>
      </div>
      {tab === 'python' ? (
        <CodeBlock language="python">{PYTHON_SNIPPET}</CodeBlock>
      ) : (
        <CodeBlock language="json">{JSON_SNIPPET}</CodeBlock>
      )}
    </div>
  );
}

function Hero() {
  return (
    <header className={styles.hero}>
      <div className={styles.heroGrid} aria-hidden="true" />
      <div className="container">
        <div className={styles.heroInner}>
          <div className={styles.heroText}>
            <h1 className={styles.heroTitle}>
              Prototype streaming task graphs.
              <br />
              <span className={styles.heroTitleAccent}>Run them at native speed.</span>
            </h1>
            <p className={styles.heroSubtitle}>
              Tomii is a Rust runtime for packet-driven streaming pipelines.
              Define the DAG in Python or JSON, mix Rust, C, and Python kernels in one
              graph, replay it across multiple concurrent frames, and let an agent
              tune it, verifier-gated, without recompiling.
            </p>
            <div className={styles.heroButtons}>
              <Link
                className="button button--primary button--lg"
                to="/docs/guide/getting-started/installation">
                Get started
              </Link>
              <Link
                className="button button--outline button--lg"
                to="/docs/overview/what-is-tomii">
                What is Tomii?
              </Link>
              <Link
                className="button button--outline button--lg"
                href="https://github.com/Giotyp/Tomii">
                GitHub
              </Link>
            </div>
          </div>
          <HeroCode />
        </div>
      </div>
    </header>
  );
}

function Tripartite() {
  return (
    <section className={styles.tripartite}>
      <div className="container">
        <h2 className={styles.sectionTitle}>Tripartite Decoupling: Three artifacts, not one codebase</h2>
        <p className={styles.sectionLead}>
          Streaming frameworks usually fuse what you compute, how each kernel is
          implemented, and how execution is organized into a single program.
          Tomii keeps them separate, so each one can change without touching the
          other two.
        </p>
        <div className={styles.tripartiteCards}>
          <div className={styles.tripartiteCard}>
            <span className={styles.tripartiteTag}>graph.json</span>
            <h3>Graph specification</h3>
            <p>
              Declarative, machine-readable, language-agnostic. Nodes,
              dependencies, barriers, network sources; nothing about how
              computation runs.
            </p>
          </div>
          <div className={styles.tripartiteArrow} aria-hidden="true">
            ⊥
          </div>
          <div className={styles.tripartiteCard}>
            <span className={styles.tripartiteTag}>plugin.so</span>
            <h3>Kernel library</h3>
            <p>
              Rust, C, or Python functions compiled independently and loaded at
              runtime. The runtime never knows what language a kernel is
              written in.
            </p>
          </div>
          <div className={styles.tripartiteArrow} aria-hidden="true">
            ⊥
          </div>
          <div className={styles.tripartiteCard}>
            <span className={styles.tripartiteTag}>CLI flags</span>
            <h3>Runtime control</h3>
            <p>
              Workers, slots, scheduler, batching; a bounded, documented
              control surface. Reconfigure execution without rebuilding
              anything.
            </p>
          </div>
        </div>
      </div>
    </section>
  );
}

function Honesty() {
  return (
    <section className={styles.honesty}>
      <div className="container">
        <h2 className={styles.sectionTitle}>Is Tomii for you?</h2>
        <p className={styles.sectionLead}>
          Tomii is a research and prototyping framework with a deliberate
          niche. A fused, application-specific system like Agora will beat it
          on absolute latency (a bounded 3-4x on massive-MIMO), but changing a
          subcarrier count, a scheduling policy, or a kernel in Tomii is a
          graph edit or a CLI flag, not a source change and a recompile.
        </p>
        <div className={styles.honestyColumns}>
          <div className={styles.honestyCol}>
            <h3 className={styles.honestyYes}>Built for</h3>
            <ul>
              <li>
                Packet-driven MIMO-class pipelines: network ingress, FFT/beam
                stages, concurrent frames
              </li>
              <li>
                Multi-frame replay where the same pipeline fires repeatedly on
                arriving data (per-task compute ≥ 16 µs)
              </li>
              <li>
                Agent-driven optimization research on a structured,
                verifier-gated tuning surface
              </li>
            </ul>
          </div>
          <div className={styles.honestyCol}>
            <h3 className={styles.honestyNo}>Not for</h3>
            <ul>
              <li>
                Single-frame micro-task DAGs where dispatch overhead dominates
                — Taskflow and TBB are faster there
              </li>
              <li>
                Dynamic topology: data-dependent fan-out and{' '}
                <code>parallel_for</code> reductions cannot be expressed
              </li>
              <li>
                Production baseband at the absolute latency limit — that is
                what fused systems are for
              </li>
            </ul>
          </div>
        </div>
        <p className={styles.honestyLink}>
          The full performance envelope, including the losses, is documented in{' '}
          <Link to="/docs/overview/when-to-use">When to use Tomii</Link>.
        </p>
      </div>
    </section>
  );
}

function ClosingCta() {
  return (
    <section className={styles.closing}>
      <div className="container">
        <h2 className={styles.closingTitle}>Start with a twelve-line graph.</h2>
        <div className={styles.heroButtons}>
          <Link
            className="button button--primary button--lg"
            to="/docs/guide/getting-started/installation">
            Get started
          </Link>
          <Link
            className="button button--outline button--lg"
            href="https://github.com/Giotyp/Tomii">
            GitHub
          </Link>
        </div>
      </div>
    </section>
  );
}

export default function Home(): React.ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={siteConfig.title}
      description="Tomii is a Rust task-graph runtime for packet-driven streaming pipelines: Python-defined JSON graphs, polyglot Rust/C/Python kernels, multi-slot frame replay, and an agent-tunable runtime surface.">
      <Hero />
      <main>
        <Tripartite />
        <HomepageFeatures />
        <Honesty />
        <ClosingCta />
      </main>
    </Layout>
  );
}
