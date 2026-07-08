import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  overview: [
    'overview/what-is-tomii',
    'overview/when-to-use',
    'overview/comparison',
    'overview/benchmarks',
    'overview/faq',
  ],
  guide: [
    {
      type: 'category',
      label: 'Getting Started',
      collapsed: false,
      items: [
        'guide/getting-started/installation',
        'guide/getting-started/first-graph',
        'guide/getting-started/running',
      ],
    },
    {
      type: 'category',
      label: 'Building Graphs',
      items: [
        'guide/graphs/nodes-and-vars',
        'guide/graphs/types',
        'guide/graphs/control-flow',
        'guide/graphs/network-sources',
      ],
    },
    {
      type: 'category',
      label: 'Writing Kernels',
      items: [
        'guide/plugins/rust',
        'guide/plugins/c',
        'guide/plugins/python',
        'guide/plugins/polyglot',
      ],
    },
    {
      type: 'category',
      label: 'Tuning and Observability',
      items: [
        'guide/tuning/knobs',
        'guide/tuning/agent-tuning',
        'guide/tuning/observability',
      ],
    },
    'guide/examples',
  ],
  reference: [
    'reference/python-api',
    'reference/json-graph-format',
    'reference/cli',
    'reference/knob-catalog',
    'reference/environment',
  ],
};

export default sidebars;
