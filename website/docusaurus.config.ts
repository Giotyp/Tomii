import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

const config: Config = {
  title: 'Tomii',
  tagline: 'A task-graph framework for prototyping low-latency streaming pipelines',
  favicon: 'img/favicon.ico',

  future: {
    v4: true,
  },

  url: 'https://tomii.dev',
  baseUrl: '/',

  // GitHub pages deployment config.
  organizationName: 'Giotyp',
  projectName: 'Tomii',
  trailingSlash: false,

  onBrokenLinks: 'throw',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          editUrl: 'https://github.com/Giotyp/Tomii/tree/main/website/',
        },
        blog: false,
        theme: {
          customCss: ['./src/css/theme.css', './src/css/custom.css'],
        },
      } satisfies Preset.Options,
    ],
  ],

  themes: [
    [
      require.resolve('@easyops-cn/docusaurus-search-local'),
      {
        hashed: true,
        indexBlog: false,
        highlightSearchTermsOnTargetPage: true,
        searchResultLimits: 8,
      },
    ],
  ],

  themeConfig: {
    image: 'img/tomii-social-card.png',
    colorMode: {
      defaultMode: 'light',
      disableSwitch: false,
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'Tomii',
      logo: {
        alt: 'Tomii logo',
        src: 'img/logo.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'overview',
          position: 'left',
          label: 'Overview',
        },
        {
          type: 'docSidebar',
          sidebarId: 'guide',
          position: 'left',
          label: 'Guide',
        },
        {
          type: 'docSidebar',
          sidebarId: 'reference',
          position: 'left',
          label: 'Reference',
        },
        {
          href: 'https://github.com/Giotyp/Tomii',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'light',
      links: [
        {
          title: 'Project',
          items: [
            {label: 'GitHub', href: 'https://github.com/Giotyp/Tomii'},
            {
              label: 'License (Apache 2.0)',
              href: 'https://github.com/Giotyp/Tomii/blob/main/LICENSE',
            },
            {
              label: 'Roadmap',
              href: 'https://github.com/Giotyp/Tomii/blob/main/ROADMAP.md',
            },
          ],
        },
        {
          title: 'Docs',
          items: [
            {label: 'Overview', to: '/docs/overview/what-is-tomii'},
            {label: 'Guide', to: '/docs/guide/getting-started/installation'},
            {label: 'Reference', to: '/docs/reference/python-api'},
          ],
        },
        {
          title: 'Packages',
          items: [
            {label: 'tomii-rt on PyPI', href: 'https://pypi.org/project/tomii-rt/'},
            {label: 'tomii-core on crates.io', href: 'https://crates.io/crates/tomii-core'},
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} the Tomii authors. Apache-2.0 licensed.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.vsDark,
      additionalLanguages: ['rust', 'python', 'json', 'bash', 'toml'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
