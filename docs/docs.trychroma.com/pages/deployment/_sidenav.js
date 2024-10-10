export const items = [
    // {
    //     title: 'Runtimes',
    //     links: [
    //       { href: '/deployment/in-memory', children: 'In-memory' },
    //       { href: '/deployment/persistent-client', children: 'Persistent' },
    //       { href: '/deployment/docker', children: 'Docker' },
    //       { href: '/deployment/kubernetes', children: 'Kubernetes' },
    //     ]
    // },
    {
        title: "Chroma Server",
        links: [
            { href: '/deployment/client-server-mode', children: "Client/Server Mode"},
            { href: '/deployment/thin-client', children: "Python Thin-Client"},
        ]
    },
    {
        title: 'Containers',
        links: [
            { href: '/deployment/docker', children: 'Docker' },
        ]
    },
    {
        title: 'Cloud Providers',
        links: [
          { href: '/deployment/aws', children: 'AWS' },
          // { href: '/deployment/gcp', children: 'GCP' },
          // { href: '/deployment/azure', children: 'Azure' },
        ]
    },
    {
      title: 'Administration',
      links: [
          { href: '/deployment/performance', children: "🚀 Performance"},
        // { href: '/deployment/logging', children: 'Logging' },
        { href: '/deployment/observability', children: '👀 Observability' },
        { href: '/deployment/migration', children: '✈️ Migration' },
        // { href: '/deployment/security', children: 'Security' },
        // { href: '/deployment/encryption', children: 'Encryption' },
        { href: '/deployment/auth', children: '🔒 Auth' },
        // { href: '/deployment/performance', children: 'Performance' },
      ]
    },

  ];
