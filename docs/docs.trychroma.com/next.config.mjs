/** @type {import('next').NextConfig} */
const nextConfig = {
  webpack(config, { isServer }) {
    config.module.rules.push({
      test: /\.svg$/,
      use: [
        {
          loader: "@svgr/webpack",
          options: {
            icon: true,
          },
        },
      ],
    });

    config.externals = [
      ...(config.externals || []),
      "@xenova/transformers",
      "chromadb",
    ];

    return config;
  },
};

export default nextConfig;
