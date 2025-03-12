import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // output: 'standalone',
  experimental: {
    turbo: {
      rules: {
        "*.svg": {
          loaders: ["@svgr/webpack"],
          as: "*.js",
        },
      },
    },
  },
};

export default nextConfig;
