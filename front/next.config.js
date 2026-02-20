/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  eslint: {
    // aituber-kit 既存コードの lint 警告がビルドを阻害するため
    ignoreDuringBuilds: true,
  },
  assetPrefix: process.env.BASE_PATH || '',
  basePath: process.env.BASE_PATH || '',
  trailingSlash: true,
  outputFileTracingRoot: __dirname,
  env: {
    NEXT_PUBLIC_BASE_PATH: process.env.BASE_PATH || '',
  },
  webpack: (config, { isServer }) => {
    if (!isServer) {
      config.resolve.fallback = {
        ...(config.resolve.fallback ?? {}),
        fs: false,
      }
    }
    return config
  },
}

module.exports = nextConfig
