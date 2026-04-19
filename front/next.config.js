/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  assetPrefix: process.env.BASE_PATH || '',
  basePath: process.env.BASE_PATH || '',
  trailingSlash: true,
  outputFileTracingRoot: __dirname,
  serverExternalPackages: ['openai', 'xxhash-wasm'],
  // ビルド時の ESLint チェックを無効化（lint は別途 `pnpm lint` で実行する運用）
  eslint: {
    ignoreDuringBuilds: true,
  },
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
