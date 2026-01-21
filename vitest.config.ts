import { defineConfig } from 'vitest/config'
import { fileURLToPath } from 'url'
import { dirname, join, resolve } from 'path'

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

const tbDistPath = resolve(__dirname, '../pocs/packages/tigerbeetle-do/dist')

export default defineConfig({
  test: {
    // Include test files
    include: ['tests/**/*.test.ts', 'databases/**/*.test.ts'],
    // Exclude benchmark files from test runs
    exclude: ['**/*.bench.ts'],
  },
  resolve: {
    // Help resolve ESM modules without .js extensions
    extensions: ['.js', '.ts', '.mjs', '.mts', '.jsx', '.tsx', '.json'],
    alias: {
      // Point to submodules directly for tigerbeetle-do
      '@dotdo/poc-tigerbeetle-do/core': join(tbDistPath, 'core'),
      '@dotdo/poc-tigerbeetle-do/types': join(tbDistPath, 'types'),
      '@dotdo/poc-tigerbeetle-do/vfs': join(tbDistPath, 'vfs'),
      '@dotdo/poc-tigerbeetle-do': tbDistPath,
    },
  },
  // Allow importing without .js extensions
  esbuild: {
    format: 'esm',
  },
})
