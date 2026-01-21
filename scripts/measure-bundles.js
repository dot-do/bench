#!/usr/bin/env node

/**
 * Bundle Size Measurement Script
 *
 * Measures:
 * - TypeScript/JS bundle size (types, bindings)
 * - WASM size (if applicable)
 * - Gzipped vs uncompressed
 * - Tree-shaking effectiveness
 */

import { build } from 'esbuild'
import { gzipSize } from 'gzip-size'
import { readFileSync, writeFileSync, existsSync, statSync } from 'fs'
import { join, dirname } from 'path'
import { fileURLToPath } from 'url'

const __dirname = dirname(fileURLToPath(import.meta.url))
const resultsDir = join(__dirname, '..', 'results')

const databases = [
  {
    name: 'db4',
    packages: ['db4ai', '@db4/core', '@db4/query', '@db4/do', '@db4/vortex'],
    hasWasm: false,
    wasmPaths: [],
  },
  {
    name: 'evodb',
    packages: ['@evodb/core', '@evodb/lakehouse', '@evodb/reader', '@evodb/writer'],
    hasWasm: false,
    wasmPaths: [],
  },
  {
    name: 'postgres',
    packages: ['postgres.do', '@dotdo/postgres', '@dotdo/pglite'],
    hasWasm: true,
    wasmPaths: ['node_modules/@electric-sql/pglite/dist/pglite.wasm'],
  },
  {
    name: 'sqlite',
    packages: ['turso.do', '@dotdo/turso', '@dotdo/sqlite'],
    hasWasm: true,
    wasmPaths: ['node_modules/@libsql/client/wasm/libsql.wasm'],
  },
  {
    name: 'duckdb',
    packages: ['duck.do', '@dotdo/duckdb'],
    hasWasm: true,
    wasmPaths: ['node_modules/@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm'],
  },
]

async function measureBundle(entryPoint, name) {
  try {
    const result = await build({
      entryPoints: [entryPoint],
      bundle: true,
      write: false,
      format: 'esm',
      platform: 'browser',
      minify: true,
      treeShaking: true,
      external: ['*.wasm', '*.data'],
    })

    const code = result.outputFiles[0].text
    const uncompressed = Buffer.byteLength(code, 'utf8')
    const compressed = await gzipSize(code)

    return {
      name,
      uncompressed,
      compressed,
      uncompressedKB: (uncompressed / 1024).toFixed(2),
      compressedKB: (compressed / 1024).toFixed(2),
    }
  } catch (error) {
    return {
      name,
      error: error.message,
    }
  }
}

function measureWasm(paths) {
  let totalSize = 0
  const files = []

  for (const path of paths) {
    if (existsSync(path)) {
      const stats = statSync(path)
      totalSize += stats.size
      files.push({ path, size: stats.size, sizeKB: (stats.size / 1024).toFixed(2) })
    }
  }

  return {
    totalSize,
    totalSizeKB: (totalSize / 1024).toFixed(2),
    totalSizeMB: (totalSize / 1024 / 1024).toFixed(2),
    files,
  }
}

async function main() {
  console.log('Measuring bundle sizes...\n')

  const results = {
    timestamp: new Date().toISOString(),
    databases: {},
  }

  for (const db of databases) {
    console.log(`\n=== ${db.name.toUpperCase()} ===`)

    const dbResult = {
      packages: [],
      wasm: null,
      totalJS: { uncompressed: 0, compressed: 0 },
      totalWithWasm: { uncompressed: 0, compressed: 0 },
    }

    // Measure JS bundles
    for (const pkg of db.packages) {
      try {
        const entryPoint = `node_modules/${pkg}`
        const bundle = await measureBundle(entryPoint, pkg)
        dbResult.packages.push(bundle)

        if (!bundle.error) {
          dbResult.totalJS.uncompressed += bundle.uncompressed
          dbResult.totalJS.compressed += bundle.compressed
        }

        console.log(`  ${pkg}: ${bundle.compressedKB || 'error'}KB gzipped`)
      } catch (e) {
        console.log(`  ${pkg}: not found`)
      }
    }

    // Measure WASM
    if (db.hasWasm) {
      dbResult.wasm = measureWasm(db.wasmPaths)
      console.log(`  WASM: ${dbResult.wasm.totalSizeMB}MB`)
    }

    // Calculate totals
    dbResult.totalJS.uncompressedKB = (dbResult.totalJS.uncompressed / 1024).toFixed(2)
    dbResult.totalJS.compressedKB = (dbResult.totalJS.compressed / 1024).toFixed(2)

    dbResult.totalWithWasm.uncompressed = dbResult.totalJS.uncompressed + (dbResult.wasm?.totalSize || 0)
    dbResult.totalWithWasm.compressed = dbResult.totalJS.compressed + (dbResult.wasm?.totalSize || 0) // WASM doesn't compress much
    dbResult.totalWithWasm.uncompressedMB = (dbResult.totalWithWasm.uncompressed / 1024 / 1024).toFixed(2)
    dbResult.totalWithWasm.compressedMB = (dbResult.totalWithWasm.compressed / 1024 / 1024).toFixed(2)

    results.databases[db.name] = dbResult

    console.log(`  Total JS: ${dbResult.totalJS.compressedKB}KB gzipped`)
    console.log(`  Total w/ WASM: ${dbResult.totalWithWasm.compressedMB}MB`)
  }

  // Summary table
  console.log('\n\n=== SUMMARY ===\n')
  console.log('| Database | JS (gzip) | WASM | Total | Lazy-Load? |')
  console.log('|----------|-----------|------|-------|------------|')

  for (const [name, data] of Object.entries(results.databases)) {
    const js = data.totalJS.compressedKB + 'KB'
    const wasm = data.wasm ? data.wasm.totalSizeMB + 'MB' : 'N/A'
    const total = data.totalWithWasm.compressedMB + 'MB'
    const lazy = data.wasm ? 'Yes' : 'N/A'
    console.log(`| ${name.padEnd(8)} | ${js.padEnd(9)} | ${wasm.padEnd(4)} | ${total.padEnd(5)} | ${lazy.padEnd(10)} |`)
  }

  // Write results
  writeFileSync(
    join(resultsDir, 'bundle-sizes.json'),
    JSON.stringify(results, null, 2)
  )
  console.log(`\nResults written to results/bundle-sizes.json`)
}

main().catch(console.error)
