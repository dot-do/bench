import { bench, describe } from 'vitest'

/**
 * Cold Start Benchmarks
 *
 * Measures time from zero state to first query result.
 * Critical for understanding user-facing latency on first request.
 */

describe('Cold Start - Worker', () => {
  // Worker cold start: WASM instantiation + first query

  bench('db4 cold start', async () => {
    const { createDB4Store } = await import('../databases/db4')
    const store = await createDB4Store()
    await store.query('SELECT 1')
    await store.close()
  }, { iterations: 10, warmupIterations: 0 })

  bench('evodb cold start', async () => {
    const { createEvoDBStore } = await import('../databases/evodb')
    const store = await createEvoDBStore()
    await store.query('SELECT 1')
    await store.close()
  }, { iterations: 10, warmupIterations: 0 })

  bench('postgres cold start (PGLite WASM)', async () => {
    const { createPostgresStore } = await import('../databases/postgres')
    const store = await createPostgresStore()
    await store.query('SELECT 1')
    await store.close()
  }, { iterations: 10, warmupIterations: 0 })

  bench('sqlite cold start (libsql WASM)', async () => {
    const { createSQLiteStore } = await import('../databases/sqlite')
    const store = await createSQLiteStore()
    await store.query('SELECT 1')
    await store.close()
  }, { iterations: 10, warmupIterations: 0 })

  bench('duckdb cold start (DuckDB WASM)', async () => {
    const { createDuckDBStore } = await import('../databases/duckdb')
    const store = await createDuckDBStore()
    await store.query('SELECT 1')
    await store.close()
  }, { iterations: 10, warmupIterations: 0 })
})

describe('Cold Start - DO Hibernation Wake', () => {
  // Simulates DO waking from hibernation
  // In real scenario, this would be measured via Worker → DO fetch

  bench('db4 hibernation wake', async () => {
    // Simulate DO state restoration
    const { createDB4Store, restoreFromStorage } = await import('../databases/db4')
    const store = await createDB4Store()
    await restoreFromStorage(store, getMockDOStorage())
    await store.query('SELECT * FROM things LIMIT 1')
  }, { iterations: 10, warmupIterations: 0 })

  // ... similar for other databases
})

function getMockDOStorage() {
  // Mock DO storage for benchmarking
  return new Map<string, ArrayBuffer>()
}
