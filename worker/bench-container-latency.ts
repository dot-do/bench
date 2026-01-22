/**
 * Container vs WASM Latency Benchmark Worker
 *
 * Compares latency characteristics between:
 * - WASM databases running in-Worker (from databases/*.ts)
 * - Containerized databases running in Cloudflare Containers (from containers/*.ts)
 *
 * Measures:
 * - Cold start latency (WASM instantiation vs container startup)
 * - Warm query latency (subsequent queries after initialization)
 * - P50/P99 latency percentiles across multiple iterations
 *
 * Endpoint: POST /benchmark/container-latency
 * Query params: ?database=postgres&size=standard-1&iterations=100
 */

import { Hono } from 'hono'

// Container adapters
import type { Container } from '@cloudflare/containers'
import {
  type ContainerDatabase,
  type ContainerSize,
  createPostgresContainer,
  createClickHouseContainer,
  createMongoContainer,
  createDuckDBContainer,
  createSQLiteContainer,
} from '../containers/index.js'

// WASM database types
import type { PostgresStore } from '../databases/postgres.js'
import type { SQLiteStore } from '../databases/sqlite.js'
import type { DuckDBStore } from '../databases/duckdb.js'

// Environment bindings
interface Env {
  // Container bindings
  POSTGRES_CONTAINER: Container
  CLICKHOUSE_CONTAINER: Container
  MONGO_CONTAINER: Container
  DUCKDB_CONTAINER: Container
  SQLITE_CONTAINER: Container

  // R2 bucket for storing results
  BENCHMARK_RESULTS: R2Bucket
}

// Supported database types
type DatabaseType = 'postgres' | 'sqlite' | 'duckdb' | 'clickhouse' | 'mongo'

// Benchmark operations
type BenchmarkOperation = 'point_lookup' | 'range_scan' | 'insert' | 'aggregate'

// Benchmark result for a single operation
interface OperationResult {
  operation: BenchmarkOperation
  database: DatabaseType
  runtime: 'wasm' | 'container'
  containerSize?: ContainerSize
  iterations: number
  coldStartMs: number
  warmQueryMs: number[]
  stats: {
    min: number
    max: number
    avg: number
    p50: number
    p95: number
    p99: number
  }
}

// Full benchmark result
interface BenchmarkResult {
  timestamp: string
  database: DatabaseType
  containerSize: ContainerSize
  iterations: number
  wasm: OperationResult[]
  container: OperationResult[]
}

// JSONL output line
interface JSONLLine {
  timestamp: string
  database: DatabaseType
  runtime: 'wasm' | 'container'
  containerSize?: ContainerSize
  operation: BenchmarkOperation
  coldStartMs: number
  warmAvgMs: number
  p50Ms: number
  p99Ms: number
  iterations: number
}

const app = new Hono<{ Bindings: Env }>()

/**
 * Calculate percentiles from sorted array of times
 */
function calculateStats(times: number[]): OperationResult['stats'] {
  if (times.length === 0) {
    return { min: 0, max: 0, avg: 0, p50: 0, p95: 0, p99: 0 }
  }

  const sorted = [...times].sort((a, b) => a - b)
  const sum = sorted.reduce((a, b) => a + b, 0)

  return {
    min: sorted[0],
    max: sorted[sorted.length - 1],
    avg: sum / sorted.length,
    p50: sorted[Math.floor(sorted.length * 0.5)],
    p95: sorted[Math.floor(sorted.length * 0.95)],
    p99: sorted[Math.floor(sorted.length * 0.99)],
  }
}

/**
 * Get benchmark queries for each operation type
 */
function getOperationQueries(database: DatabaseType, operation: BenchmarkOperation): {
  setup: string[]
  query: string
  params?: unknown[]
} {
  // SQL-based databases (postgres, sqlite, duckdb)
  if (database === 'postgres' || database === 'sqlite' || database === 'duckdb') {
    const setup = [
      `CREATE TABLE IF NOT EXISTS benchmark_items (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        value INTEGER NOT NULL,
        category TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`,
      // Seed data if needed
      `INSERT INTO benchmark_items (id, name, value, category) VALUES
        ('item-001', 'Item 1', 100, 'A'),
        ('item-002', 'Item 2', 200, 'B'),
        ('item-003', 'Item 3', 300, 'A'),
        ('item-004', 'Item 4', 400, 'B'),
        ('item-005', 'Item 5', 500, 'A')
      ON CONFLICT (id) DO NOTHING`,
    ]

    switch (operation) {
      case 'point_lookup':
        return { setup, query: `SELECT * FROM benchmark_items WHERE id = $1`, params: ['item-001'] }
      case 'range_scan':
        return { setup, query: `SELECT * FROM benchmark_items WHERE category = $1 ORDER BY value`, params: ['A'] }
      case 'insert':
        return {
          setup,
          query: `INSERT INTO benchmark_items (id, name, value, category) VALUES ($1, $2, $3, $4)
                  ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value`,
          params: [`item-${Date.now()}`, 'Benchmark Item', Math.floor(Math.random() * 1000), 'TEST'],
        }
      case 'aggregate':
        return { setup, query: `SELECT category, COUNT(*) as count, AVG(value) as avg_value FROM benchmark_items GROUP BY category` }
    }
  }

  // ClickHouse (analytical queries)
  if (database === 'clickhouse') {
    const setup = [
      `CREATE TABLE IF NOT EXISTS benchmark_items (
        id String,
        name String,
        value Int32,
        category String,
        created_at DateTime DEFAULT now()
      ) ENGINE = MergeTree()
      ORDER BY (category, id)`,
      `INSERT INTO benchmark_items (id, name, value, category) VALUES
        ('item-001', 'Item 1', 100, 'A'),
        ('item-002', 'Item 2', 200, 'B'),
        ('item-003', 'Item 3', 300, 'A'),
        ('item-004', 'Item 4', 400, 'B'),
        ('item-005', 'Item 5', 500, 'A')`,
    ]

    switch (operation) {
      case 'point_lookup':
        return { setup, query: `SELECT * FROM benchmark_items WHERE id = $1`, params: ['item-001'] }
      case 'range_scan':
        return { setup, query: `SELECT * FROM benchmark_items WHERE category = $1 ORDER BY value`, params: ['A'] }
      case 'insert':
        return {
          setup,
          query: `INSERT INTO benchmark_items (id, name, value, category) VALUES ($1, $2, $3, $4)`,
          params: [`item-${Date.now()}`, 'Benchmark Item', Math.floor(Math.random() * 1000), 'TEST'],
        }
      case 'aggregate':
        return { setup, query: `SELECT category, count() as count, avg(value) as avg_value FROM benchmark_items GROUP BY category` }
    }
  }

  // MongoDB (document queries)
  if (database === 'mongo') {
    // MongoDB uses its own query format - we'll use the container adapter's methods
    return {
      setup: [],
      query: operation, // Pass operation name, handler will use appropriate method
    }
  }

  throw new Error(`Unknown database: ${database}`)
}

/**
 * Create container database adapter
 */
function createContainerAdapter(
  env: Env,
  database: DatabaseType,
  size: ContainerSize
): ContainerDatabase {
  const sessionId = crypto.randomUUID()

  switch (database) {
    case 'postgres':
      return createPostgresContainer(env.POSTGRES_CONTAINER, { sessionId, size })
    case 'clickhouse':
      return createClickHouseContainer(env.CLICKHOUSE_CONTAINER, { sessionId, size })
    case 'mongo':
      return createMongoContainer(env.MONGO_CONTAINER, { sessionId, size })
    case 'duckdb':
      return createDuckDBContainer(env.DUCKDB_CONTAINER, { sessionId, size })
    case 'sqlite':
      return createSQLiteContainer(env.SQLITE_CONTAINER, { sessionId, size })
    default:
      throw new Error(`Unknown database: ${database}`)
  }
}

/**
 * Benchmark container database
 */
async function benchmarkContainer(
  env: Env,
  database: DatabaseType,
  size: ContainerSize,
  operation: BenchmarkOperation,
  iterations: number
): Promise<OperationResult> {
  const warmQueryMs: number[] = []

  // Measure cold start (container startup + connection)
  const coldStartTime = performance.now()
  const adapter = createContainerAdapter(env, database, size)
  await adapter.connect()
  const coldStartMs = performance.now() - coldStartTime

  // Setup schema/data
  const { setup, query, params } = getOperationQueries(database, operation)

  if (database !== 'mongo') {
    for (const sql of setup) {
      await adapter.execute(sql)
    }
  } else {
    // MongoDB setup
    const mongoAdapter = adapter as import('../containers/mongo.js').MongoContainer
    // Seed some test data
    await mongoAdapter.insertMany('benchmark_items', [
      { _id: 'item-001', name: 'Item 1', value: 100, category: 'A' },
      { _id: 'item-002', name: 'Item 2', value: 200, category: 'B' },
      { _id: 'item-003', name: 'Item 3', value: 300, category: 'A' },
      { _id: 'item-004', name: 'Item 4', value: 400, category: 'B' },
      { _id: 'item-005', name: 'Item 5', value: 500, category: 'A' },
    ]).catch(() => {}) // Ignore duplicate key errors
  }

  // Run warm queries
  for (let i = 0; i < iterations; i++) {
    const start = performance.now()

    if (database === 'mongo') {
      const mongoAdapter = adapter as import('../containers/mongo.js').MongoContainer
      switch (operation) {
        case 'point_lookup':
          await mongoAdapter.findOne('benchmark_items', { _id: 'item-001' })
          break
        case 'range_scan':
          await mongoAdapter.find('benchmark_items', { category: 'A' }, { sort: { value: 1 } })
          break
        case 'insert':
          await mongoAdapter.insertOne('benchmark_items', {
            _id: `item-${Date.now()}-${i}`,
            name: 'Benchmark Item',
            value: Math.floor(Math.random() * 1000),
            category: 'TEST',
          })
          break
        case 'aggregate':
          await mongoAdapter.aggregate('benchmark_items', [
            { $group: { _id: '$category', count: { $sum: 1 }, avg_value: { $avg: '$value' } } },
          ])
          break
      }
    } else {
      await adapter.query(query, params)
    }

    warmQueryMs.push(performance.now() - start)
  }

  await adapter.close()

  return {
    operation,
    database,
    runtime: 'container',
    containerSize: size,
    iterations,
    coldStartMs,
    warmQueryMs,
    stats: calculateStats(warmQueryMs),
  }
}

/**
 * Create WASM database store
 */
async function createWasmStore(database: DatabaseType): Promise<{
  store: PostgresStore | SQLiteStore | DuckDBStore | null
  type: 'sql' | 'mongo'
}> {
  switch (database) {
    case 'postgres': {
      const { createPostgresStore } = await import('../databases/postgres.js')
      return { store: await createPostgresStore(), type: 'sql' }
    }
    case 'sqlite': {
      const { createSQLiteStore } = await import('../databases/sqlite.js')
      return { store: await createSQLiteStore(), type: 'sql' }
    }
    case 'duckdb': {
      const { createDuckDBStore } = await import('../databases/duckdb.js')
      return { store: await createDuckDBStore(), type: 'sql' }
    }
    case 'clickhouse':
    case 'mongo':
      // These don't have WASM equivalents, return null
      return { store: null, type: database === 'mongo' ? 'mongo' : 'sql' }
    default:
      throw new Error(`Unknown database: ${database}`)
  }
}

/**
 * Benchmark WASM database
 */
async function benchmarkWasm(
  database: DatabaseType,
  operation: BenchmarkOperation,
  iterations: number
): Promise<OperationResult | null> {
  // Check if WASM version exists for this database
  if (database === 'clickhouse' || database === 'mongo') {
    // No WASM version available
    return null
  }

  const warmQueryMs: number[] = []

  // Measure cold start (WASM instantiation)
  const coldStartTime = performance.now()
  const { store } = await createWasmStore(database)
  if (!store) {
    return null
  }
  const coldStartMs = performance.now() - coldStartTime

  // Setup schema/data
  const { setup, query, params } = getOperationQueries(database, operation)

  for (const sql of setup) {
    await store.query(sql)
  }

  // Run warm queries
  for (let i = 0; i < iterations; i++) {
    const start = performance.now()

    // For insert operations, generate unique IDs per iteration
    let queryParams = params
    if (operation === 'insert' && params) {
      queryParams = [`item-${Date.now()}-${i}`, 'Benchmark Item', Math.floor(Math.random() * 1000), 'TEST']
    }

    await store.query(query, queryParams)
    warmQueryMs.push(performance.now() - start)
  }

  await store.close()

  return {
    operation,
    database,
    runtime: 'wasm',
    iterations,
    coldStartMs,
    warmQueryMs,
    stats: calculateStats(warmQueryMs),
  }
}

/**
 * Convert results to JSONL format
 */
function toJSONL(results: OperationResult[]): string {
  const lines: JSONLLine[] = results.map((r) => ({
    timestamp: new Date().toISOString(),
    database: r.database,
    runtime: r.runtime,
    containerSize: r.containerSize,
    operation: r.operation,
    coldStartMs: r.coldStartMs,
    warmAvgMs: r.stats.avg,
    p50Ms: r.stats.p50,
    p99Ms: r.stats.p99,
    iterations: r.iterations,
  }))

  return lines.map((line) => JSON.stringify(line)).join('\n')
}

// Health check endpoint
app.get('/health', (c) => {
  return c.json({ status: 'ok', service: 'bench-container-latency' })
})

// Main benchmark endpoint
app.post('/benchmark/container-latency', async (c) => {
  const database = (c.req.query('database') || 'postgres') as DatabaseType
  const size = (c.req.query('size') || 'standard-1') as ContainerSize
  const iterations = parseInt(c.req.query('iterations') || '100', 10)
  const operations: BenchmarkOperation[] = ['point_lookup', 'range_scan', 'insert', 'aggregate']

  // Validate inputs
  const validDatabases: DatabaseType[] = ['postgres', 'sqlite', 'duckdb', 'clickhouse', 'mongo']
  const validSizes: ContainerSize[] = ['lite', 'basic', 'standard-1', 'standard-2', 'standard-4', 'performance-8', 'performance-16']

  if (!validDatabases.includes(database)) {
    return c.json({ error: `Invalid database. Valid options: ${validDatabases.join(', ')}` }, 400)
  }

  if (!validSizes.includes(size)) {
    return c.json({ error: `Invalid size. Valid options: ${validSizes.join(', ')}` }, 400)
  }

  if (iterations < 1 || iterations > 1000) {
    return c.json({ error: 'Iterations must be between 1 and 1000' }, 400)
  }

  const results: OperationResult[] = []
  const errors: string[] = []

  // Run benchmarks for each operation
  for (const operation of operations) {
    try {
      // Benchmark container
      const containerResult = await benchmarkContainer(c.env, database, size, operation, iterations)
      results.push(containerResult)

      // Benchmark WASM (if available)
      const wasmResult = await benchmarkWasm(database, operation, iterations)
      if (wasmResult) {
        results.push(wasmResult)
      }
    } catch (error) {
      errors.push(`${operation}: ${error instanceof Error ? error.message : String(error)}`)
    }
  }

  // Generate JSONL output
  const jsonl = toJSONL(results)

  // Store results in R2 (if bucket is bound)
  if (c.env.BENCHMARK_RESULTS) {
    const key = `container-latency/${database}/${size}/${new Date().toISOString()}.jsonl`
    await c.env.BENCHMARK_RESULTS.put(key, jsonl, {
      customMetadata: {
        database,
        containerSize: size,
        iterations: String(iterations),
        timestamp: new Date().toISOString(),
      },
    })
  }

  // Return JSONL response
  return c.text(jsonl, 200, {
    'Content-Type': 'application/x-ndjson',
    'X-Benchmark-Database': database,
    'X-Benchmark-Size': size,
    'X-Benchmark-Iterations': String(iterations),
    'X-Benchmark-Errors': errors.length > 0 ? errors.join('; ') : 'none',
  })
})

// Batch benchmark endpoint - runs all combinations
app.post('/benchmark/container-latency/batch', async (c) => {
  const databases: DatabaseType[] = ['postgres', 'sqlite', 'duckdb', 'clickhouse', 'mongo']
  const sizes: ContainerSize[] = ['lite', 'basic', 'standard-1', 'standard-2']
  const iterations = parseInt(c.req.query('iterations') || '50', 10)
  const operations: BenchmarkOperation[] = ['point_lookup', 'range_scan', 'insert', 'aggregate']

  const allResults: OperationResult[] = []
  const errors: string[] = []

  for (const database of databases) {
    for (const size of sizes) {
      for (const operation of operations) {
        try {
          // Container benchmark
          const containerResult = await benchmarkContainer(c.env, database, size, operation, iterations)
          allResults.push(containerResult)

          // WASM benchmark (only for first size since WASM doesn't use container sizes)
          if (size === 'lite') {
            const wasmResult = await benchmarkWasm(database, operation, iterations)
            if (wasmResult) {
              allResults.push(wasmResult)
            }
          }
        } catch (error) {
          errors.push(`${database}/${size}/${operation}: ${error instanceof Error ? error.message : String(error)}`)
        }
      }
    }
  }

  const jsonl = toJSONL(allResults)

  // Store results
  if (c.env.BENCHMARK_RESULTS) {
    const key = `container-latency/batch/${new Date().toISOString()}.jsonl`
    await c.env.BENCHMARK_RESULTS.put(key, jsonl, {
      customMetadata: {
        type: 'batch',
        databases: databases.join(','),
        sizes: sizes.join(','),
        iterations: String(iterations),
        timestamp: new Date().toISOString(),
      },
    })
  }

  return c.text(jsonl, 200, {
    'Content-Type': 'application/x-ndjson',
    'X-Benchmark-Type': 'batch',
    'X-Benchmark-Total-Results': String(allResults.length),
    'X-Benchmark-Errors': errors.length > 0 ? errors.join('; ') : 'none',
  })
})

// Get stored results
app.get('/benchmark/container-latency/results', async (c) => {
  if (!c.env.BENCHMARK_RESULTS) {
    return c.json({ error: 'Results bucket not configured' }, 500)
  }

  const prefix = c.req.query('prefix') || 'container-latency/'
  const limit = parseInt(c.req.query('limit') || '100', 10)

  const list = await c.env.BENCHMARK_RESULTS.list({
    prefix,
    limit,
  })

  const results = list.objects.map((obj) => ({
    key: obj.key,
    size: obj.size,
    uploaded: obj.uploaded,
    customMetadata: obj.customMetadata,
  }))

  return c.json({ results, truncated: list.truncated })
})

// Get specific result file
app.get('/benchmark/container-latency/results/:key{.+}', async (c) => {
  if (!c.env.BENCHMARK_RESULTS) {
    return c.json({ error: 'Results bucket not configured' }, 500)
  }

  const key = c.req.param('key')
  const object = await c.env.BENCHMARK_RESULTS.get(key)

  if (!object) {
    return c.json({ error: 'Result not found' }, 404)
  }

  const body = await object.text()
  return c.text(body, 200, {
    'Content-Type': 'application/x-ndjson',
  })
})

// Summary statistics endpoint
app.get('/benchmark/container-latency/summary', async (c) => {
  if (!c.env.BENCHMARK_RESULTS) {
    return c.json({ error: 'Results bucket not configured' }, 500)
  }

  const database = c.req.query('database') as DatabaseType | undefined
  const prefix = database
    ? `container-latency/${database}/`
    : 'container-latency/'

  const list = await c.env.BENCHMARK_RESULTS.list({ prefix, limit: 1000 })

  // Aggregate statistics from all results
  const summary: Record<string, {
    database: DatabaseType
    runtime: 'wasm' | 'container'
    containerSize?: ContainerSize
    operation: BenchmarkOperation
    sampleCount: number
    avgColdStartMs: number
    avgWarmMs: number
    avgP50Ms: number
    avgP99Ms: number
  }> = {}

  for (const obj of list.objects) {
    const object = await c.env.BENCHMARK_RESULTS.get(obj.key)
    if (!object) continue

    const text = await object.text()
    const lines = text.trim().split('\n')

    for (const line of lines) {
      const data = JSON.parse(line) as JSONLLine
      const key = `${data.database}-${data.runtime}-${data.containerSize || 'n/a'}-${data.operation}`

      if (!summary[key]) {
        summary[key] = {
          database: data.database,
          runtime: data.runtime,
          containerSize: data.containerSize,
          operation: data.operation,
          sampleCount: 0,
          avgColdStartMs: 0,
          avgWarmMs: 0,
          avgP50Ms: 0,
          avgP99Ms: 0,
        }
      }

      const s = summary[key]
      const n = s.sampleCount
      s.avgColdStartMs = (s.avgColdStartMs * n + data.coldStartMs) / (n + 1)
      s.avgWarmMs = (s.avgWarmMs * n + data.warmAvgMs) / (n + 1)
      s.avgP50Ms = (s.avgP50Ms * n + data.p50Ms) / (n + 1)
      s.avgP99Ms = (s.avgP99Ms * n + data.p99Ms) / (n + 1)
      s.sampleCount++
    }
  }

  return c.json({
    summary: Object.values(summary),
    totalFiles: list.objects.length,
    truncated: list.truncated,
  })
})

export default app
