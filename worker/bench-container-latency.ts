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
import { getContainer, type Container } from '@cloudflare/containers'

// Export Container DO classes for wrangler to bind
export {
  PostgresBenchContainer,
  ClickHouseBenchContainer,
  MongoBenchContainer,
  DuckDBBenchContainer,
  SQLiteBenchContainer,
} from './adapters/container-do.js'

// Container adapters
import {
  type ContainerSize,
} from '../containers/index.js'

// Import MongoContainer for MongoDB-specific operations
import type { MongoContainer } from '../containers/mongo.js'

// WASM database types
import type { PostgresStore } from '../databases/postgres.js'
import type { SQLiteStore } from '../databases/sqlite.js'
import type { DuckDBStore } from '../databases/duckdb.js'

// Environment bindings
interface Env {
  // Container Durable Object namespaces for container management
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  POSTGRES_DO: DurableObjectNamespace<any>
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  CLICKHOUSE_DO: DurableObjectNamespace<any>
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  MONGO_DO: DurableObjectNamespace<any>
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  DUCKDB_DO: DurableObjectNamespace<any>
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  SQLITE_DO: DurableObjectNamespace<any>

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

// Container ports for each database type
const CONTAINER_PORTS = {
  postgres: 8080,    // HTTP bridge
  clickhouse: 8123,  // Native HTTP
  mongo: 8080,       // HTTP bridge
  duckdb: 9999,      // HTTP bridge
  sqlite: 8080,      // HTTP bridge
} as const

// Container stub type from getContainer
type ContainerStub = ReturnType<typeof getContainer>

// Container adapter interface for benchmark operations
interface ContainerAdapter {
  sessionId: string
  database: DatabaseType
  container: ContainerStub
  port: number
}

/**
 * Create container adapter using getContainer
 */
function createContainerAdapter(
  env: Env,
  database: DatabaseType,
  _size: ContainerSize
): ContainerAdapter {
  const sessionId = `bench-${crypto.randomUUID()}`

  // Get container instance using getContainer based on database type
  let container: ContainerStub

  // Use type assertion for getContainer - the runtime types work correctly
  // but the TypeScript types from @cloudflare/workers-types and @cloudflare/containers
  // don't align perfectly. getContainer expects DurableObjectNamespace<Container>.
  switch (database) {
    case 'postgres':
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      container = getContainer(env.POSTGRES_DO as any, sessionId)
      break
    case 'clickhouse':
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      container = getContainer(env.CLICKHOUSE_DO as any, sessionId)
      break
    case 'mongo':
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      container = getContainer(env.MONGO_DO as any, sessionId)
      break
    case 'duckdb':
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      container = getContainer(env.DUCKDB_DO as any, sessionId)
      break
    case 'sqlite':
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      container = getContainer(env.SQLITE_DO as any, sessionId)
      break
    default:
      throw new Error(`Unknown database: ${database}`)
  }

  return {
    sessionId,
    database,
    container,
    port: CONTAINER_PORTS[database],
  }
}

/**
 * Execute SQL query on container via HTTP
 */
async function containerQuery<T = unknown>(
  adapter: ContainerAdapter,
  sql: string,
  params?: unknown[]
): Promise<T[]> {
  const { container, database, port } = adapter

  let request: Request

  switch (database) {
    case 'postgres':
    case 'sqlite':
    case 'duckdb':
      // HTTP bridge with POST /query
      request = new Request(`http://container:${port}/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql, params: params ?? [] }),
      })
      break

    case 'clickhouse':
      // Native HTTP interface - POST with SQL body
      request = new Request(`http://container:${port}/?default_format=JSONEachRow`, {
        method: 'POST',
        headers: { 'Content-Type': 'text/plain' },
        body: processClickHouseParams(sql, params),
      })
      break

    case 'mongo':
      // For MongoDB, use the generic query endpoint
      request = new Request(`http://container:${port}/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql, params: params ?? [] }),
      })
      break

    default:
      throw new Error(`Unknown database: ${database}`)
  }

  const response = await container.fetch(request)

  if (!response.ok) {
    const error = await response.text()
    throw new Error(`${database} query failed: ${error}`)
  }

  // Parse response based on database type
  if (database === 'clickhouse') {
    const text = await response.text()
    if (!text.trim()) return []
    return text.trim().split('\n').map(line => JSON.parse(line) as T)
  }

  const result = await response.json() as { rows?: T[], documents?: T[] }
  return result.rows ?? result.documents ?? []
}

/**
 * Execute SQL statement on container via HTTP
 */
async function containerExecute(
  adapter: ContainerAdapter,
  sql: string,
  params?: unknown[]
): Promise<{ rowsAffected: number }> {
  const { container, database, port } = adapter

  let request: Request

  switch (database) {
    case 'postgres':
    case 'sqlite':
    case 'duckdb':
      // HTTP bridge with POST /execute
      request = new Request(`http://container:${port}/execute`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql, params: params ?? [] }),
      })
      break

    case 'clickhouse':
      // Native HTTP interface
      request = new Request(`http://container:${port}/`, {
        method: 'POST',
        headers: { 'Content-Type': 'text/plain' },
        body: processClickHouseParams(sql, params),
      })
      break

    case 'mongo':
      request = new Request(`http://container:${port}/execute`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql, params: params ?? [] }),
      })
      break

    default:
      throw new Error(`Unknown database: ${database}`)
  }

  const response = await container.fetch(request)

  if (!response.ok) {
    const error = await response.text()
    throw new Error(`${database} execute failed: ${error}`)
  }

  if (database === 'clickhouse') {
    const summary = response.headers.get('X-ClickHouse-Summary')
    if (summary) {
      const parsed = JSON.parse(summary) as { written_rows?: string }
      return { rowsAffected: parseInt(parsed.written_rows ?? '0', 10) }
    }
    return { rowsAffected: 0 }
  }

  const result = await response.json() as { rowsAffected?: number }
  return { rowsAffected: result.rowsAffected ?? 0 }
}

/**
 * Check if container is ready
 */
async function containerReady(adapter: ContainerAdapter): Promise<boolean> {
  const { container, database, port } = adapter

  try {
    const healthPath = database === 'clickhouse' ? '/ping' : '/ready'
    const request = new Request(`http://container:${port}${healthPath}`, {
      method: 'GET',
    })

    const response = await container.fetch(request)
    return response.ok
  } catch {
    return false
  }
}

/**
 * Wait for container to be ready
 */
async function waitForContainerReady(
  adapter: ContainerAdapter,
  maxAttempts = 30,
  delayMs = 1000
): Promise<void> {
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    if (await containerReady(adapter)) {
      return
    }
    await sleep(delayMs)
  }
  throw new Error(`${adapter.database} container failed to become ready`)
}

/**
 * Process ClickHouse parameters (replace $1, $2, etc.)
 */
function processClickHouseParams(sql: string, params?: unknown[]): string {
  if (!params || params.length === 0) return sql

  let processed = sql
  params.forEach((param, index) => {
    const placeholder = `$${index + 1}`
    const value = formatClickHouseValue(param)
    processed = processed.replace(placeholder, value)
  })

  return processed
}

/**
 * Format value for ClickHouse
 */
function formatClickHouseValue(value: unknown): string {
  if (value === null || value === undefined) return 'NULL'
  if (typeof value === 'string') return `'${value.replace(/'/g, "''")}'`
  if (typeof value === 'number') return String(value)
  if (typeof value === 'boolean') return value ? '1' : '0'
  if (value instanceof Date) return `'${value.toISOString()}'`
  return `'${JSON.stringify(value).replace(/'/g, "''")}'`
}

/**
 * MongoDB-specific operations via HTTP bridge
 */
async function mongoFind<T = unknown>(
  adapter: ContainerAdapter,
  collection: string,
  filter: Record<string, unknown>,
  options?: { sort?: Record<string, number>; limit?: number }
): Promise<T[]> {
  const request = new Request(`http://container:${adapter.port}/find`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      database: 'benchmark',
      collection,
      filter,
      ...options,
    }),
  })

  const response = await adapter.container.fetch(request)

  if (!response.ok) {
    const error = await response.text()
    throw new Error(`MongoDB find failed: ${error}`)
  }

  const result = await response.json() as { documents: T[] }
  return result.documents
}

async function mongoFindOne<T = unknown>(
  adapter: ContainerAdapter,
  collection: string,
  filter: Record<string, unknown>
): Promise<T | null> {
  const request = new Request(`http://container:${adapter.port}/findOne`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      database: 'benchmark',
      collection,
      filter,
    }),
  })

  const response = await adapter.container.fetch(request)

  if (!response.ok) {
    const error = await response.text()
    throw new Error(`MongoDB findOne failed: ${error}`)
  }

  const result = await response.json() as { document: T | null }
  return result.document
}

async function mongoInsertOne(
  adapter: ContainerAdapter,
  collection: string,
  document: Record<string, unknown>
): Promise<{ insertedId?: string; insertedCount?: number }> {
  const request = new Request(`http://container:${adapter.port}/insertOne`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      database: 'benchmark',
      collection,
      document,
    }),
  })

  const response = await adapter.container.fetch(request)

  if (!response.ok) {
    const error = await response.text()
    throw new Error(`MongoDB insertOne failed: ${error}`)
  }

  return response.json() as Promise<{ insertedId?: string; insertedCount?: number }>
}

async function mongoInsertMany(
  adapter: ContainerAdapter,
  collection: string,
  documents: Record<string, unknown>[]
): Promise<{ insertedCount?: number }> {
  const request = new Request(`http://container:${adapter.port}/insertMany`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      database: 'benchmark',
      collection,
      documents,
    }),
  })

  const response = await adapter.container.fetch(request)

  if (!response.ok) {
    const error = await response.text()
    throw new Error(`MongoDB insertMany failed: ${error}`)
  }

  return response.json() as Promise<{ insertedCount?: number }>
}

async function mongoAggregate<T = unknown>(
  adapter: ContainerAdapter,
  collection: string,
  pipeline: Record<string, unknown>[]
): Promise<T[]> {
  const request = new Request(`http://container:${adapter.port}/aggregate`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      database: 'benchmark',
      collection,
      pipeline,
    }),
  })

  const response = await adapter.container.fetch(request)

  if (!response.ok) {
    const error = await response.text()
    throw new Error(`MongoDB aggregate failed: ${error}`)
  }

  const result = await response.json() as { documents: T[] }
  return result.documents
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
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
  await waitForContainerReady(adapter)
  const coldStartMs = performance.now() - coldStartTime

  // Setup schema/data
  const { setup, query, params } = getOperationQueries(database, operation)

  if (database !== 'mongo') {
    for (const sql of setup) {
      await containerExecute(adapter, sql)
    }
  } else {
    // MongoDB setup - seed test data
    await mongoInsertMany(adapter, 'benchmark_items', [
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
      switch (operation) {
        case 'point_lookup':
          await mongoFindOne(adapter, 'benchmark_items', { _id: 'item-001' })
          break
        case 'range_scan':
          await mongoFind(adapter, 'benchmark_items', { category: 'A' }, { sort: { value: 1 } })
          break
        case 'insert':
          await mongoInsertOne(adapter, 'benchmark_items', {
            _id: `item-${Date.now()}-${i}`,
            name: 'Benchmark Item',
            value: Math.floor(Math.random() * 1000),
            category: 'TEST',
          })
          break
        case 'aggregate':
          await mongoAggregate(adapter, 'benchmark_items', [
            { $group: { _id: '$category', count: { $sum: 1 }, avg_value: { $avg: '$value' } } },
          ])
          break
      }
    } else {
      await containerQuery(adapter, query, params)
    }

    warmQueryMs.push(performance.now() - start)
  }

  // Container connections are stateless via HTTP, no close needed

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

// Warmup endpoint - starts container and waits for ready without running full benchmark
app.get('/warmup/:database', async (c) => {
  const database = c.req.param('database') as DatabaseType
  const size = (c.req.query('size') || 'standard-1') as ContainerSize
  const maxAttempts = parseInt(c.req.query('maxAttempts') || '60', 10)
  const delayMs = parseInt(c.req.query('delayMs') || '1000', 10)

  // Validate database
  const validDatabases: DatabaseType[] = ['postgres', 'sqlite', 'duckdb', 'clickhouse', 'mongo']
  if (!validDatabases.includes(database)) {
    return c.json({ error: `Invalid database. Valid options: ${validDatabases.join(', ')}` }, 400)
  }

  const startTime = performance.now()

  try {
    const adapter = createContainerAdapter(c.env, database, size)

    // Wait for container to be ready with configurable timeout
    await waitForContainerReady(adapter, maxAttempts, delayMs)

    const readyTime = performance.now() - startTime

    // Run a simple query to verify the container is fully functional
    let verifyTime = 0
    if (database !== 'mongo') {
      const verifyStart = performance.now()
      await containerQuery(adapter, 'SELECT 1')
      verifyTime = performance.now() - verifyStart
    } else {
      const verifyStart = performance.now()
      await mongoFind(adapter, 'benchmark_items', {}, { limit: 1 })
      verifyTime = performance.now() - verifyStart
    }

    return c.json({
      status: 'ready',
      database,
      containerSize: size,
      sessionId: adapter.sessionId,
      timings: {
        readyMs: readyTime,
        verifyMs: verifyTime,
        totalMs: performance.now() - startTime,
      },
      message: `${database} container is warm and ready for benchmarks`,
    })
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error)
    return c.json({
      status: 'error',
      database,
      containerSize: size,
      error: errorMessage,
      timings: {
        totalMs: performance.now() - startTime,
      },
      message: `Failed to warm up ${database} container`,
    }, 500)
  }
})

// Browser-accessible single database benchmark endpoint (GET)
app.get('/benchmark/container/:database', async (c) => {
  const database = c.req.param('database') as DatabaseType
  const size = (c.req.query('size') || 'standard-1') as ContainerSize
  const iterations = parseInt(c.req.query('iterations') || '10', 10)
  const operation = (c.req.query('operation') || 'point_lookup') as BenchmarkOperation

  // Validate inputs
  const validDatabases: DatabaseType[] = ['postgres', 'sqlite', 'duckdb', 'clickhouse', 'mongo']
  const validSizes: ContainerSize[] = ['lite', 'basic', 'standard-1', 'standard-2', 'standard-4', 'performance-8', 'performance-16']
  const validOperations: BenchmarkOperation[] = ['point_lookup', 'range_scan', 'insert', 'aggregate']

  if (!validDatabases.includes(database)) {
    return c.json({ error: `Invalid database. Valid options: ${validDatabases.join(', ')}` }, 400)
  }

  if (!validSizes.includes(size)) {
    return c.json({ error: `Invalid size. Valid options: ${validSizes.join(', ')}` }, 400)
  }

  if (!validOperations.includes(operation)) {
    return c.json({ error: `Invalid operation. Valid options: ${validOperations.join(', ')}` }, 400)
  }

  if (iterations < 1 || iterations > 100) {
    return c.json({ error: 'Iterations must be between 1 and 100 for browser access' }, 400)
  }

  try {
    const result = await benchmarkContainer(c.env, database, size, operation, iterations)

    // Also try WASM benchmark for comparison (if available) - catch errors separately
    let wasmResult = null
    let wasmError = null
    try {
      wasmResult = await benchmarkWasm(database, operation, iterations)
    } catch (err) {
      wasmError = err instanceof Error ? err.message : String(err)
    }

    return c.json({
      timestamp: new Date().toISOString(),
      database,
      containerSize: size,
      operation,
      iterations,
      container: {
        coldStartMs: result.coldStartMs,
        avgMs: result.stats.avg,
        p50Ms: result.stats.p50,
        p99Ms: result.stats.p99,
        minMs: result.stats.min,
        maxMs: result.stats.max,
      },
      wasm: wasmResult ? {
        coldStartMs: wasmResult.coldStartMs,
        avgMs: wasmResult.stats.avg,
        p50Ms: wasmResult.stats.p50,
        p99Ms: wasmResult.stats.p99,
        minMs: wasmResult.stats.min,
        maxMs: wasmResult.stats.max,
      } : null,
      wasmError: wasmError,
      comparison: wasmResult ? {
        coldStartRatio: result.coldStartMs / wasmResult.coldStartMs,
        avgLatencyRatio: result.stats.avg / wasmResult.stats.avg,
        message: result.stats.avg < wasmResult.stats.avg
          ? `Container is ${(wasmResult.stats.avg / result.stats.avg).toFixed(1)}x faster on warm queries`
          : `WASM is ${(result.stats.avg / wasmResult.stats.avg).toFixed(1)}x faster on warm queries`,
      } : null,
    })
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error)
    return c.json({
      error: errorMessage,
      database,
      containerSize: size,
      operation,
      iterations,
      hint: 'Try warming up the container first with GET /warmup/:database',
    }, 500)
  }
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
