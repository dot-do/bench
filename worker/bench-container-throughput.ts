/**
 * Container Throughput Benchmark Worker
 *
 * Benchmarks container database throughput and calculates costs.
 * Measures operations per second across container size tiers and
 * compares with WASM + DO storage costs.
 *
 * Endpoint: POST /benchmark/container-throughput
 * Query params:
 *   - database: postgres|clickhouse|mongo|duckdb|sqlite
 *   - concurrency: 1|10|50|100 (parallel requests)
 *   - duration: seconds to run sustained load (default: 60)
 *
 * @see https://developers.cloudflare.com/containers/
 */

import { getContainer, type Container } from '@cloudflare/containers'
import { type ContainerSize } from '../containers/index.js'

// Export Container DO classes for wrangler to bind
export {
  PostgresBenchContainer,
  ClickHouseBenchContainer,
  MongoBenchContainer,
  DuckDBBenchContainer,
  SQLiteBenchContainer,
} from './adapters/container-do.js'

// =============================================================================
// Types
// =============================================================================

export interface Env {
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

  // R2 bucket for results
  RESULTS_BUCKET: R2Bucket
}

type DatabaseType = 'postgres' | 'clickhouse' | 'mongo' | 'duckdb' | 'sqlite'

interface BenchmarkParams {
  database: DatabaseType
  concurrency: number
  duration: number
  operations?: ('read' | 'write' | 'mixed')[]
  containerSize?: ContainerSize
}

interface OperationResult {
  success: boolean
  latencyMs: number
  error?: string
}

interface ThroughputMetrics {
  totalOperations: number
  successfulOperations: number
  failedOperations: number
  operationsPerSecond: number
  avgLatencyMs: number
  p50LatencyMs: number
  p95LatencyMs: number
  p99LatencyMs: number
  minLatencyMs: number
  maxLatencyMs: number
}

interface CostMetrics {
  containerRuntimeCostPerOp: number  // $ per operation
  containerCostPer1M: number         // $ per 1M operations
  wasmDoCostPer1M: number            // $ per 1M operations (for comparison)
  breakEvenOpsPerHour: number        // ops/hour where container becomes cheaper
  monthlyProjections: MonthlyProjection[]
}

interface MonthlyProjection {
  opsPerMonth: number
  containerCost: number
  wasmDoCost: number
  winner: 'container' | 'wasm-do'
  savings: number
  savingsPercent: number
}

interface BenchmarkResult {
  timestamp: string
  database: DatabaseType
  containerSize: ContainerSize
  concurrency: number
  durationSeconds: number
  scenarios: {
    singleWorkerSingleContainer: ThroughputMetrics
    concurrentRequests: ThroughputMetrics
    sustainedLoad: ThroughputMetrics
  }
  costAnalysis: CostMetrics
  containerMetrics: {
    coldStartMs: number
    warmStartMs: number
    memoryUsageMB?: number
  }
}

// =============================================================================
// Pricing Constants (2026)
// =============================================================================

const PRICING = {
  // Cloudflare Container Pricing (per size tier, hourly)
  containerHourly: {
    'lite': 0.0025,           // 256MB RAM, 0.25 vCPU - $0.0025/hr
    'basic': 0.005,           // 512MB RAM, 0.5 vCPU - $0.005/hr
    'standard-1': 0.01,       // 1GB RAM, 1 vCPU - $0.01/hr
    'standard-2': 0.02,       // 2GB RAM, 2 vCPU - $0.02/hr
    'standard-4': 0.04,       // 4GB RAM, 4 vCPU - $0.04/hr
    'performance-8': 0.08,    // 8GB RAM, 8 vCPU - $0.08/hr
    'performance-16': 0.16,   // 16GB RAM, 16 vCPU - $0.16/hr
  } as Record<ContainerSize, number>,

  // DO SQLite pricing (for WASM comparison)
  doRowsRead: 0.001,      // $ per 1M rows read
  doRowsWritten: 1.00,    // $ per 1M rows written
  doDuration: 0.001,      // $ per 1M GB-seconds

  // Workers pricing
  workersRequests: 0.30,  // $ per 1M requests

  // R2 pricing
  r2ClassA: 4.50,         // $ per 1M writes
  r2ClassB: 0.36,         // $ per 1M reads
  r2StorageGB: 0.015,     // $ per GB/month
}

// Average rows touched per operation (for WASM + DO cost calculation)
const AVG_ROWS_PER_OP = {
  read: 1,      // Point lookup: 1 row
  write: 1,     // Single insert: 1 row
  mixed: 1.5,   // Mix of reads and writes
}

// =============================================================================
// Worker Entry Point
// =============================================================================

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url)
    const path = url.pathname

    // CORS headers
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    }

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders })
    }

    try {
      if (path === '/benchmark/container-throughput' && request.method === 'POST') {
        const params = parseBenchmarkParams(url)
        const result = await runThroughputBenchmark(params, env, ctx)

        // Store result in R2
        const resultKey = `throughput/${params.database}/${new Date().toISOString()}.json`
        await env.RESULTS_BUCKET.put(resultKey, JSON.stringify(result, null, 2))

        return new Response(JSON.stringify(result, null, 2), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        })
      }

      if (path === '/benchmark/container-throughput/quick' && request.method === 'GET') {
        // Quick benchmark with reduced duration for testing
        const params = parseBenchmarkParams(url)
        params.duration = 5 // 5 seconds for quick test
        const result = await runThroughputBenchmark(params, env, ctx)

        return new Response(JSON.stringify(result, null, 2), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        })
      }

      if (path === '/health') {
        return new Response(JSON.stringify({ status: 'ok', timestamp: new Date().toISOString() }), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        })
      }

      // Warmup endpoint - starts container and waits for ready
      const warmupMatch = path.match(/^\/warmup\/(\w+)$/)
      if (warmupMatch && request.method === 'GET') {
        const database = warmupMatch[1] as DatabaseType
        const maxAttempts = parseInt(url.searchParams.get('maxAttempts') || '60', 10)
        const delayMs = parseInt(url.searchParams.get('delayMs') || '1000', 10)

        // Validate database
        const validDatabases: DatabaseType[] = ['postgres', 'clickhouse', 'mongo', 'duckdb', 'sqlite']
        if (!validDatabases.includes(database)) {
          return new Response(JSON.stringify({
            error: `Invalid database. Valid options: ${validDatabases.join(', ')}`,
          }), {
            status: 400,
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          })
        }

        const startTime = performance.now()

        try {
          const adapter = createDatabaseAdapter(database, env)

          // Wait for container to be ready with configurable timeout
          await containerConnectWithTimeout(adapter, maxAttempts, delayMs)

          const readyTime = performance.now() - startTime

          // Run a simple query to verify the container is fully functional
          const verifyStart = performance.now()
          await containerPing(adapter)
          const verifyTime = performance.now() - verifyStart

          return new Response(JSON.stringify({
            status: 'ready',
            database,
            sessionId: adapter.sessionId,
            timings: {
              readyMs: readyTime,
              verifyMs: verifyTime,
              totalMs: performance.now() - startTime,
            },
            message: `${database} container is warm and ready for benchmarks`,
          }), {
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          })
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : String(error)
          return new Response(JSON.stringify({
            status: 'error',
            database,
            error: errorMessage,
            timings: {
              totalMs: performance.now() - startTime,
            },
            message: `Failed to warm up ${database} container`,
          }), {
            status: 500,
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          })
        }
      }

      // Browser-accessible single database benchmark endpoint (GET)
      const benchmarkMatch = path.match(/^\/benchmark\/container\/(\w+)$/)
      if (benchmarkMatch && request.method === 'GET') {
        const database = benchmarkMatch[1] as DatabaseType
        const duration = parseInt(url.searchParams.get('duration') || '5', 10)
        const concurrency = parseInt(url.searchParams.get('concurrency') || '10', 10)
        const containerSize = (url.searchParams.get('size') || 'standard-1') as ContainerSize

        // Validate database
        const validDatabases: DatabaseType[] = ['postgres', 'clickhouse', 'mongo', 'duckdb', 'sqlite']
        if (!validDatabases.includes(database)) {
          return new Response(JSON.stringify({
            error: `Invalid database. Valid options: ${validDatabases.join(', ')}`,
          }), {
            status: 400,
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          })
        }

        // Limit duration for browser access
        const safeDuration = Math.min(duration, 30)
        const safeConcurrency = Math.min(concurrency, 50)

        try {
          const result = await runThroughputBenchmark({
            database,
            concurrency: safeConcurrency,
            duration: safeDuration,
            containerSize,
          }, env, ctx)

          return new Response(JSON.stringify(result, null, 2), {
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          })
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : String(error)
          return new Response(JSON.stringify({
            error: errorMessage,
            database,
            duration: safeDuration,
            concurrency: safeConcurrency,
            hint: 'Try warming up the container first with GET /warmup/:database',
          }), {
            status: 500,
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          })
        }
      }

      return new Response(JSON.stringify({
        error: 'Not found',
        availableEndpoints: [
          'POST /benchmark/container-throughput?database=postgres&concurrency=50&duration=60',
          'GET /benchmark/container-throughput/quick?database=postgres',
          'GET /warmup/:database - Warm up container (postgres, clickhouse, mongo, duckdb, sqlite)',
          'GET /benchmark/container/:database - Quick benchmark for browser',
          'GET /health',
        ],
      }), {
        status: 404,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      })
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error)
      return new Response(JSON.stringify({ error: errorMessage }), {
        status: 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      })
    }
  },
}

// =============================================================================
// Parameter Parsing
// =============================================================================

function parseBenchmarkParams(url: URL): BenchmarkParams {
  const database = (url.searchParams.get('database') || 'postgres') as DatabaseType
  const concurrency = parseInt(url.searchParams.get('concurrency') || '50', 10)
  const duration = parseInt(url.searchParams.get('duration') || '60', 10)
  const containerSize = (url.searchParams.get('size') || 'standard-1') as ContainerSize

  // Validate
  if (!['postgres', 'clickhouse', 'mongo', 'duckdb', 'sqlite'].includes(database)) {
    throw new Error(`Invalid database: ${database}. Must be one of: postgres, clickhouse, mongo, duckdb, sqlite`)
  }

  if (![1, 10, 50, 100, 200].includes(concurrency)) {
    throw new Error(`Invalid concurrency: ${concurrency}. Must be one of: 1, 10, 50, 100, 200`)
  }

  if (duration < 1 || duration > 300) {
    throw new Error(`Invalid duration: ${duration}. Must be between 1 and 300 seconds`)
  }

  return { database, concurrency, duration, containerSize }
}

// =============================================================================
// Container Configuration
// =============================================================================

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

// Container adapter interface
interface ContainerAdapter {
  sessionId: string
  database: DatabaseType
  container: ContainerStub
  port: number
}

// =============================================================================
// Database Adapter Factory
// =============================================================================

function createDatabaseAdapter(database: DatabaseType, env: Env): ContainerAdapter {
  const sessionId = `bench-throughput-${crypto.randomUUID()}`

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

// =============================================================================
// Container HTTP Operations
// =============================================================================

async function containerConnect(adapter: ContainerAdapter): Promise<void> {
  return containerConnectWithTimeout(adapter, 30, 1000)
}

async function containerConnectWithTimeout(
  adapter: ContainerAdapter,
  maxAttempts: number = 30,
  delayMs: number = 1000
): Promise<void> {
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      const healthPath = adapter.database === 'clickhouse' ? '/ping' : '/ready'
      const request = new Request(`http://container:${adapter.port}${healthPath}`, {
        method: 'GET',
      })

      const response = await adapter.container.fetch(request)
      if (response.ok) {
        return
      }
    } catch {
      // Container may not be ready yet
    }

    await sleep(delayMs)
  }

  throw new Error(`${adapter.database} container failed to become ready after ${maxAttempts} attempts`)
}

async function containerPing(adapter: ContainerAdapter): Promise<void> {
  const healthPath = adapter.database === 'clickhouse' ? '/ping' : '/health'
  const request = new Request(`http://container:${adapter.port}${healthPath}`, {
    method: 'GET',
  })

  const response = await adapter.container.fetch(request)
  if (!response.ok) {
    throw new Error(`${adapter.database} ping failed`)
  }
}

async function containerQuery<T = unknown>(
  adapter: ContainerAdapter,
  sql: string,
  _params?: unknown[]
): Promise<T[]> {
  const { container, database, port } = adapter

  let request: Request

  switch (database) {
    case 'postgres':
    case 'sqlite':
    case 'duckdb':
      request = new Request(`http://container:${port}/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql, params: _params ?? [] }),
      })
      break

    case 'clickhouse':
      request = new Request(`http://container:${port}/?default_format=JSONEachRow`, {
        method: 'POST',
        headers: { 'Content-Type': 'text/plain' },
        body: processClickHouseParams(sql, _params),
      })
      break

    case 'mongo':
      request = new Request(`http://container:${port}/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql, params: _params ?? [] }),
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

  if (database === 'clickhouse') {
    const text = await response.text()
    if (!text.trim()) return []
    return text.trim().split('\n').map(line => JSON.parse(line) as T)
  }

  const result = await response.json() as { rows?: T[], documents?: T[] }
  return result.rows ?? result.documents ?? []
}

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
      request = new Request(`http://container:${port}/execute`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql, params: params ?? [] }),
      })
      break

    case 'clickhouse':
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

function formatClickHouseValue(value: unknown): string {
  if (value === null || value === undefined) return 'NULL'
  if (typeof value === 'string') return `'${value.replace(/'/g, "''")}'`
  if (typeof value === 'number') return String(value)
  if (typeof value === 'boolean') return value ? '1' : '0'
  if (value instanceof Date) return `'${value.toISOString()}'`
  return `'${JSON.stringify(value).replace(/'/g, "''")}'`
}

// =============================================================================
// Benchmark Execution
// =============================================================================

async function runThroughputBenchmark(
  params: BenchmarkParams,
  env: Env,
  _ctx: ExecutionContext
): Promise<BenchmarkResult> {
  const { database, concurrency, duration, containerSize = 'standard-1' } = params

  // Create database adapter using getContainer
  const adapter = createDatabaseAdapter(database, env)

  // Measure cold start
  const coldStartTime = performance.now()
  await containerConnect(adapter)
  const coldStartMs = performance.now() - coldStartTime

  // Warm the connection
  await containerPing(adapter)
  const warmStartTime = performance.now()
  await containerPing(adapter)
  const warmStartMs = performance.now() - warmStartTime

  // Setup test table
  await setupTestTable(adapter, database)

  // Run benchmark scenarios
  const singleWorkerSingleContainer = await runSingleWorkerBenchmark(adapter, database, duration)
  const concurrentRequests = await runConcurrentBenchmark(adapter, database, concurrency, duration)
  const sustainedLoad = await runSustainedLoadBenchmark(adapter, database, concurrency, duration)

  // Calculate costs
  const costAnalysis = calculateCosts(sustainedLoad, containerSize)

  // Container connections are stateless via HTTP, no close needed

  return {
    timestamp: new Date().toISOString(),
    database,
    containerSize,
    concurrency,
    durationSeconds: duration,
    scenarios: {
      singleWorkerSingleContainer,
      concurrentRequests,
      sustainedLoad,
    },
    costAnalysis,
    containerMetrics: {
      coldStartMs,
      warmStartMs,
    },
  }
}

// =============================================================================
// Test Table Setup
// =============================================================================

async function setupTestTable(adapter: ContainerAdapter, database: DatabaseType): Promise<void> {
  const createTableSQL: Record<DatabaseType, string> = {
    postgres: `
      CREATE TABLE IF NOT EXISTS bench_throughput (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        value INTEGER NOT NULL,
        data JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `,
    clickhouse: `
      CREATE TABLE IF NOT EXISTS bench_throughput (
        id String,
        name String,
        value Int64,
        data String,
        created_at DateTime DEFAULT now()
      ) ENGINE = MergeTree()
      ORDER BY id
    `,
    mongo: '', // MongoDB doesn't need table creation
    duckdb: `
      CREATE TABLE IF NOT EXISTS bench_throughput (
        id VARCHAR PRIMARY KEY,
        name VARCHAR NOT NULL,
        value BIGINT NOT NULL,
        data JSON,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `,
    sqlite: `
      CREATE TABLE IF NOT EXISTS bench_throughput (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        value INTEGER NOT NULL,
        data TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    `,
  }

  const sql = createTableSQL[database]
  if (sql) {
    await containerExecute(adapter, sql)
  }
}

// =============================================================================
// Benchmark Scenarios
// =============================================================================

/**
 * Single Worker -> Single Container benchmark
 * Sequential operations from one connection
 */
async function runSingleWorkerBenchmark(
  adapter: ContainerAdapter,
  database: DatabaseType,
  durationSeconds: number
): Promise<ThroughputMetrics> {
  const latencies: number[] = []
  const endTime = Date.now() + durationSeconds * 1000
  let successful = 0
  let failed = 0

  while (Date.now() < endTime) {
    const result = await runSingleOperation(adapter, database)
    latencies.push(result.latencyMs)

    if (result.success) {
      successful++
    } else {
      failed++
    }
  }

  return calculateMetrics(latencies, successful, failed, durationSeconds)
}

/**
 * Concurrent requests benchmark
 * Multiple parallel requests to the container
 */
async function runConcurrentBenchmark(
  adapter: ContainerAdapter,
  database: DatabaseType,
  concurrency: number,
  durationSeconds: number
): Promise<ThroughputMetrics> {
  const latencies: number[] = []
  const endTime = Date.now() + durationSeconds * 1000
  let successful = 0
  let failed = 0

  while (Date.now() < endTime) {
    // Run batch of concurrent operations
    const batch = Array.from({ length: concurrency }, () =>
      runSingleOperation(adapter, database)
    )

    const results = await Promise.all(batch)

    for (const result of results) {
      latencies.push(result.latencyMs)
      if (result.success) {
        successful++
      } else {
        failed++
      }
    }
  }

  return calculateMetrics(latencies, successful, failed, durationSeconds)
}

/**
 * Sustained load benchmark
 * Continuous load over the full duration
 */
async function runSustainedLoadBenchmark(
  adapter: ContainerAdapter,
  database: DatabaseType,
  concurrency: number,
  durationSeconds: number
): Promise<ThroughputMetrics> {
  const latencies: number[] = []
  let successful = 0
  let failed = 0
  let activeRequests = 0

  const endTime = Date.now() + durationSeconds * 1000

  // Use a semaphore pattern for sustained load
  const runWithConcurrency = async (): Promise<void> => {
    while (Date.now() < endTime) {
      if (activeRequests < concurrency) {
        activeRequests++

        runSingleOperation(adapter, database)
          .then((result) => {
            latencies.push(result.latencyMs)
            if (result.success) {
              successful++
            } else {
              failed++
            }
            activeRequests--
          })
          .catch(() => {
            failed++
            activeRequests--
          })
      } else {
        // Wait a bit before checking again
        await sleep(1)
      }
    }

    // Wait for remaining requests to complete
    while (activeRequests > 0) {
      await sleep(10)
    }
  }

  await runWithConcurrency()

  return calculateMetrics(latencies, successful, failed, durationSeconds)
}

// =============================================================================
// Single Operation
// =============================================================================

async function runSingleOperation(
  adapter: ContainerAdapter,
  database: DatabaseType
): Promise<OperationResult> {
  const startTime = performance.now()
  const id = `bench-${Date.now()}-${Math.random().toString(36).slice(2)}`

  try {
    // Mix of read and write operations (70% read, 30% write)
    const isWrite = Math.random() < 0.3

    if (isWrite) {
      await runWriteOperation(adapter, database, id)
    } else {
      await runReadOperation(adapter, database)
    }

    return {
      success: true,
      latencyMs: performance.now() - startTime,
    }
  } catch (error) {
    return {
      success: false,
      latencyMs: performance.now() - startTime,
      error: error instanceof Error ? error.message : String(error),
    }
  }
}

async function runWriteOperation(
  adapter: ContainerAdapter,
  database: DatabaseType,
  id: string
): Promise<void> {
  const data = {
    id,
    name: `Item ${id}`,
    value: Math.floor(Math.random() * 1000000),
    data: JSON.stringify({ timestamp: Date.now(), random: Math.random() }),
  }

  switch (database) {
    case 'postgres':
    case 'duckdb':
    case 'sqlite':
      await containerExecute(
        adapter,
        `INSERT INTO bench_throughput (id, name, value, data) VALUES ($1, $2, $3, $4)
         ON CONFLICT (id) DO UPDATE SET value = $3, data = $4`,
        [data.id, data.name, data.value, data.data]
      )
      break
    case 'clickhouse':
      await containerExecute(
        adapter,
        `INSERT INTO bench_throughput (id, name, value, data) VALUES ($1, $2, $3, $4)`,
        [data.id, data.name, data.value, data.data]
      )
      break
    case 'mongo':
      // MongoDB uses HTTP bridge with insertOne endpoint
      const request = new Request(`http://container:${adapter.port}/insertOne`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          database: 'benchmark',
          collection: 'bench_throughput',
          document: { _id: data.id, name: data.name, value: data.value, data: data.data },
        }),
      })
      const response = await adapter.container.fetch(request)
      if (!response.ok) {
        const error = await response.text()
        throw new Error(`MongoDB insertOne failed: ${error}`)
      }
      break
  }
}

async function runReadOperation(
  adapter: ContainerAdapter,
  database: DatabaseType
): Promise<void> {
  // Try to read an existing record, or fall back to a scan
  if (database === 'mongo') {
    // MongoDB uses HTTP bridge with find endpoint
    const request = new Request(`http://container:${adapter.port}/find`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        database: 'benchmark',
        collection: 'bench_throughput',
        filter: {},
        limit: 1,
      }),
    })
    const response = await adapter.container.fetch(request)
    if (!response.ok) {
      const error = await response.text()
      throw new Error(`MongoDB find failed: ${error}`)
    }
    return
  }

  const queries: Record<DatabaseType, string> = {
    postgres: `SELECT * FROM bench_throughput ORDER BY created_at DESC LIMIT 1`,
    clickhouse: `SELECT * FROM bench_throughput ORDER BY created_at DESC LIMIT 1`,
    mongo: ``, // Handled above
    duckdb: `SELECT * FROM bench_throughput ORDER BY created_at DESC LIMIT 1`,
    sqlite: `SELECT * FROM bench_throughput ORDER BY created_at DESC LIMIT 1`,
  }

  await containerQuery(adapter, queries[database])
}

// =============================================================================
// Metrics Calculation
// =============================================================================

function calculateMetrics(
  latencies: number[],
  successful: number,
  failed: number,
  durationSeconds: number
): ThroughputMetrics {
  if (latencies.length === 0) {
    return {
      totalOperations: 0,
      successfulOperations: 0,
      failedOperations: 0,
      operationsPerSecond: 0,
      avgLatencyMs: 0,
      p50LatencyMs: 0,
      p95LatencyMs: 0,
      p99LatencyMs: 0,
      minLatencyMs: 0,
      maxLatencyMs: 0,
    }
  }

  // Sort for percentile calculations
  const sorted = [...latencies].sort((a, b) => a - b)

  return {
    totalOperations: latencies.length,
    successfulOperations: successful,
    failedOperations: failed,
    operationsPerSecond: latencies.length / durationSeconds,
    avgLatencyMs: latencies.reduce((a, b) => a + b, 0) / latencies.length,
    p50LatencyMs: sorted[Math.floor(sorted.length * 0.5)],
    p95LatencyMs: sorted[Math.floor(sorted.length * 0.95)],
    p99LatencyMs: sorted[Math.floor(sorted.length * 0.99)],
    minLatencyMs: sorted[0],
    maxLatencyMs: sorted[sorted.length - 1],
  }
}

// =============================================================================
// Cost Analysis
// =============================================================================

function calculateCosts(metrics: ThroughputMetrics, containerSize: ContainerSize): CostMetrics {
  const opsPerSecond = metrics.operationsPerSecond
  const opsPerHour = opsPerSecond * 3600

  // Container runtime cost per operation
  const containerHourlyCost = PRICING.containerHourly[containerSize]
  const containerRuntimeCostPerOp = opsPerHour > 0 ? containerHourlyCost / opsPerHour : 0
  const containerCostPer1M = containerRuntimeCostPerOp * 1_000_000

  // WASM + DO cost per operation (using 2MB blob optimization)
  // Assume 100 rows packed per blob operation
  const packingRatio = 100
  const readCostPerOp = (AVG_ROWS_PER_OP.mixed / packingRatio / 1_000_000) * PRICING.doRowsRead
  const writeCostPerOp = (AVG_ROWS_PER_OP.mixed / packingRatio / 1_000_000) * PRICING.doRowsWritten * 0.3 // 30% writes
  const workerCostPerOp = 1 / 1_000_000 * PRICING.workersRequests
  const wasmDoCostPerOp = readCostPerOp + writeCostPerOp + workerCostPerOp
  const wasmDoCostPer1M = wasmDoCostPerOp * 1_000_000

  // Break-even analysis: when does container beat WASM?
  // Container cost = hourly_rate, WASM cost = ops * per_op_cost
  // hourly_rate = ops * per_op_cost
  // ops = hourly_rate / per_op_cost
  const breakEvenOpsPerHour = wasmDoCostPerOp > 0 ? containerHourlyCost / wasmDoCostPerOp : 0

  // Monthly projections at various traffic levels
  const trafficLevels = [
    1_000_000,      // 1M ops/month
    10_000_000,     // 10M ops/month
    100_000_000,    // 100M ops/month
    1_000_000_000,  // 1B ops/month
    10_000_000_000, // 10B ops/month
  ]

  const monthlyProjections: MonthlyProjection[] = trafficLevels.map((opsPerMonth) => {
    // Container cost: hours needed * hourly rate
    const hoursNeeded = opsPerMonth / (opsPerHour || 1)
    const containerCost = hoursNeeded * containerHourlyCost

    // WASM + DO cost
    const wasmDoCost = opsPerMonth * wasmDoCostPerOp

    const winner = containerCost < wasmDoCost ? 'container' : 'wasm-do'
    const savings = Math.abs(containerCost - wasmDoCost)
    const savingsPercent = winner === 'container'
      ? (1 - containerCost / wasmDoCost) * 100
      : (1 - wasmDoCost / containerCost) * 100

    return {
      opsPerMonth,
      containerCost,
      wasmDoCost,
      winner,
      savings,
      savingsPercent,
    }
  })

  return {
    containerRuntimeCostPerOp,
    containerCostPer1M,
    wasmDoCostPer1M,
    breakEvenOpsPerHour,
    monthlyProjections,
  }
}

// =============================================================================
// Utility Functions
// =============================================================================

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
