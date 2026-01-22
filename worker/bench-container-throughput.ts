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

// =============================================================================
// Types
// =============================================================================

export interface Env {
  // Container bindings
  POSTGRES_CONTAINER: Container
  CLICKHOUSE_CONTAINER: Container
  MONGO_CONTAINER: Container
  DUCKDB_CONTAINER: Container
  SQLITE_CONTAINER: Container

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

      return new Response(JSON.stringify({
        error: 'Not found',
        availableEndpoints: [
          'POST /benchmark/container-throughput?database=postgres&concurrency=50&duration=60',
          'GET /benchmark/container-throughput/quick?database=postgres',
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
// Database Adapter Factory
// =============================================================================

function createDatabaseAdapter(database: DatabaseType, env: Env): ContainerDatabase {
  switch (database) {
    case 'postgres':
      return createPostgresContainer(env.POSTGRES_CONTAINER)
    case 'clickhouse':
      return createClickHouseContainer(env.CLICKHOUSE_CONTAINER)
    case 'mongo':
      return createMongoContainer(env.MONGO_CONTAINER)
    case 'duckdb':
      return createDuckDBContainer(env.DUCKDB_CONTAINER)
    case 'sqlite':
      return createSQLiteContainer(env.SQLITE_CONTAINER)
    default:
      throw new Error(`Unknown database: ${database}`)
  }
}

// =============================================================================
// Benchmark Execution
// =============================================================================

async function runThroughputBenchmark(
  params: BenchmarkParams,
  env: Env,
  ctx: ExecutionContext
): Promise<BenchmarkResult> {
  const { database, concurrency, duration, containerSize = 'standard-1' } = params

  // Create database adapter
  const db = createDatabaseAdapter(database, env)

  // Measure cold start
  const coldStartTime = performance.now()
  await db.connect()
  const coldStartMs = performance.now() - coldStartTime

  // Warm the connection
  await db.ping()
  const warmStartTime = performance.now()
  await db.ping()
  const warmStartMs = performance.now() - warmStartTime

  // Setup test table
  await setupTestTable(db, database)

  // Run benchmark scenarios
  const singleWorkerSingleContainer = await runSingleWorkerBenchmark(db, database, duration)
  const concurrentRequests = await runConcurrentBenchmark(db, database, concurrency, duration)
  const sustainedLoad = await runSustainedLoadBenchmark(db, database, concurrency, duration)

  // Calculate costs
  const costAnalysis = calculateCosts(sustainedLoad, containerSize)

  // Clean up
  await db.close()

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

async function setupTestTable(db: ContainerDatabase, database: DatabaseType): Promise<void> {
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
    await db.execute(sql)
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
  db: ContainerDatabase,
  database: DatabaseType,
  durationSeconds: number
): Promise<ThroughputMetrics> {
  const latencies: number[] = []
  const endTime = Date.now() + durationSeconds * 1000
  let successful = 0
  let failed = 0

  while (Date.now() < endTime) {
    const result = await runSingleOperation(db, database)
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
  db: ContainerDatabase,
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
      runSingleOperation(db, database)
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
  db: ContainerDatabase,
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

        runSingleOperation(db, database)
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
  db: ContainerDatabase,
  database: DatabaseType
): Promise<OperationResult> {
  const startTime = performance.now()
  const id = `bench-${Date.now()}-${Math.random().toString(36).slice(2)}`

  try {
    // Mix of read and write operations (70% read, 30% write)
    const isWrite = Math.random() < 0.3

    if (isWrite) {
      await runWriteOperation(db, database, id)
    } else {
      await runReadOperation(db, database, id)
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
  db: ContainerDatabase,
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
      await db.execute(
        `INSERT INTO bench_throughput (id, name, value, data) VALUES ($1, $2, $3, $4)
         ON CONFLICT (id) DO UPDATE SET value = $3, data = $4`,
        [data.id, data.name, data.value, data.data]
      )
      break
    case 'clickhouse':
      await db.execute(
        `INSERT INTO bench_throughput (id, name, value, data) VALUES ($1, $2, $3, $4)`,
        [data.id, data.name, data.value, data.data]
      )
      break
    case 'mongo':
      // MongoDB uses different API
      await db.execute(
        `INSERT INTO bench_throughput (id, name, value, data) VALUES ($1, $2, $3, $4)`,
        [data.id, data.name, data.value, data.data]
      )
      break
  }
}

async function runReadOperation(
  db: ContainerDatabase,
  database: DatabaseType,
  id: string
): Promise<void> {
  // Try to read an existing record, or fall back to a scan
  const queries: Record<DatabaseType, string> = {
    postgres: `SELECT * FROM bench_throughput ORDER BY created_at DESC LIMIT 1`,
    clickhouse: `SELECT * FROM bench_throughput ORDER BY created_at DESC LIMIT 1`,
    mongo: `SELECT * FROM bench_throughput LIMIT 1`,
    duckdb: `SELECT * FROM bench_throughput ORDER BY created_at DESC LIMIT 1`,
    sqlite: `SELECT * FROM bench_throughput ORDER BY created_at DESC LIMIT 1`,
  }

  await db.query(queries[database])
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
