/**
 * OLTP Benchmark Worker
 *
 * Cloudflare Worker that runs OLTP benchmarks across multiple databases.
 * Uses staged datasets from R2 and outputs JSONL results.
 *
 * Databases: db4, evodb, postgres, sqlite, @db4/mongo, @dotdo/mongodb, sdb
 * Datasets: ecommerce, saas, social (from R2 at sizes 1mb, 10mb, 100mb, 1gb)
 * Operations: point lookup, range scan, single insert, batch insert, update, delete, transactions
 *
 * Endpoint: POST /benchmark/oltp
 * Query params: ?database=db4&dataset=ecommerce&size=100mb
 */

import { DurableObject } from 'cloudflare:workers'
import { Hono } from 'hono'

// ============================================================================
// Inlined Types (from instrumentation/types.ts)
// Inlined to avoid bundling issues in Worker context
// ============================================================================

type BenchmarkEnvironment = 'worker' | 'do' | 'container' | 'local'

interface BenchmarkResult {
  benchmark: string
  database: string
  dataset: string
  p50_ms: number
  p99_ms: number
  min_ms: number
  max_ms: number
  mean_ms: number
  stddev_ms?: number
  ops_per_sec: number
  iterations: number
  total_duration_ms?: number
  vfs_reads: number
  vfs_writes: number
  vfs_bytes_read: number
  vfs_bytes_written: number
  timestamp: string
  environment: BenchmarkEnvironment
  colo?: string
  run_id: string
}

// ============================================================================
// Configuration
// ============================================================================

const BENCHMARK_CONFIG = {
  iterations: {
    lookup: 100,
    scan: 50,
    insert: 100,
    batch: 10,
    update: 100,
    delete: 50,
    transaction: 20,
    // Stress test operations - fewer iterations since each is expensive
    fullscan: 10,
    aggregatecount: 10,
    aggregatesum: 10,
    complexfilter: 10,
    sortall: 5,
  },
  batchSizes: {
    small: 10,
    medium: 100,
    large: 1000,
  },
  warmupIterations: 5,
} as const

// Supported databases
type DatabaseType = 'db4' | 'evodb' | 'postgres' | 'sqlite' | 'db4-mongo' | 'dotdo-mongodb' | 'sdb'

// Supported datasets
type DatasetType = 'ecommerce' | 'saas' | 'social'

// Supported sizes
type SizeType = '1mb' | '10mb' | '100mb' | '1gb'

// OLTP benchmark operations
type OLTPOperation =
  | 'point_lookup'
  | 'range_scan'
  | 'single_insert'
  | 'batch_insert'
  | 'update'
  | 'delete'
  | 'transaction'
  // Stress test operations
  | 'full_scan'
  | 'aggregate_count'
  | 'aggregate_sum'
  | 'complex_filter'
  | 'sort_all'

// ============================================================================
// Types
// ============================================================================

interface Env {
  // Durable Object namespaces for each database
  DB4_DO: DurableObjectNamespace<OLTPBenchDO>
  EVODB_DO: DurableObjectNamespace<OLTPBenchDO>
  POSTGRES_DO: DurableObjectNamespace<OLTPBenchDO>
  SQLITE_DO: DurableObjectNamespace<OLTPBenchDO>
  DB4_MONGO_DO: DurableObjectNamespace<OLTPBenchDO>
  DOTDO_MONGODB_DO: DurableObjectNamespace<OLTPBenchDO>
  SDB_DO: DurableObjectNamespace<OLTPBenchDO>

  // R2 buckets
  DATASETS: R2Bucket
  RESULTS: R2Bucket
}

interface BenchmarkRequest {
  database: DatabaseType
  dataset: DatasetType
  size: SizeType
  operations?: OLTPOperation[]
  iterations?: number
  runId?: string
}

interface BenchmarkTiming {
  name: string
  iterations: number
  totalMs: number
  minMs: number
  maxMs: number
  meanMs: number
  stddevMs: number
  p50Ms: number
  p99Ms: number
  opsPerSec: number
}

interface OLTPBenchmarkResults {
  runId: string
  database: DatabaseType
  dataset: DatasetType
  size: SizeType
  timestamp: string
  environment: BenchmarkEnvironment
  colo?: string
  benchmarks: BenchmarkTiming[]
  datasetStats: {
    tablesLoaded: string[]
    totalRecords: number
    loadTimeMs: number
  }
  summary: {
    totalDurationMs: number
    totalOperations: number
    overallOpsPerSec: number
  }
}

// Generic document type for benchmarks (named BenchDoc to avoid conflict with @db4/client Document)
interface BenchDoc {
  id?: string
  _id?: string
  $id?: string
  [key: string]: unknown
}

// ============================================================================
// Utility Functions
// ============================================================================

function generateRunId(): string {
  const timestamp = Date.now().toString(36)
  const random = Math.random().toString(36).substring(2, 8)
  return `oltp-${timestamp}-${random}`
}

function calculatePercentile(sortedValues: number[], percentile: number): number {
  if (sortedValues.length === 0) return 0
  const index = Math.ceil((percentile / 100) * sortedValues.length) - 1
  return sortedValues[Math.max(0, index)]
}

function calculateStats(times: number[]): {
  totalMs: number
  minMs: number
  maxMs: number
  meanMs: number
  stddevMs: number
  p50Ms: number
  p99Ms: number
} {
  if (times.length === 0) {
    return { totalMs: 0, minMs: 0, maxMs: 0, meanMs: 0, stddevMs: 0, p50Ms: 0, p99Ms: 0 }
  }

  const sorted = [...times].sort((a, b) => a - b)
  const totalMs = times.reduce((a, b) => a + b, 0)
  const meanMs = totalMs / times.length

  // Calculate standard deviation
  const squareDiffs = times.map((t) => Math.pow(t - meanMs, 2))
  const avgSquareDiff = squareDiffs.reduce((a, b) => a + b, 0) / times.length
  const stddevMs = Math.sqrt(avgSquareDiff)

  return {
    totalMs,
    minMs: sorted[0],
    maxMs: sorted[sorted.length - 1],
    meanMs,
    stddevMs,
    p50Ms: calculatePercentile(sorted, 50),
    p99Ms: calculatePercentile(sorted, 99),
  }
}

// Parse JSONL content into array of documents
function parseJSONL(content: string): BenchDoc[] {
  return content
    .trim()
    .split('\n')
    .filter((line) => line.trim())
    .map((line) => JSON.parse(line))
}

// ============================================================================
// Database Adapter Interface
// ============================================================================

interface DatabaseAdapter {
  name: DatabaseType
  connect(): Promise<void>
  close(): Promise<void>

  // OLTP operations
  pointLookup(collection: string, id: string): Promise<BenchDoc | null>
  rangeScan(collection: string, filter: Record<string, unknown>, limit?: number): Promise<BenchDoc[]>
  insert(collection: string, doc: BenchDoc): Promise<void>
  batchInsert(collection: string, docs: BenchDoc[]): Promise<void>
  update(collection: string, id: string, updates: Record<string, unknown>): Promise<void>
  delete(collection: string, id: string): Promise<boolean>
  transaction(fn: () => Promise<void>): Promise<void>

  // Data loading
  loadData(collection: string, docs: BenchDoc[]): Promise<number>
  getCollectionIds(collection: string): Promise<string[]>
}

// ============================================================================
// Database Adapters
// ============================================================================

/**
 * DB4 Adapter - Pure TypeScript document store
 * Uses @db4/client for the client SDK
 *
 * @db4/client provides an in-memory implementation when created with a baseUrl.
 * The client stores data in a module-level Map for testing purposes.
 */
class DB4Adapter implements DatabaseAdapter {
  name: DatabaseType = 'db4'
  private client: import('@db4/client').DB4Client | null = null
  // Use any here because @db4/client's Document type requires id: string (not optional)
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private collections: Map<string, any> = new Map()

  async connect(): Promise<void> {
    const { createClient } = await import('@db4/client')
    // Create client - the @db4/client uses an in-memory store by default for testing
    // The baseUrl is required but the client stores data in-memory for testing
    this.client = createClient({ baseUrl: 'http://localhost:0' })
  }

  async close(): Promise<void> {
    this.collections.clear()
    await this.client?.close()
    this.client = null
  }

  private getCollection(name: string) {
    if (!this.client) throw new Error('Client not connected')
    if (!this.collections.has(name)) {
      // Use any type to avoid @db4/core Document constraint (requires id: string)
      this.collections.set(name, this.client.collection(name))
    }
    return this.collections.get(name)!
  }

  private normalizeId(doc: BenchDoc): string {
    return (doc.id ?? doc._id ?? doc.$id ?? '') as string
  }

  async pointLookup(collection: string, id: string): Promise<BenchDoc | null> {
    const col = this.getCollection(collection)
    const result = await col.get(id)
    return result as BenchDoc | null
  }

  async rangeScan(collection: string, filter: Record<string, unknown>, limit = 100): Promise<BenchDoc[]> {
    const col = this.getCollection(collection)
    const results = await col.findMany({ filter, limit })
    return results.documents as BenchDoc[]
  }

  async insert(collection: string, doc: BenchDoc): Promise<void> {
    const col = this.getCollection(collection)
    await col.create(doc)
  }

  async batchInsert(collection: string, docs: BenchDoc[]): Promise<void> {
    const col = this.getCollection(collection)
    await col.createMany(docs)
  }

  async update(collection: string, id: string, updates: Record<string, unknown>): Promise<void> {
    const col = this.getCollection(collection)
    await col.update(id, updates)
  }

  async delete(collection: string, id: string): Promise<boolean> {
    const col = this.getCollection(collection)
    const result = await col.delete(id)
    return result.deleted
  }

  async transaction(fn: () => Promise<void>): Promise<void> {
    // DB4 client doesn't expose transactions directly, execute serially
    await fn()
  }

  async loadData(collection: string, docs: BenchDoc[]): Promise<number> {
    await this.batchInsert(collection, docs)
    return docs.length
  }

  async getCollectionIds(collection: string): Promise<string[]> {
    const col = this.getCollection(collection)
    const results = await col.findMany({ limit: 10000, projection: ['id'] })
    return results.documents.map((d: BenchDoc) => d.id ?? '') as string[]
  }
}

// TODO: EvoDB Adapter - @evodb/core package is not linked to the workspace
// The EvoDB implementation exists in packages/evodb/core but is not published
// or linked to the workspace's node_modules. The npm package 'evodb' is a
// different, unrelated project.
// Commenting out until @evodb/core is properly linked.
//
// class EvoDBAdapter implements DatabaseAdapter {
//   name: DatabaseType = 'evodb'
//   // Implementation would use: new EvoDB({ mode: 'development' })
// }

/**
 * Placeholder EvoDBAdapter - not implemented
 * TODO: Implement when @evodb/core is properly linked to the workspace
 */
class EvoDBAdapter implements DatabaseAdapter {
  name: DatabaseType = 'evodb'

  async connect(): Promise<void> {
    throw new Error('@evodb/core is not linked to this workspace. TODO: Add workspace link to packages/evodb/core.')
  }

  async close(): Promise<void> {}

  async pointLookup(_collection: string, _id: string): Promise<BenchDoc | null> {
    throw new Error('Not implemented')
  }

  async rangeScan(_collection: string, _filter: Record<string, unknown>, _limit?: number): Promise<BenchDoc[]> {
    throw new Error('Not implemented')
  }

  async insert(_collection: string, _doc: BenchDoc): Promise<void> {
    throw new Error('Not implemented')
  }

  async batchInsert(_collection: string, _docs: BenchDoc[]): Promise<void> {
    throw new Error('Not implemented')
  }

  async update(_collection: string, _id: string, _updates: Record<string, unknown>): Promise<void> {
    throw new Error('Not implemented')
  }

  async delete(_collection: string, _id: string): Promise<boolean> {
    throw new Error('Not implemented')
  }

  async transaction(_fn: () => Promise<void>): Promise<void> {
    throw new Error('Not implemented')
  }

  async loadData(_collection: string, _docs: BenchDoc[]): Promise<number> {
    throw new Error('Not implemented')
  }

  async getCollectionIds(_collection: string): Promise<string[]> {
    throw new Error('Not implemented')
  }
}

// TODO: PostgreSQL Adapter - requires a running PostgresDO instance
// The @dotdo/sqlite client requires a WebSocket URL to connect to a Durable Object.
// For in-memory benchmarking, we would need to run this inside a DO context.
// Commenting out until we have a proper PostgresDO endpoint for benchmarking.
//
// class PostgresAdapter implements DatabaseAdapter {
//   name: DatabaseType = 'postgres'
//   // Implementation would use @dotdo/sqlite createClient({ url: 'wss://...' })
// }

/**
 * Placeholder PostgresAdapter - not implemented
 * TODO: Implement when PostgresDO endpoint is available for benchmarking
 */
class PostgresAdapter implements DatabaseAdapter {
  name: DatabaseType = 'postgres'

  async connect(): Promise<void> {
    throw new Error('PostgreSQL adapter requires a running PostgresDO instance. TODO: Implement when endpoint is available.')
  }

  async close(): Promise<void> {}

  async pointLookup(_collection: string, _id: string): Promise<BenchDoc | null> {
    throw new Error('Not implemented')
  }

  async rangeScan(_collection: string, _filter: Record<string, unknown>, _limit?: number): Promise<BenchDoc[]> {
    throw new Error('Not implemented')
  }

  async insert(_collection: string, _doc: BenchDoc): Promise<void> {
    throw new Error('Not implemented')
  }

  async batchInsert(_collection: string, _docs: BenchDoc[]): Promise<void> {
    throw new Error('Not implemented')
  }

  async update(_collection: string, _id: string, _updates: Record<string, unknown>): Promise<void> {
    throw new Error('Not implemented')
  }

  async delete(_collection: string, _id: string): Promise<boolean> {
    throw new Error('Not implemented')
  }

  async transaction(_fn: () => Promise<void>): Promise<void> {
    throw new Error('Not implemented')
  }

  async loadData(_collection: string, _docs: BenchDoc[]): Promise<number> {
    throw new Error('Not implemented')
  }

  async getCollectionIds(_collection: string): Promise<string[]> {
    throw new Error('Not implemented')
  }
}

// TODO: SQLite Adapter - requires a running SQLiteDO instance
// The @dotdo/sqlite client requires a WebSocket URL to connect to a Durable Object.
// For in-memory benchmarking, we could use createMemoryDb() but that's internal.
// Commenting out until we have a proper SQLiteDO endpoint for benchmarking.
//
// class SQLiteAdapter implements DatabaseAdapter {
//   name: DatabaseType = 'sqlite'
//   // Implementation would use @dotdo/sqlite createClient({ url: 'wss://...' })
// }

/**
 * Placeholder SQLiteAdapter - not implemented
 * TODO: Implement when SQLiteDO endpoint is available for benchmarking
 */
class SQLiteAdapter implements DatabaseAdapter {
  name: DatabaseType = 'sqlite'

  async connect(): Promise<void> {
    throw new Error('SQLite adapter requires a running SQLiteDO instance. TODO: Implement when endpoint is available.')
  }

  async close(): Promise<void> {}

  async pointLookup(_collection: string, _id: string): Promise<BenchDoc | null> {
    throw new Error('Not implemented')
  }

  async rangeScan(_collection: string, _filter: Record<string, unknown>, _limit?: number): Promise<BenchDoc[]> {
    throw new Error('Not implemented')
  }

  async insert(_collection: string, _doc: BenchDoc): Promise<void> {
    throw new Error('Not implemented')
  }

  async batchInsert(_collection: string, _docs: BenchDoc[]): Promise<void> {
    throw new Error('Not implemented')
  }

  async update(_collection: string, _id: string, _updates: Record<string, unknown>): Promise<void> {
    throw new Error('Not implemented')
  }

  async delete(_collection: string, _id: string): Promise<boolean> {
    throw new Error('Not implemented')
  }

  async transaction(_fn: () => Promise<void>): Promise<void> {
    throw new Error('Not implemented')
  }

  async loadData(_collection: string, _docs: BenchDoc[]): Promise<number> {
    throw new Error('Not implemented')
  }

  async getCollectionIds(_collection: string): Promise<string[]> {
    throw new Error('Not implemented')
  }
}

/**
 * @db4/mongo Adapter - MongoDB API with db4 backend
 * Uses @db4/client with MongoDB-style API compatibility
 *
 * This is essentially the same as DB4Adapter but uses _id as the primary key
 * to match MongoDB conventions.
 */
class DB4MongoAdapter implements DatabaseAdapter {
  name: DatabaseType = 'db4-mongo'
  private client: import('@db4/client').DB4Client | null = null
  // Use any here because @db4/client's Document type requires id: string (not optional)
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private collections: Map<string, any> = new Map()

  async connect(): Promise<void> {
    const { createClient } = await import('@db4/client')
    // Create client - the @db4/client uses an in-memory store by default for testing
    this.client = createClient({ baseUrl: 'http://localhost:0' })
  }

  async close(): Promise<void> {
    this.collections.clear()
    await this.client?.close()
    this.client = null
  }

  private getCollection(name: string) {
    if (!this.client) throw new Error('Client not connected')
    if (!this.collections.has(name)) {
      // Use any type to avoid @db4/core Document constraint (requires id: string)
      this.collections.set(name, this.client.collection(name))
    }
    return this.collections.get(name)!
  }

  private normalizeId(doc: BenchDoc): string {
    return (doc._id ?? doc.id ?? doc.$id ?? '') as string
  }

  async pointLookup(collection: string, id: string): Promise<BenchDoc | null> {
    const col = this.getCollection(collection)
    // Use findOne with _id filter for MongoDB-style lookup
    const result = await col.findOne({ _id: id } as Record<string, unknown>)
    return result as BenchDoc | null
  }

  async rangeScan(collection: string, filter: Record<string, unknown>, limit = 100): Promise<BenchDoc[]> {
    const col = this.getCollection(collection)
    const results = await col.findMany({ filter, limit })
    return results.documents as BenchDoc[]
  }

  async insert(collection: string, doc: BenchDoc): Promise<void> {
    const col = this.getCollection(collection)
    const mongoDoc = { ...doc, _id: this.normalizeId(doc) }
    await col.create(mongoDoc)
  }

  async batchInsert(collection: string, docs: BenchDoc[]): Promise<void> {
    const col = this.getCollection(collection)
    const mongoDocs = docs.map((doc) => ({
      ...doc,
      _id: this.normalizeId(doc)
    }))
    await col.createMany(mongoDocs)
  }

  async update(collection: string, id: string, updates: Record<string, unknown>): Promise<void> {
    const col = this.getCollection(collection)
    // @db4/client update takes (id, updates) directly
    await col.update(id, updates)
  }

  async delete(collection: string, id: string): Promise<boolean> {
    const col = this.getCollection(collection)
    const result = await col.delete(id)
    return result.deleted
  }

  async transaction(fn: () => Promise<void>): Promise<void> {
    // db4/mongo doesn't have native transactions
    await fn()
  }

  async loadData(collection: string, docs: BenchDoc[]): Promise<number> {
    await this.batchInsert(collection, docs)
    return docs.length
  }

  async getCollectionIds(collection: string): Promise<string[]> {
    const col = this.getCollection(collection)
    const results = await col.findMany({ limit: 10000, projection: ['_id'] })
    return results.documents.map((d: BenchDoc) => (d._id ?? d.id ?? '') as string)
  }
}

// TODO: @dotdo/mongodb Adapter - package does not exist
// There is no @dotdo/mongodb package in this monorepo.
// This was a hypothetical MongoDB compatibility layer.
// Commenting out until the package is implemented.
//
// class DotDoMongoDBAdapter implements DatabaseAdapter {
//   name: DatabaseType = 'dotdo-mongodb'
//   // Implementation would use @dotdo/mongodb MongoClient
// }

/**
 * Placeholder DotDoMongoDBAdapter - not implemented
 * TODO: Implement when @dotdo/mongodb package is available
 */
class DotDoMongoDBAdapter implements DatabaseAdapter {
  name: DatabaseType = 'dotdo-mongodb'

  async connect(): Promise<void> {
    throw new Error('@dotdo/mongodb package does not exist. TODO: Implement when package is available.')
  }

  async close(): Promise<void> {}

  async pointLookup(_collection: string, _id: string): Promise<BenchDoc | null> {
    throw new Error('Not implemented')
  }

  async rangeScan(_collection: string, _filter: Record<string, unknown>, _limit?: number): Promise<BenchDoc[]> {
    throw new Error('Not implemented')
  }

  async insert(_collection: string, _doc: BenchDoc): Promise<void> {
    throw new Error('Not implemented')
  }

  async batchInsert(_collection: string, _docs: BenchDoc[]): Promise<void> {
    throw new Error('Not implemented')
  }

  async update(_collection: string, _id: string, _updates: Record<string, unknown>): Promise<void> {
    throw new Error('Not implemented')
  }

  async delete(_collection: string, _id: string): Promise<boolean> {
    throw new Error('Not implemented')
  }

  async transaction(_fn: () => Promise<void>): Promise<void> {
    throw new Error('Not implemented')
  }

  async loadData(_collection: string, _docs: BenchDoc[]): Promise<number> {
    throw new Error('Not implemented')
  }

  async getCollectionIds(_collection: string): Promise<string[]> {
    throw new Error('Not implemented')
  }
}

// TODO: SDB Adapter - requires a running SDB Durable Object instance
// The @dotdo/sdb DB() function requires a schema and { url: string } config
// to connect to a remote SDB server. There's no in-memory mode.
// Commenting out until we have a proper SDB endpoint for benchmarking.
//
// class SDBAdapter implements DatabaseAdapter {
//   name: DatabaseType = 'sdb'
//   // Implementation would use:
//   // const db = DB({ EntityType: { field: 'type', ... } }, { url: 'https://tenant.sdb.do' })
// }

/**
 * Placeholder SDBAdapter - not implemented
 * TODO: Implement when SDB endpoint is available for benchmarking
 */
class SDBAdapter implements DatabaseAdapter {
  name: DatabaseType = 'sdb'

  async connect(): Promise<void> {
    throw new Error('SDB adapter requires a running SDB Durable Object instance. TODO: Implement when endpoint is available.')
  }

  async close(): Promise<void> {}

  async pointLookup(_collection: string, _id: string): Promise<BenchDoc | null> {
    throw new Error('Not implemented')
  }

  async rangeScan(_collection: string, _filter: Record<string, unknown>, _limit?: number): Promise<BenchDoc[]> {
    throw new Error('Not implemented')
  }

  async insert(_collection: string, _doc: BenchDoc): Promise<void> {
    throw new Error('Not implemented')
  }

  async batchInsert(_collection: string, _docs: BenchDoc[]): Promise<void> {
    throw new Error('Not implemented')
  }

  async update(_collection: string, _id: string, _updates: Record<string, unknown>): Promise<void> {
    throw new Error('Not implemented')
  }

  async delete(_collection: string, _id: string): Promise<boolean> {
    throw new Error('Not implemented')
  }

  async transaction(_fn: () => Promise<void>): Promise<void> {
    throw new Error('Not implemented')
  }

  async loadData(_collection: string, _docs: BenchDoc[]): Promise<number> {
    throw new Error('Not implemented')
  }

  async getCollectionIds(_collection: string): Promise<string[]> {
    throw new Error('Not implemented')
  }
}

// ============================================================================
// Adapter Factory
// ============================================================================

function createAdapter(database: DatabaseType): DatabaseAdapter {
  switch (database) {
    case 'db4':
      return new DB4Adapter()
    case 'evodb':
      return new EvoDBAdapter()
    case 'postgres':
      return new PostgresAdapter()
    case 'sqlite':
      return new SQLiteAdapter()
    case 'db4-mongo':
      return new DB4MongoAdapter()
    case 'dotdo-mongodb':
      return new DotDoMongoDBAdapter()
    case 'sdb':
      return new SDBAdapter()
    default:
      throw new Error(`Unknown database: ${database}`)
  }
}

// ============================================================================
// Dataset Configuration
// ============================================================================

const DATASET_TABLES: Record<DatasetType, string[]> = {
  ecommerce: ['customers', 'products', 'orders', 'reviews'],
  saas: ['orgs', 'users', 'workspaces', 'documents'],
  social: ['users', 'posts', 'comments', 'likes', 'follows'],
}

// ============================================================================
// OLTP Benchmark Durable Object
// ============================================================================

export class OLTPBenchDO extends DurableObject<Env> {
  private adapter: DatabaseAdapter  | null = null
  private loadedData: Map<string, string[]> = new Map() // collection -> ids
  private dataLoaded = false

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
  }

  /**
   * Load dataset from R2 into the database
   */
  private async loadDataset(
    database: DatabaseType,
    dataset: DatasetType,
    size: SizeType
  ): Promise<{ tablesLoaded: string[]; totalRecords: number; loadTimeMs: number }> {
    const startTime = performance.now()

    // Create and connect adapter
    this.adapter = createAdapter(database)
    await this.adapter.connect()

    const tables = DATASET_TABLES[dataset]
    const tablesLoaded: string[] = []
    let totalRecords = 0

    for (const table of tables) {
      const key = `oltp/${dataset}/${size}/${table}.jsonl`
      const object = await this.env.DATASETS.get(key)

      if (!object) {
        console.warn(`Dataset not found: ${key}`)
        continue
      }

      const content = await object.text()
      const docs = parseJSONL(content)

      const loaded = await this.adapter.loadData(table, docs)
      totalRecords += loaded
      tablesLoaded.push(table)

      // Store IDs for benchmark operations
      const ids = await this.adapter.getCollectionIds(table)
      this.loadedData.set(table, ids)
    }

    this.dataLoaded = true

    return {
      tablesLoaded,
      totalRecords,
      loadTimeMs: performance.now() - startTime,
    }
  }

  /**
   * Run a single benchmark operation
   */
  private async runOperation(
    operation: OLTPOperation,
    iterations: number
  ): Promise<BenchmarkTiming> {
    const times: number[] = []

    // Get a random collection and its IDs
    const collections = Array.from(this.loadedData.keys())
    if (collections.length === 0) {
      throw new Error('No data loaded')
    }

    const collection = collections[0]
    const ids = this.loadedData.get(collection) ?? []

    // Warmup
    for (let i = 0; i < BENCHMARK_CONFIG.warmupIterations; i++) {
      await this.executeOperation(operation, collection, ids)
    }

    // Benchmark - time entire batch, force clock tick with real I/O
    const BATCH_SIZE = 100 // Batch many ops together to get measurable time
    const numBatches = Math.max(1, Math.floor(iterations / BATCH_SIZE))

    for (let batch = 0; batch < numBatches; batch++) {
      // Force clock to tick with minimal I/O (HEAD request is fast)
      await fetch('https://1.1.1.1/cdn-cgi/trace', { method: 'HEAD' }).catch(() => {})
      const batchStart = performance.now()

      for (let i = 0; i < BATCH_SIZE; i++) {
        await this.executeOperation(operation, collection, ids)
      }

      // Force clock to tick after batch with I/O
      await fetch('https://1.1.1.1/cdn-cgi/trace', { method: 'HEAD' }).catch(() => {})
      const batchTime = performance.now() - batchStart
      const timePerOp = batchTime / BATCH_SIZE

      // Record per-op time for this batch
      for (let i = 0; i < BATCH_SIZE; i++) {
        times.push(timePerOp)
      }
    }

    const stats = calculateStats(times)
    const effectiveIterations = times.length

    return {
      name: operation,
      iterations: effectiveIterations,
      ...stats,
      opsPerSec: stats.totalMs > 0 ? effectiveIterations / (stats.totalMs / 1000) : null,
    }
  }

  /**
   * Execute a single operation instance
   */
  private async executeOperation(
    operation: OLTPOperation,
    collection: string,
    ids: string[]
  ): Promise<void> {
    const randomId = ids[Math.floor(Math.random() * ids.length)]
    const randomIds = Array.from({ length: 10 }, () =>
      ids[Math.floor(Math.random() * ids.length)]
    )

    switch (operation) {
      case 'point_lookup':
        await this.adapter!.pointLookup(collection, randomId)
        break

      case 'range_scan':
        // Scan with a status filter (common in OLTP)
        await this.adapter!.rangeScan(collection, { status: 'active' }, 100)
        break

      case 'single_insert':
        const newId = `bench-${Date.now()}-${Math.random().toString(36).slice(2)}`
        await this.adapter!.insert(collection, {
          id: newId,
          _id: newId,
          $id: newId,
          name: `Benchmark Item ${newId}`,
          status: 'pending',
          created_at: new Date().toISOString(),
        })
        break

      case 'batch_insert':
        const batchDocs = Array.from({ length: BENCHMARK_CONFIG.batchSizes.medium }, (_, i) => {
          const batchId = `batch-${Date.now()}-${i}-${Math.random().toString(36).slice(2)}`
          return {
            id: batchId,
            _id: batchId,
            $id: batchId,
            name: `Batch Item ${i}`,
            status: 'pending',
            created_at: new Date().toISOString(),
          }
        })
        await this.adapter!.batchInsert(collection, batchDocs)
        break

      case 'update':
        await this.adapter!.update(collection, randomId, {
          updated_at: new Date().toISOString(),
          modified_count: Math.floor(Math.random() * 100),
        })
        break

      case 'delete':
        // Create a temporary item to delete
        const deleteId = `delete-${Date.now()}-${Math.random().toString(36).slice(2)}`
        await this.adapter!.insert(collection, {
          id: deleteId,
          _id: deleteId,
          $id: deleteId,
          name: 'To Delete',
          status: 'temporary',
        })
        await this.adapter!.delete(collection, deleteId)
        break

      case 'transaction':
        await this.adapter!.transaction(async () => {
          // Simulate a transaction: read, modify, write
          const doc = await this.adapter!.pointLookup(collection, randomId)
          if (doc) {
            await this.adapter!.update(collection, randomId, {
              tx_updated: new Date().toISOString(),
            })
          }
          // Also insert a new record
          const txId = `tx-${Date.now()}-${Math.random().toString(36).slice(2)}`
          await this.adapter!.insert(collection, {
            id: txId,
            _id: txId,
            $id: txId,
            name: 'Transaction Item',
            status: 'committed',
          })
        })
        break

      // Stress test operations - these should take measurable time
      case 'full_scan': {
        // Scan ALL records without limit - forces iteration through entire dataset
        const allDocs = await this.adapter!.rangeScan(collection, {}, 1000000)
        // Force iteration through results to prevent lazy evaluation
        let count = 0
        for (const doc of allDocs) {
          if (doc.id) count++
        }
        void count // prevent optimization
        break
      }

      case 'aggregate_count': {
        // Scan all and count matching criteria
        const docs = await this.adapter!.rangeScan(collection, {}, 1000000)
        let activeCount = 0
        let pendingCount = 0
        for (const doc of docs) {
          if (doc.status === 'active') activeCount++
          if (doc.status === 'pending') pendingCount++
        }
        void (activeCount + pendingCount)
        break
      }

      case 'aggregate_sum': {
        // Scan all and sum numeric fields
        const docs = await this.adapter!.rangeScan(collection, {}, 1000000)
        let total = 0
        for (const doc of docs) {
          if (typeof doc.amount === 'number') total += doc.amount
          if (typeof doc.price === 'number') total += doc.price
          if (typeof doc.quantity === 'number') total += doc.quantity
        }
        void total
        break
      }

      case 'complex_filter': {
        // Multi-condition filter requiring full scan and evaluation
        const docs = await this.adapter!.rangeScan(collection, {}, 1000000)
        const filtered = docs.filter(doc => {
          const hasStatus = doc.status === 'active' || doc.status === 'completed'
          const hasDate = doc.created_at && doc.created_at > '2024-01-01'
          const hasAmount = typeof doc.amount === 'number' && doc.amount > 100
          return hasStatus && hasDate && hasAmount
        })
        void filtered.length
        break
      }

      case 'sort_all': {
        // Scan all and sort - memory and CPU intensive
        const docs = await this.adapter!.rangeScan(collection, {}, 1000000)
        const sorted = [...docs].sort((a, b) => {
          const aDate = String(a.created_at ?? '')
          const bDate = String(b.created_at ?? '')
          return bDate.localeCompare(aDate) // descending
        })
        void sorted.length
        break
      }
    }
  }

  /**
   * Run all OLTP benchmarks
   */
  async runBenchmarks(request: BenchmarkRequest): Promise<OLTPBenchmarkResults> {
    const { database, dataset, size, operations, iterations, runId } = request

    const actualRunId = runId ?? generateRunId()
    const actualOperations: OLTPOperation[] = operations ?? [
      'point_lookup',
      'range_scan',
      'single_insert',
      'batch_insert',
      'update',
      'delete',
      'transaction',
    ]

    // Load dataset
    const datasetStats = await this.loadDataset(database, dataset, size)

    const benchmarks: BenchmarkTiming[] = []
    const startTime = performance.now()

    for (const op of actualOperations) {
      const opIterations = iterations ?? BENCHMARK_CONFIG.iterations[op.replace('_', '') as keyof typeof BENCHMARK_CONFIG.iterations] ?? 100
      try {
        const timing = await this.runOperation(op, opIterations)
        benchmarks.push(timing)
      } catch (error) {
        console.error(`Error running ${op}:`, error)
        // Add failed result
        benchmarks.push({
          name: op,
          iterations: 0,
          totalMs: 0,
          minMs: 0,
          maxMs: 0,
          meanMs: 0,
          stddevMs: 0,
          p50Ms: 0,
          p99Ms: 0,
          opsPerSec: 0,
        })
      }
    }

    const totalDurationMs = performance.now() - startTime
    const totalOperations = benchmarks.reduce((sum, b) => sum + b.iterations, 0)

    // Cleanup
    await this.adapter?.close()
    this.adapter = null
    this.loadedData.clear()
    this.dataLoaded = false

    return {
      runId: actualRunId,
      database,
      dataset,
      size,
      timestamp: new Date().toISOString(),
      environment: 'do',
      benchmarks,
      datasetStats,
      summary: {
        totalDurationMs,
        totalOperations,
        overallOpsPerSec: totalOperations / (totalDurationMs / 1000),
      },
    }
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)

    if (request.method === 'POST' && url.pathname === '/run') {
      try {
        const body = (await request.json()) as BenchmarkRequest
        const results = await this.runBenchmarks(body)
        return new Response(JSON.stringify(results, null, 2), {
          headers: { 'Content-Type': 'application/json' },
        })
      } catch (error) {
        return new Response(
          JSON.stringify({ error: error instanceof Error ? error.message : 'Unknown error' }),
          { status: 500, headers: { 'Content-Type': 'application/json' } }
        )
      }
    }

    return new Response('Not found', { status: 404 })
  }
}

// ============================================================================
// Worker Entry Point
// ============================================================================

const app = new Hono<{ Bindings: Env }>()

// Root endpoint - API documentation
app.get('/', (c) => {
  return c.json({
    service: 'bench-oltp',
    description: 'OLTP Benchmark Worker - stress test database operations',
    endpoints: {
      'GET /benchmark/oltp/:database/:dataset/:size': 'Run benchmarks (e.g., /benchmark/oltp/db4/ecommerce/100mb)',
      'GET /benchmark/oltp/options': 'List available options',
      'GET /health': 'Health check',
    },
    examples: [
      '/benchmark/oltp/db4/ecommerce/100mb',
      '/benchmark/oltp/sqlite/ecommerce/10mb',
      '/benchmark/oltp/db4-mongo/saas/100mb',
    ],
    databases: ['db4', 'evodb', 'postgres', 'sqlite', 'db4-mongo', 'dotdo-mongodb', 'sdb'],
    datasets: ['ecommerce', 'saas', 'social'],
    sizes: ['1mb', '10mb', '100mb', '1gb'],
  })
})

// Health check
app.get('/health', (c) => {
  return c.json({ status: 'ok', service: 'bench-oltp', timestamp: new Date().toISOString() })
})

// List available options
app.get('/benchmark/oltp/options', (c) => {
  return c.json({
    databases: ['db4', 'evodb', 'postgres', 'sqlite', 'db4-mongo', 'dotdo-mongodb', 'sdb'],
    datasets: ['ecommerce', 'saas', 'social'],
    sizes: ['1mb', '10mb', '100mb', '1gb'],
    operations: [
      'point_lookup',
      'range_scan',
      'single_insert',
      'batch_insert',
      'update',
      'delete',
      'transaction',
      // Stress test operations
      'full_scan',
      'aggregate_count',
      'aggregate_sum',
      'complex_filter',
      'sort_all',
    ],
  })
})

// RESTful GET endpoint: /benchmark/oltp/:database/:dataset/:size
// Example: /benchmark/oltp/db4/ecommerce/100mb
app.get('/benchmark/oltp/:database/:dataset/:size', async (c) => {
  const database = c.req.param('database') as DatabaseType
  const dataset = c.req.param('dataset') as DatasetType
  const size = c.req.param('size') as SizeType

  // Validate inputs
  const validDatabases: DatabaseType[] = ['db4', 'evodb', 'postgres', 'sqlite', 'db4-mongo', 'dotdo-mongodb', 'sdb']
  const validDatasets: DatasetType[] = ['ecommerce', 'saas', 'social']
  const validSizes: SizeType[] = ['1mb', '10mb', '100mb', '1gb']

  if (!validDatabases.includes(database)) {
    return c.json({ error: `Invalid database. Valid options: ${validDatabases.join(', ')}` }, 400)
  }

  if (!validDatasets.includes(dataset)) {
    return c.json({ error: `Invalid dataset. Valid options: ${validDatasets.join(', ')}` }, 400)
  }

  if (!validSizes.includes(size)) {
    return c.json({ error: `Invalid size. Valid options: ${validSizes.join(', ')}` }, 400)
  }

  try {
    // Get the appropriate DO namespace
    const doNamespaceMap: Record<DatabaseType, DurableObjectNamespace<OLTPBenchDO>> = {
      db4: c.env.DB4_DO,
      evodb: c.env.EVODB_DO,
      postgres: c.env.POSTGRES_DO,
      sqlite: c.env.SQLITE_DO,
      'db4-mongo': c.env.DB4_MONGO_DO,
      'dotdo-mongodb': c.env.DOTDO_MONGODB_DO,
      sdb: c.env.SDB_DO,
    }

    const doNamespace = doNamespaceMap[database]
    if (!doNamespace) {
      return c.json({ error: `DO namespace not configured for database: ${database}` }, 500)
    }

    const doId = doNamespace.idFromName(`oltp-${database}-${dataset}-${size}-${Date.now()}`)
    const benchDO = doNamespace.get(doId)

    const benchmarkRequest: BenchmarkRequest = {
      database,
      dataset,
      size,
      // Default to stress test operations for meaningful benchmarks
      operations: ['full_scan', 'aggregate_count', 'aggregate_sum', 'complex_filter', 'sort_all'],
      iterations: 10,
    }

    const doRequest = new Request('http://internal/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(benchmarkRequest),
    })

    const response = await benchDO.fetch(doRequest)
    const results = (await response.json()) as OLTPBenchmarkResults

    // Add colo info
    results.colo = (c.req.raw as { cf?: { colo?: string } }).cf?.colo ?? 'unknown'

    return c.json(results)
  } catch (error) {
    return c.json(
      { error: error instanceof Error ? error.message : 'Unknown error' },
      500
    )
  }
})

// Legacy POST endpoint (for backward compatibility)
app.post('/benchmark/oltp', async (c) => {
  const database = (c.req.query('database') || 'db4') as DatabaseType
  const dataset = (c.req.query('dataset') || 'ecommerce') as DatasetType
  const size = (c.req.query('size') || '10mb') as SizeType

  // Validate inputs
  const validDatabases: DatabaseType[] = ['db4', 'evodb', 'postgres', 'sqlite', 'db4-mongo', 'dotdo-mongodb', 'sdb']
  const validDatasets: DatasetType[] = ['ecommerce', 'saas', 'social']
  const validSizes: SizeType[] = ['1mb', '10mb', '100mb', '1gb']

  if (!validDatabases.includes(database)) {
    return c.json({ error: `Invalid database. Valid options: ${validDatabases.join(', ')}` }, 400)
  }

  if (!validDatasets.includes(dataset)) {
    return c.json({ error: `Invalid dataset. Valid options: ${validDatasets.join(', ')}` }, 400)
  }

  if (!validSizes.includes(size)) {
    return c.json({ error: `Invalid size. Valid options: ${validSizes.join(', ')}` }, 400)
  }

  try {
    // Get the appropriate DO namespace
    const doNamespaceMap: Record<DatabaseType, DurableObjectNamespace<OLTPBenchDO>> = {
      db4: c.env.DB4_DO,
      evodb: c.env.EVODB_DO,
      postgres: c.env.POSTGRES_DO,
      sqlite: c.env.SQLITE_DO,
      'db4-mongo': c.env.DB4_MONGO_DO,
      'dotdo-mongodb': c.env.DOTDO_MONGODB_DO,
      sdb: c.env.SDB_DO,
    }

    const doNamespace = doNamespaceMap[database]
    if (!doNamespace) {
      return c.json({ error: `DO namespace not configured for database: ${database}` }, 500)
    }

    // Create a unique DO instance for this benchmark run
    const doId = doNamespace.idFromName(`oltp-${database}-${dataset}-${size}-${Date.now()}`)
    const benchDO = doNamespace.get(doId)

    // Parse optional request body
    let body: Partial<BenchmarkRequest> = {}
    try {
      body = (await c.req.json()) as Partial<BenchmarkRequest>
    } catch {
      // Empty body is fine
    }

    // Run benchmarks
    const benchmarkRequest: BenchmarkRequest = {
      database,
      dataset,
      size,
      operations: body.operations,
      iterations: body.iterations,
      runId: body.runId,
    }

    const doRequest = new Request('http://internal/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(benchmarkRequest),
    })

    const response = await benchDO.fetch(doRequest)
    const results = (await response.json()) as OLTPBenchmarkResults

    // Add colo information
    const colo = c.req.raw.cf?.colo as string | undefined
    if (colo) {
      results.colo = colo
    }

    // Convert to JSONL format for R2 storage
    const jsonlResults = results.benchmarks.map((b) => {
      const result: BenchmarkResult = {
        benchmark: `oltp/${b.name}`,
        database,
        dataset: `${dataset}-${size}`,
        p50_ms: b.p50Ms,
        p99_ms: b.p99Ms,
        min_ms: b.minMs,
        max_ms: b.maxMs,
        mean_ms: b.meanMs,
        stddev_ms: b.stddevMs,
        ops_per_sec: b.opsPerSec,
        iterations: b.iterations,
        vfs_reads: 0,
        vfs_writes: 0,
        vfs_bytes_read: 0,
        vfs_bytes_written: 0,
        timestamp: results.timestamp,
        environment: results.environment,
        run_id: results.runId,
        colo: results.colo,
        total_duration_ms: b.totalMs,
      }
      return JSON.stringify(result)
    })

    // Store results in R2
    const resultsKey = `oltp/${database}/${dataset}/${size}/${results.runId}.jsonl`
    await c.env.RESULTS.put(resultsKey, jsonlResults.join('\n'))

    return c.json(results, 200, {
      'Content-Type': 'application/json',
      'X-Results-Key': resultsKey,
      'X-Run-Id': results.runId,
    })
  } catch (error) {
    return c.json(
      {
        error: error instanceof Error ? error.message : 'Unknown error',
        stack: error instanceof Error ? error.stack : undefined,
      },
      500
    )
  }
})

// Batch benchmark endpoint - runs all databases for a given dataset/size
app.post('/benchmark/oltp/batch', async (c) => {
  const dataset = (c.req.query('dataset') || 'ecommerce') as DatasetType
  const size = (c.req.query('size') || '10mb') as SizeType
  const databases: DatabaseType[] = ['db4', 'evodb', 'postgres', 'sqlite', 'db4-mongo', 'dotdo-mongodb', 'sdb']

  const results: OLTPBenchmarkResults[] = []
  const errors: Array<{ database: string; error: string }> = []

  for (const database of databases) {
    try {
      // Make internal request to main endpoint
      const response = await app.fetch(
        new Request(`http://localhost/benchmark/oltp?database=${database}&dataset=${dataset}&size=${size}`, {
          method: 'POST',
        }),
        c.env
      )

      if (response.ok) {
        const result = (await response.json()) as OLTPBenchmarkResults
        results.push(result)
      } else {
        const errorBody = await response.json() as { error: string }
        errors.push({ database, error: errorBody.error })
      }
    } catch (error) {
      errors.push({ database, error: error instanceof Error ? error.message : String(error) })
    }
  }

  return c.json({
    dataset,
    size,
    successful: results.length,
    failed: errors.length,
    results,
    errors,
  })
})

// List stored results
app.get('/benchmark/oltp/results', async (c) => {
  const database = c.req.query('database')
  const dataset = c.req.query('dataset')
  const size = c.req.query('size')

  let prefix = 'oltp/'
  if (database) prefix += `${database}/`
  if (dataset) prefix += `${dataset}/`
  if (size) prefix += `${size}/`

  const list = await c.env.RESULTS.list({ prefix, limit: 100 })
  const results = list.objects.map((obj) => ({
    key: obj.key,
    size: obj.size,
    uploaded: obj.uploaded.toISOString(),
  }))

  return c.json({ results, truncated: list.truncated })
})

// Get specific result
app.get('/benchmark/oltp/results/:runId', async (c) => {
  const runId = c.req.param('runId')
  const database = c.req.query('database') || '*'
  const dataset = c.req.query('dataset') || '*'
  const size = c.req.query('size') || '*'

  // Try to find the result file
  let key = `oltp/${database}/${dataset}/${size}/${runId}.jsonl`

  // If wildcards, search for the file
  if (database === '*' || dataset === '*' || size === '*') {
    const list = await c.env.RESULTS.list({ prefix: 'oltp/' })
    const match = list.objects.find((obj) => obj.key.endsWith(`${runId}.jsonl`))
    if (match) {
      key = match.key
    }
  }

  const object = await c.env.RESULTS.get(key)

  if (!object) {
    return c.json({ error: 'Result not found' }, 404)
  }

  return c.text(await object.text(), 200, {
    'Content-Type': 'application/x-ndjson',
  })
})

// Check dataset availability
app.get('/benchmark/oltp/datasets', async (c) => {
  const datasets: Record<DatasetType, Record<SizeType, { exists: boolean; tables: string[] }>> = {
    ecommerce: { '1mb': { exists: false, tables: [] }, '10mb': { exists: false, tables: [] }, '100mb': { exists: false, tables: [] }, '1gb': { exists: false, tables: [] } },
    saas: { '1mb': { exists: false, tables: [] }, '10mb': { exists: false, tables: [] }, '100mb': { exists: false, tables: [] }, '1gb': { exists: false, tables: [] } },
    social: { '1mb': { exists: false, tables: [] }, '10mb': { exists: false, tables: [] }, '100mb': { exists: false, tables: [] }, '1gb': { exists: false, tables: [] } },
  }

  for (const dataset of Object.keys(datasets) as DatasetType[]) {
    for (const size of ['1mb', '10mb', '100mb', '1gb'] as SizeType[]) {
      const prefix = `oltp/${dataset}/${size}/`
      const list = await c.env.DATASETS.list({ prefix })

      if (list.objects.length > 0) {
        datasets[dataset][size].exists = true
        datasets[dataset][size].tables = list.objects.map((obj) =>
          obj.key.split('/').pop()?.replace('.jsonl', '') ?? ''
        )
      }
    }
  }

  return c.json({ datasets })
})

export default app
