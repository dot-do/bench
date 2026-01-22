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

// Generic document type for benchmarks
interface Document {
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
function parseJSONL(content: string): Document[] {
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
  pointLookup(collection: string, id: string): Promise<Document | null>
  rangeScan(collection: string, filter: Record<string, unknown>, limit?: number): Promise<Document[]>
  insert(collection: string, doc: Document): Promise<void>
  batchInsert(collection: string, docs: Document[]): Promise<void>
  update(collection: string, id: string, updates: Record<string, unknown>): Promise<void>
  delete(collection: string, id: string): Promise<boolean>
  transaction(fn: () => Promise<void>): Promise<void>

  // Data loading
  loadData(collection: string, docs: Document[]): Promise<number>
  getCollectionIds(collection: string): Promise<string[]>
}

// ============================================================================
// Database Adapters
// ============================================================================

/**
 * DB4 Adapter - Pure TypeScript document store
 * Uses @db4/client for the client SDK
 */
class DB4Adapter implements DatabaseAdapter {
  name: DatabaseType = 'db4'
  private client: Awaited<ReturnType<typeof import('@db4/client').createClient>> | null = null
  private collections: Map<string, ReturnType<typeof this.client.collection>> = new Map()

  async connect(): Promise<void> {
    const { createClient } = await import('@db4/client')
    // Create in-memory client for benchmarking
    this.client = createClient({ mode: 'memory' })
  }

  async close(): Promise<void> {
    this.collections.clear()
    this.client = null
  }

  private getCollection(name: string) {
    if (!this.collections.has(name)) {
      this.collections.set(name, this.client!.collection(name))
    }
    return this.collections.get(name)!
  }

  private normalizeId(doc: Document): string {
    return (doc.id ?? doc._id ?? doc.$id ?? '') as string
  }

  async pointLookup(collection: string, id: string): Promise<Document | null> {
    const col = this.getCollection(collection)
    const result = await col.findOne({ id })
    return result as Document | null
  }

  async rangeScan(collection: string, filter: Record<string, unknown>, limit = 100): Promise<Document[]> {
    const col = this.getCollection(collection)
    const results = await col.find(filter, { limit })
    return results as Document[]
  }

  async insert(collection: string, doc: Document): Promise<void> {
    const col = this.getCollection(collection)
    await col.create(doc)
  }

  async batchInsert(collection: string, docs: Document[]): Promise<void> {
    const col = this.getCollection(collection)
    await col.createMany(docs)
  }

  async update(collection: string, id: string, updates: Record<string, unknown>): Promise<void> {
    const col = this.getCollection(collection)
    await col.update({ id }, updates)
  }

  async delete(collection: string, id: string): Promise<boolean> {
    const col = this.getCollection(collection)
    const result = await col.delete({ id })
    return result.deleted > 0
  }

  async transaction(fn: () => Promise<void>): Promise<void> {
    // DB4 client doesn't expose transactions directly, execute serially
    await fn()
  }

  async loadData(collection: string, docs: Document[]): Promise<number> {
    await this.batchInsert(collection, docs)
    return docs.length
  }

  async getCollectionIds(collection: string): Promise<string[]> {
    const col = this.getCollection(collection)
    const docs = await col.find({}, { limit: 10000, projection: { id: 1 } })
    return docs.map((d: Document) => d.id ?? '') as string[]
  }
}

/**
 * EvoDB Adapter - Event-sourced columnar document store
 * Uses evodb package for columnar JSON storage
 */
class EvoDBAdapter implements DatabaseAdapter {
  name: DatabaseType = 'evodb'
  private db: InstanceType<typeof import('evodb').EvoDB> | null = null

  async connect(): Promise<void> {
    const { EvoDB } = await import('evodb')
    // Create in-memory EvoDB instance for benchmarking
    this.db = new EvoDB({ storage: null }) // null storage = in-memory
  }

  async close(): Promise<void> {
    this.db = null
  }

  private normalizeId(doc: Document): string {
    return (doc.id ?? doc._id ?? doc.$id ?? '') as string
  }

  async pointLookup(collection: string, id: string): Promise<Document | null> {
    const results = await this.db!.query(collection).where('id', '=', id).limit(1).execute()
    return (results.data[0] as Document) ?? null
  }

  async rangeScan(collection: string, filter: Record<string, unknown>, limit = 100): Promise<Document[]> {
    let query = this.db!.query(collection)
    for (const [key, value] of Object.entries(filter)) {
      query = query.where(key, '=', value)
    }
    const results = await query.limit(limit).execute()
    return results.data as Document[]
  }

  async insert(collection: string, doc: Document): Promise<void> {
    await this.db!.insert(collection, [doc])
  }

  async batchInsert(collection: string, docs: Document[]): Promise<void> {
    await this.db!.insert(collection, docs)
  }

  async update(collection: string, id: string, updates: Record<string, unknown>): Promise<void> {
    await this.db!.update(collection, { id }, updates)
  }

  async delete(collection: string, id: string): Promise<boolean> {
    const result = await this.db!.delete(collection, { id })
    return result.deleted > 0
  }

  async transaction(fn: () => Promise<void>): Promise<void> {
    // EvoDB is event-sourced, execute atomically
    await fn()
  }

  async loadData(collection: string, docs: Document[]): Promise<number> {
    await this.batchInsert(collection, docs)
    return docs.length
  }

  async getCollectionIds(collection: string): Promise<string[]> {
    const results = await this.db!.query(collection).select(['id']).limit(10000).execute()
    return results.data.map((d: Record<string, unknown>) => (d.id ?? '') as string)
  }
}

/**
 * PostgreSQL Adapter - Uses @dotdo/sqlite client (Turso-compatible)
 * Note: For OLTP benchmarks in Workers, we use the SQLite client
 * which provides PostgreSQL-like API via Hrana protocol
 */
class PostgresAdapter implements DatabaseAdapter {
  name: DatabaseType = 'postgres'
  private client: Awaited<ReturnType<typeof import('@dotdo/sqlite').createClient>> | null = null
  private tableSchemas: Map<string, string> = new Map()

  async connect(): Promise<void> {
    const { createClient } = await import('@dotdo/sqlite')
    // Create in-memory SQLite client for benchmarking
    // In production, this would connect to a real PostgresDO
    this.client = createClient({ url: ':memory:' })
  }

  async close(): Promise<void> {
    this.client?.close()
    this.client = null
  }

  private normalizeId(doc: Document): string {
    return (doc.id ?? doc._id ?? doc.$id ?? '') as string
  }

  private async ensureTable(collection: string): Promise<void> {
    if (this.tableSchemas.has(collection)) return

    // Create a simple JSON-based table
    await this.client!.execute(`
      CREATE TABLE IF NOT EXISTS ${collection} (
        id TEXT PRIMARY KEY,
        data TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now'))
      )
    `)
    this.tableSchemas.set(collection, 'created')
  }

  async pointLookup(collection: string, id: string): Promise<Document | null> {
    const result = await this.client!.execute({
      sql: `SELECT data FROM ${collection} WHERE id = ?`,
      args: [id]
    })
    if (result.rows.length === 0) return null
    return JSON.parse(result.rows[0].data as string)
  }

  async rangeScan(collection: string, filter: Record<string, unknown>, limit = 100): Promise<Document[]> {
    // Use JSON extraction for filtering
    const conditions: string[] = []
    const params: (string | number)[] = []

    for (const [key, value] of Object.entries(filter)) {
      conditions.push(`json_extract(data, '$.${key}') = ?`)
      params.push(String(value))
    }

    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : ''
    const result = await this.client!.execute({
      sql: `SELECT data FROM ${collection} ${whereClause} LIMIT ?`,
      args: [...params, limit]
    })
    return result.rows.map((r) => JSON.parse(r.data as string))
  }

  async insert(collection: string, doc: Document): Promise<void> {
    await this.ensureTable(collection)
    const id = this.normalizeId(doc)
    await this.client!.execute({
      sql: `INSERT OR IGNORE INTO ${collection} (id, data) VALUES (?, ?)`,
      args: [id, JSON.stringify(doc)]
    })
  }

  async batchInsert(collection: string, docs: Document[]): Promise<void> {
    if (docs.length === 0) return
    await this.ensureTable(collection)

    const statements = docs.map((doc) => {
      const id = this.normalizeId(doc)
      return {
        sql: `INSERT OR IGNORE INTO ${collection} (id, data) VALUES (?, ?)`,
        args: [id, JSON.stringify(doc)]
      }
    })
    await this.client!.batch(statements)
  }

  async update(collection: string, id: string, updates: Record<string, unknown>): Promise<void> {
    const existing = await this.pointLookup(collection, id)
    if (existing) {
      const updated = { ...existing, ...updates }
      await this.client!.execute({
        sql: `UPDATE ${collection} SET data = ? WHERE id = ?`,
        args: [JSON.stringify(updated), id]
      })
    }
  }

  async delete(collection: string, id: string): Promise<boolean> {
    const result = await this.client!.execute({
      sql: `DELETE FROM ${collection} WHERE id = ?`,
      args: [id]
    })
    return result.rowsAffected > 0
  }

  async transaction(fn: () => Promise<void>): Promise<void> {
    const tx = await this.client!.transaction()
    try {
      await fn()
      await tx.commit()
    } catch (e) {
      await tx.rollback()
      throw e
    }
  }

  async loadData(collection: string, docs: Document[]): Promise<number> {
    await this.batchInsert(collection, docs)
    return docs.length
  }

  async getCollectionIds(collection: string): Promise<string[]> {
    const result = await this.client!.execute({
      sql: `SELECT id FROM ${collection} LIMIT 10000`,
      args: []
    })
    return result.rows.map((r) => r.id as string)
  }
}

/**
 * SQLite Adapter - Uses @dotdo/sqlite (Turso WASM SQLite)
 */
class SQLiteAdapter implements DatabaseAdapter {
  name: DatabaseType = 'sqlite'
  private client: Awaited<ReturnType<typeof import('@dotdo/sqlite').createClient>> | null = null
  private tableSchemas: Map<string, string> = new Map()

  async connect(): Promise<void> {
    const { createClient } = await import('@dotdo/sqlite')
    // Create in-memory SQLite client for benchmarking
    this.client = createClient({ url: ':memory:' })
  }

  async close(): Promise<void> {
    this.client?.close()
    this.client = null
  }

  private normalizeId(doc: Document): string {
    return (doc.id ?? doc._id ?? doc.$id ?? '') as string
  }

  private async ensureTable(collection: string): Promise<void> {
    if (this.tableSchemas.has(collection)) return

    await this.client!.execute(`
      CREATE TABLE IF NOT EXISTS ${collection} (
        id TEXT PRIMARY KEY,
        data TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now'))
      )
    `)
    this.tableSchemas.set(collection, 'created')
  }

  async pointLookup(collection: string, id: string): Promise<Document | null> {
    const result = await this.client!.execute({
      sql: `SELECT data FROM ${collection} WHERE id = ?`,
      args: [id]
    })
    if (result.rows.length === 0) return null
    return JSON.parse(result.rows[0].data as string)
  }

  async rangeScan(collection: string, filter: Record<string, unknown>, limit = 100): Promise<Document[]> {
    // Use json_extract for JSON path queries
    const conditions: string[] = []
    const params: (string | number)[] = []

    for (const [key, value] of Object.entries(filter)) {
      conditions.push(`json_extract(data, '$.${key}') = ?`)
      params.push(String(value))
    }

    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : ''
    const result = await this.client!.execute({
      sql: `SELECT data FROM ${collection} ${whereClause} LIMIT ?`,
      args: [...params, limit]
    })
    return result.rows.map((r) => JSON.parse(r.data as string))
  }

  async insert(collection: string, doc: Document): Promise<void> {
    await this.ensureTable(collection)
    const id = this.normalizeId(doc)
    await this.client!.execute({
      sql: `INSERT OR IGNORE INTO ${collection} (id, data) VALUES (?, ?)`,
      args: [id, JSON.stringify(doc)]
    })
  }

  async batchInsert(collection: string, docs: Document[]): Promise<void> {
    if (docs.length === 0) return
    await this.ensureTable(collection)

    const statements = docs.map((doc) => {
      const id = this.normalizeId(doc)
      return {
        sql: `INSERT OR IGNORE INTO ${collection} (id, data) VALUES (?, ?)`,
        args: [id, JSON.stringify(doc)]
      }
    })
    await this.client!.batch(statements)
  }

  async update(collection: string, id: string, updates: Record<string, unknown>): Promise<void> {
    const existing = await this.pointLookup(collection, id)
    if (existing) {
      const updated = { ...existing, ...updates }
      await this.client!.execute({
        sql: `UPDATE ${collection} SET data = ? WHERE id = ?`,
        args: [JSON.stringify(updated), id]
      })
    }
  }

  async delete(collection: string, id: string): Promise<boolean> {
    const result = await this.client!.execute({
      sql: `DELETE FROM ${collection} WHERE id = ?`,
      args: [id]
    })
    return result.rowsAffected > 0
  }

  async transaction(fn: () => Promise<void>): Promise<void> {
    const tx = await this.client!.transaction()
    try {
      await fn()
      await tx.commit()
    } catch (e) {
      await tx.rollback()
      throw e
    }
  }

  async loadData(collection: string, docs: Document[]): Promise<number> {
    await this.batchInsert(collection, docs)
    return docs.length
  }

  async getCollectionIds(collection: string): Promise<string[]> {
    const result = await this.client!.execute({
      sql: `SELECT id FROM ${collection} LIMIT 10000`,
      args: []
    })
    return result.rows.map((r) => r.id as string)
  }
}

/**
 * @db4/mongo Adapter - MongoDB API with db4 backend
 * Uses @db4/client with MongoDB-style API compatibility
 */
class DB4MongoAdapter implements DatabaseAdapter {
  name: DatabaseType = 'db4-mongo'
  private client: Awaited<ReturnType<typeof import('@db4/client').createClient>> | null = null
  private collections: Map<string, ReturnType<typeof this.client.collection>> = new Map()

  async connect(): Promise<void> {
    const { createClient } = await import('@db4/client')
    // Create in-memory client for benchmarking
    this.client = createClient({ mode: 'memory' })
  }

  async close(): Promise<void> {
    this.collections.clear()
    this.client = null
  }

  private getCollection(name: string) {
    if (!this.collections.has(name)) {
      this.collections.set(name, this.client!.collection(name))
    }
    return this.collections.get(name)!
  }

  private normalizeId(doc: Document): string {
    return (doc._id ?? doc.id ?? doc.$id ?? '') as string
  }

  async pointLookup(collection: string, id: string): Promise<Document | null> {
    const col = this.getCollection(collection)
    const result = await col.findOne({ _id: id })
    return result as Document | null
  }

  async rangeScan(collection: string, filter: Record<string, unknown>, limit = 100): Promise<Document[]> {
    const col = this.getCollection(collection)
    const results = await col.find(filter, { limit })
    return results as Document[]
  }

  async insert(collection: string, doc: Document): Promise<void> {
    const col = this.getCollection(collection)
    const mongoDoc = { ...doc, _id: this.normalizeId(doc) }
    await col.create(mongoDoc)
  }

  async batchInsert(collection: string, docs: Document[]): Promise<void> {
    const col = this.getCollection(collection)
    const mongoDocs = docs.map((doc) => ({
      ...doc,
      _id: this.normalizeId(doc)
    }))
    await col.createMany(mongoDocs)
  }

  async update(collection: string, id: string, updates: Record<string, unknown>): Promise<void> {
    const col = this.getCollection(collection)
    await col.update({ _id: id }, updates)
  }

  async delete(collection: string, id: string): Promise<boolean> {
    const col = this.getCollection(collection)
    const result = await col.delete({ _id: id })
    return result.deleted > 0
  }

  async transaction(fn: () => Promise<void>): Promise<void> {
    // db4/mongo doesn't have native transactions
    await fn()
  }

  async loadData(collection: string, docs: Document[]): Promise<number> {
    await this.batchInsert(collection, docs)
    return docs.length
  }

  async getCollectionIds(collection: string): Promise<string[]> {
    const col = this.getCollection(collection)
    const docs = await col.find({}, { limit: 10000, projection: { _id: 1 } })
    return docs.map((d: Document) => (d._id ?? '') as string)
  }
}

/**
 * @dotdo/mongodb Adapter - MongoDB compatibility layer
 * Uses @dotdo/mongodb which provides MongoDB-style API
 */
class DotDoMongoDBAdapter implements DatabaseAdapter {
  name: DatabaseType = 'dotdo-mongodb'
  private client: InstanceType<typeof import('@dotdo/mongodb').MongoClient> | null = null
  private db: ReturnType<typeof this.client.db> | null = null

  async connect(): Promise<void> {
    const { MongoClient } = await import('@dotdo/mongodb')
    // Create in-memory MongoDB-compatible client for benchmarking
    this.client = new MongoClient('memory://')
    await this.client.connect()
    this.db = this.client.db('benchmark')
  }

  async close(): Promise<void> {
    await this.client?.close()
    this.client = null
    this.db = null
  }

  private normalizeId(doc: Document): string {
    return (doc._id ?? doc.id ?? doc.$id ?? '') as string
  }

  async pointLookup(collection: string, id: string): Promise<Document | null> {
    const result = await this.db!.collection(collection).findOne({ _id: id })
    return result as Document | null
  }

  async rangeScan(collection: string, filter: Record<string, unknown>, limit = 100): Promise<Document[]> {
    const cursor = this.db!.collection(collection).find(filter).limit(limit)
    return (await cursor.toArray()) as Document[]
  }

  async insert(collection: string, doc: Document): Promise<void> {
    const mongoDoc = { ...doc, _id: this.normalizeId(doc) }
    delete mongoDoc.id
    delete mongoDoc.$id
    await this.db!.collection(collection).insertOne(mongoDoc)
  }

  async batchInsert(collection: string, docs: Document[]): Promise<void> {
    const mongoDocs = docs.map((doc) => {
      const mongoDoc = { ...doc, _id: this.normalizeId(doc) }
      delete mongoDoc.id
      delete mongoDoc.$id
      return mongoDoc
    })
    await this.db!.collection(collection).insertMany(mongoDocs)
  }

  async update(collection: string, id: string, updates: Record<string, unknown>): Promise<void> {
    await this.db!.collection(collection).updateOne({ _id: id }, { $set: updates })
  }

  async delete(collection: string, id: string): Promise<boolean> {
    const result = await this.db!.collection(collection).deleteOne({ _id: id })
    return (result.deletedCount ?? 0) > 0
  }

  async transaction(fn: () => Promise<void>): Promise<void> {
    // @dotdo/mongodb supports transactions
    await fn()
  }

  async loadData(collection: string, docs: Document[]): Promise<number> {
    await this.batchInsert(collection, docs)
    return docs.length
  }

  async getCollectionIds(collection: string): Promise<string[]> {
    const docs = await this.db!.collection(collection).find({}).limit(10000).toArray()
    return docs.map((d: Document) => (d._id ?? '') as string)
  }
}

/**
 * SDB Adapter - Document/graph database
 * Uses @dotdo/sdb for document and graph database operations
 */
class SDBAdapter implements DatabaseAdapter {
  name: DatabaseType = 'sdb'
  private db: ReturnType<typeof import('@dotdo/sdb').DB> | null = null

  async connect(): Promise<void> {
    const { DB } = await import('@dotdo/sdb')
    // Create in-memory SDB instance for benchmarking
    this.db = DB({ url: 'memory://' })
  }

  async close(): Promise<void> {
    this.db = null
  }

  private normalizeId(doc: Document): string {
    return (doc.$id ?? doc.id ?? doc._id ?? '') as string
  }

  async pointLookup(collection: string, id: string): Promise<Document | null> {
    try {
      const result = await this.db![collection][id]
      return result as Document | null
    } catch {
      return null
    }
  }

  async rangeScan(collection: string, filter: Record<string, unknown>, limit = 100): Promise<Document[]> {
    const result = await this.db![collection].list({ where: filter, limit })
    return result as Document[]
  }

  async insert(collection: string, doc: Document): Promise<void> {
    const id = this.normalizeId(doc)
    const sdbDoc = { ...doc, $id: id }
    await this.db![collection].create(sdbDoc)
  }

  async batchInsert(collection: string, docs: Document[]): Promise<void> {
    for (const doc of docs) {
      await this.insert(collection, doc)
    }
  }

  async update(collection: string, id: string, updates: Record<string, unknown>): Promise<void> {
    await this.db![collection][id].update(updates)
  }

  async delete(collection: string, id: string): Promise<boolean> {
    try {
      await this.db![collection][id].delete()
      return true
    } catch {
      return false
    }
  }

  async transaction(fn: () => Promise<void>): Promise<void> {
    // SDB uses batch operations
    await fn()
  }

  async loadData(collection: string, docs: Document[]): Promise<number> {
    await this.batchInsert(collection, docs)
    return docs.length
  }

  async getCollectionIds(collection: string): Promise<string[]> {
    const result = await this.db![collection].list({ limit: 10000 })
    return (result as Document[]).map((d) => (d.$id ?? d.id ?? '') as string)
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
  private adapter: DatabaseAdapter | null = null
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

    // Benchmark
    for (let i = 0; i < iterations; i++) {
      const start = performance.now()
      await this.executeOperation(operation, collection, ids)
      times.push(performance.now() - start)
    }

    const stats = calculateStats(times)
    const effectiveIterations = times.length

    return {
      name: operation,
      iterations: effectiveIterations,
      ...stats,
      opsPerSec: effectiveIterations / (stats.totalMs / 1000),
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
    ],
  })
})

// Main benchmark endpoint
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
