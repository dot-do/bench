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
type DatabaseType = 'db4' | 'evodb' | 'postgres' | 'sqlite' | 'db4-mongo' | 'clickhouse-mongo' | 'mergetree-mongo' | 'sdb'

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
  CLICKHOUSE_MONGO_DO: DurableObjectNamespace<OLTPBenchDO>
  MERGETREE_MONGO_DO: DurableObjectNamespace<OLTPBenchDO>
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

/**
 * EvoDB Adapter - In-memory columnar document store simulation
 *
 * Simulates EvoDB's columnar storage with:
 * - Column-oriented data layout (data stored by column, not row)
 * - Schema evolution support
 * - Efficient range scans on columns
 *
 * Note: @evodb/core is available but has bundling issues in Workers.
 * This adapter simulates the columnar approach for benchmarking.
 */
class EvoDBAdapter implements DatabaseAdapter {
  name: DatabaseType = 'evodb'
  // Columnar storage: collection -> column_name -> values[]
  private columns: Map<string, Map<string, unknown[]>> = new Map()
  // Row count per collection
  private rowCounts: Map<string, number> = new Map()
  // ID index: collection -> id -> row_index
  private idIndex: Map<string, Map<string, number>> = new Map()

  async connect(): Promise<void> {
    this.columns.clear()
    this.rowCounts.clear()
    this.idIndex.clear()
  }

  async close(): Promise<void> {
    this.columns.clear()
    this.rowCounts.clear()
    this.idIndex.clear()
  }

  private getColumns(collection: string): Map<string, unknown[]> {
    if (!this.columns.has(collection)) {
      this.columns.set(collection, new Map())
      this.rowCounts.set(collection, 0)
      this.idIndex.set(collection, new Map())
    }
    return this.columns.get(collection)!
  }

  private normalizeId(doc: BenchDoc): string {
    return (doc.id ?? doc._id ?? doc.$id ?? '') as string
  }

  private rowToDoc(collection: string, rowIndex: number): BenchDoc | null {
    const cols = this.getColumns(collection)
    const doc: BenchDoc = {}

    for (const [colName, values] of cols) {
      if (rowIndex < values.length) {
        doc[colName] = values[rowIndex]
      }
    }

    return Object.keys(doc).length > 0 ? doc : null
  }

  async pointLookup(collection: string, id: string): Promise<BenchDoc | null> {
    const index = this.idIndex.get(collection)
    if (!index) return null

    const rowIndex = index.get(id)
    if (rowIndex === undefined) return null

    return this.rowToDoc(collection, rowIndex)
  }

  async rangeScan(collection: string, filter: Record<string, unknown>, limit = 100): Promise<BenchDoc[]> {
    const cols = this.getColumns(collection)
    const rowCount = this.rowCounts.get(collection) ?? 0
    const results: BenchDoc[] = []

    // Columnar scan - check filter columns first
    for (let i = 0; i < rowCount && results.length < limit; i++) {
      let matches = true

      for (const [key, value] of Object.entries(filter)) {
        const column = cols.get(key)
        if (!column || column[i] !== value) {
          matches = false
          break
        }
      }

      if (matches) {
        const doc = this.rowToDoc(collection, i)
        if (doc) results.push(doc)
      }
    }

    return results
  }

  async insert(collection: string, doc: BenchDoc): Promise<void> {
    const cols = this.getColumns(collection)
    const id = this.normalizeId(doc)
    const rowIndex = this.rowCounts.get(collection) ?? 0

    // Add each field as a column value
    for (const [key, value] of Object.entries(doc)) {
      if (!cols.has(key)) {
        // Schema evolution: add new column
        cols.set(key, new Array(rowIndex).fill(null))
      }
      cols.get(key)!.push(value)
    }

    // Ensure all columns have same length (nulls for missing fields)
    for (const [, values] of cols) {
      while (values.length <= rowIndex) {
        values.push(null)
      }
    }

    // Update index
    const index = this.idIndex.get(collection)!
    index.set(id, rowIndex)
    this.rowCounts.set(collection, rowIndex + 1)
  }

  async batchInsert(collection: string, docs: BenchDoc[]): Promise<void> {
    for (const doc of docs) {
      await this.insert(collection, doc)
    }
  }

  async update(collection: string, id: string, updates: Record<string, unknown>): Promise<void> {
    const index = this.idIndex.get(collection)
    if (!index) return

    const rowIndex = index.get(id)
    if (rowIndex === undefined) return

    const cols = this.getColumns(collection)

    for (const [key, value] of Object.entries(updates)) {
      if (!cols.has(key)) {
        // Schema evolution: add new column
        const rowCount = this.rowCounts.get(collection) ?? 0
        cols.set(key, new Array(rowCount).fill(null))
      }
      cols.get(key)![rowIndex] = value
    }
  }

  async delete(collection: string, id: string): Promise<boolean> {
    const index = this.idIndex.get(collection)
    if (!index) return false

    const rowIndex = index.get(id)
    if (rowIndex === undefined) return false

    // Mark as deleted by setting id column to null (tombstone approach)
    const cols = this.getColumns(collection)
    const idCol = cols.get('id') ?? cols.get('_id') ?? cols.get('$id')
    if (idCol) {
      idCol[rowIndex] = null
    }

    index.delete(id)
    return true
  }

  async transaction(fn: () => Promise<void>): Promise<void> {
    // EvoDB uses atomic batch writes, execute serially
    await fn()
  }

  async loadData(collection: string, docs: BenchDoc[]): Promise<number> {
    await this.batchInsert(collection, docs)
    return docs.length
  }

  async getCollectionIds(collection: string): Promise<string[]> {
    const index = this.idIndex.get(collection)
    if (!index) return []

    return Array.from(index.keys()).slice(0, 10000)
  }
}

/**
 * PostgreSQL Adapter - In-memory SQL-like document store
 *
 * Uses a pure JavaScript implementation that simulates PostgreSQL operations.
 * This provides consistent benchmarking without external dependencies.
 *
 * Note: @electric-sql/pglite requires URL resolution that doesn't work in Workers.
 * This in-memory adapter provides equivalent OLTP operations with hash-based indexing.
 *
 * PERFORMANCE FIX: Changed from O(n) sorted array insertion to O(1) Set-based indexing.
 * The previous B-tree simulation used findIndex + splice for each insert, causing
 * O(n^2) complexity on 250k record datasets and CPU timeouts.
 */
class PostgresAdapter implements DatabaseAdapter {
  name: DatabaseType = 'postgres'
  // In-memory tables: collection -> Map<id, row>
  private tables: Map<string, Map<string, BenchDoc>> = new Map()
  // Hash index on status field (O(1) add/remove using Set)
  private statusIndex: Map<string, Map<string, Set<string>>> = new Map()

  async connect(): Promise<void> {
    this.tables.clear()
    this.statusIndex.clear()
  }

  async close(): Promise<void> {
    this.tables.clear()
    this.statusIndex.clear()
  }

  private getTable(collection: string): Map<string, BenchDoc> {
    if (!this.tables.has(collection)) {
      this.tables.set(collection, new Map())
      this.statusIndex.set(collection, new Map())
    }
    return this.tables.get(collection)!
  }

  private getIndex(collection: string): Map<string, Set<string>> {
    if (!this.statusIndex.has(collection)) {
      this.statusIndex.set(collection, new Map())
    }
    return this.statusIndex.get(collection)!
  }

  private normalizeId(doc: BenchDoc): string {
    return (doc.id ?? doc._id ?? doc.$id ?? '') as string
  }

  private indexDoc(collection: string, id: string, doc: BenchDoc): void {
    const index = this.getIndex(collection)
    const status = doc.status as string | undefined
    if (status) {
      if (!index.has(status)) {
        index.set(status, new Set())
      }
      index.get(status)!.add(id)
    }
  }

  private removeFromIndex(collection: string, id: string, doc: BenchDoc): void {
    const index = this.getIndex(collection)
    const status = doc.status as string | undefined
    if (status && index.has(status)) {
      index.get(status)!.delete(id)
    }
  }

  async pointLookup(collection: string, id: string): Promise<BenchDoc | null> {
    const table = this.getTable(collection)
    return table.get(id) ?? null
  }

  async rangeScan(collection: string, filter: Record<string, unknown>, limit = 100): Promise<BenchDoc[]> {
    const table = this.getTable(collection)
    const results: BenchDoc[] = []

    // Use hash index if filtering by status only
    if (filter.status && Object.keys(filter).length === 1) {
      const index = this.getIndex(collection)
      const ids = index.get(filter.status as string)
      if (ids) {
        for (const id of ids) {
          if (results.length >= limit) break
          const doc = table.get(id)
          if (doc) results.push(doc)
        }
      }
      return results
    }

    // Full scan with filter (sequential scan in PostgreSQL terms)
    for (const doc of table.values()) {
      if (results.length >= limit) break

      let matches = true
      for (const [key, value] of Object.entries(filter)) {
        if (doc[key] !== value) {
          matches = false
          break
        }
      }
      if (matches) results.push(doc)
    }
    return results
  }

  async insert(collection: string, doc: BenchDoc): Promise<void> {
    const table = this.getTable(collection)
    const id = this.normalizeId(doc)
    const docWithId = { ...doc, id }
    table.set(id, docWithId)
    this.indexDoc(collection, id, docWithId)
  }

  async batchInsert(collection: string, docs: BenchDoc[]): Promise<void> {
    const table = this.getTable(collection)
    for (const doc of docs) {
      const id = this.normalizeId(doc)
      const docWithId = { ...doc, id }
      table.set(id, docWithId)
      this.indexDoc(collection, id, docWithId)
    }
  }

  async update(collection: string, id: string, updates: Record<string, unknown>): Promise<void> {
    const table = this.getTable(collection)
    const existing = table.get(id)
    if (existing) {
      this.removeFromIndex(collection, id, existing)
      const updated = { ...existing, ...updates }
      table.set(id, updated)
      this.indexDoc(collection, id, updated)
    }
  }

  async delete(collection: string, id: string): Promise<boolean> {
    const table = this.getTable(collection)
    const existing = table.get(id)
    if (existing) {
      this.removeFromIndex(collection, id, existing)
      return table.delete(id)
    }
    return false
  }

  async transaction(fn: () => Promise<void>): Promise<void> {
    // Simple serial execution - simulates PostgreSQL transaction
    await fn()
  }

  async loadData(collection: string, docs: BenchDoc[]): Promise<number> {
    await this.batchInsert(collection, docs)
    return docs.length
  }

  async getCollectionIds(collection: string): Promise<string[]> {
    const table = this.getTable(collection)
    return Array.from(table.keys()).slice(0, 10000)
  }
}

/**
 * SQLite Adapter - In-memory SQL-like document store
 *
 * Uses a pure JavaScript Map-based implementation that simulates SQL operations.
 * This provides consistent benchmarking without external dependencies.
 *
 * Note: @libsql/client requires remote URLs in Workers (not :memory:).
 * This in-memory adapter provides equivalent OLTP operations.
 */
class SQLiteAdapter implements DatabaseAdapter {
  name: DatabaseType = 'sqlite'
  // In-memory tables: collection -> Map<id, row>
  private tables: Map<string, Map<string, BenchDoc>> = new Map()
  // Index on status field for range scans
  private statusIndex: Map<string, Map<string, Set<string>>> = new Map()

  async connect(): Promise<void> {
    this.tables.clear()
    this.statusIndex.clear()
  }

  async close(): Promise<void> {
    this.tables.clear()
    this.statusIndex.clear()
  }

  private getTable(collection: string): Map<string, BenchDoc> {
    if (!this.tables.has(collection)) {
      this.tables.set(collection, new Map())
      this.statusIndex.set(collection, new Map())
    }
    return this.tables.get(collection)!
  }

  private getIndex(collection: string): Map<string, Set<string>> {
    if (!this.statusIndex.has(collection)) {
      this.statusIndex.set(collection, new Map())
    }
    return this.statusIndex.get(collection)!
  }

  private normalizeId(doc: BenchDoc): string {
    return (doc.id ?? doc._id ?? doc.$id ?? '') as string
  }

  private indexDoc(collection: string, id: string, doc: BenchDoc): void {
    const index = this.getIndex(collection)
    const status = doc.status as string | undefined
    if (status) {
      if (!index.has(status)) {
        index.set(status, new Set())
      }
      index.get(status)!.add(id)
    }
  }

  private removeFromIndex(collection: string, id: string, doc: BenchDoc): void {
    const index = this.getIndex(collection)
    const status = doc.status as string | undefined
    if (status && index.has(status)) {
      index.get(status)!.delete(id)
    }
  }

  async pointLookup(collection: string, id: string): Promise<BenchDoc | null> {
    const table = this.getTable(collection)
    return table.get(id) ?? null
  }

  async rangeScan(collection: string, filter: Record<string, unknown>, limit = 100): Promise<BenchDoc[]> {
    const table = this.getTable(collection)
    const results: BenchDoc[] = []

    // Use index if filtering by status
    if (filter.status && Object.keys(filter).length === 1) {
      const index = this.getIndex(collection)
      const ids = index.get(filter.status as string)
      if (ids) {
        for (const id of ids) {
          if (results.length >= limit) break
          const doc = table.get(id)
          if (doc) results.push(doc)
        }
      }
      return results
    }

    // Full scan with filter
    for (const doc of table.values()) {
      if (results.length >= limit) break

      let matches = true
      for (const [key, value] of Object.entries(filter)) {
        if (doc[key] !== value) {
          matches = false
          break
        }
      }
      if (matches) results.push(doc)
    }
    return results
  }

  async insert(collection: string, doc: BenchDoc): Promise<void> {
    const table = this.getTable(collection)
    const id = this.normalizeId(doc)
    const docWithId = { ...doc, id }
    table.set(id, docWithId)
    this.indexDoc(collection, id, docWithId)
  }

  async batchInsert(collection: string, docs: BenchDoc[]): Promise<void> {
    const table = this.getTable(collection)
    for (const doc of docs) {
      const id = this.normalizeId(doc)
      const docWithId = { ...doc, id }
      table.set(id, docWithId)
      this.indexDoc(collection, id, docWithId)
    }
  }

  async update(collection: string, id: string, updates: Record<string, unknown>): Promise<void> {
    const table = this.getTable(collection)
    const existing = table.get(id)
    if (existing) {
      this.removeFromIndex(collection, id, existing)
      const updated = { ...existing, ...updates }
      table.set(id, updated)
      this.indexDoc(collection, id, updated)
    }
  }

  async delete(collection: string, id: string): Promise<boolean> {
    const table = this.getTable(collection)
    const existing = table.get(id)
    if (existing) {
      this.removeFromIndex(collection, id, existing)
      return table.delete(id)
    }
    return false
  }

  async transaction(fn: () => Promise<void>): Promise<void> {
    // Simple serial execution - no rollback support in this simple implementation
    await fn()
  }

  async loadData(collection: string, docs: BenchDoc[]): Promise<number> {
    await this.batchInsert(collection, docs)
    return docs.length
  }

  async getCollectionIds(collection: string): Promise<string[]> {
    const table = this.getTable(collection)
    return Array.from(table.keys()).slice(0, 10000)
  }
}

/**
 * @db4/mongo Adapter - MongoDB API with db4 backend
 * Uses an in-memory MongoDB-compatible implementation.
 *
 * This adapter implements MongoDB-style operations using in-memory Maps,
 * similar to the @db4/mongo package interface but without external dependencies.
 */
class DB4MongoAdapter implements DatabaseAdapter {
  name: DatabaseType = 'db4-mongo'
  // In-memory collections: collection -> Map<_id, doc>
  private collections: Map<string, Map<string, BenchDoc>> = new Map()

  async connect(): Promise<void> {
    this.collections.clear()
  }

  async close(): Promise<void> {
    this.collections.clear()
  }

  private getCollection(name: string): Map<string, BenchDoc> {
    if (!this.collections.has(name)) {
      this.collections.set(name, new Map())
    }
    return this.collections.get(name)!
  }

  private normalizeId(doc: BenchDoc): string {
    return (doc._id ?? doc.id ?? doc.$id ?? '') as string
  }

  async pointLookup(collection: string, id: string): Promise<BenchDoc | null> {
    const col = this.getCollection(collection)
    return col.get(id) ?? null
  }

  async rangeScan(collection: string, filter: Record<string, unknown>, limit = 100): Promise<BenchDoc[]> {
    const col = this.getCollection(collection)
    const results: BenchDoc[] = []

    for (const doc of col.values()) {
      if (results.length >= limit) break

      let matches = true
      for (const [key, value] of Object.entries(filter)) {
        if (doc[key] !== value) {
          matches = false
          break
        }
      }
      if (matches) results.push(doc)
    }
    return results
  }

  async insert(collection: string, doc: BenchDoc): Promise<void> {
    const col = this.getCollection(collection)
    const id = this.normalizeId(doc)
    const mongoDoc = { ...doc, _id: id }
    col.set(id, mongoDoc)
  }

  async batchInsert(collection: string, docs: BenchDoc[]): Promise<void> {
    const col = this.getCollection(collection)
    for (const doc of docs) {
      const id = this.normalizeId(doc)
      const mongoDoc = { ...doc, _id: id }
      col.set(id, mongoDoc)
    }
  }

  async update(collection: string, id: string, updates: Record<string, unknown>): Promise<void> {
    const col = this.getCollection(collection)
    const existing = col.get(id)
    if (existing) {
      col.set(id, { ...existing, ...updates })
    }
  }

  async delete(collection: string, id: string): Promise<boolean> {
    const col = this.getCollection(collection)
    return col.delete(id)
  }

  async transaction(fn: () => Promise<void>): Promise<void> {
    await fn()
  }

  async loadData(collection: string, docs: BenchDoc[]): Promise<number> {
    await this.batchInsert(collection, docs)
    return docs.length
  }

  async getCollectionIds(collection: string): Promise<string[]> {
    const col = this.getCollection(collection)
    return Array.from(col.keys()).slice(0, 10000)
  }
}

/**
 * ClickHouse MongoDB Compat Adapter
 * Uses @dotdo/chdb-mongo-compat for MongoDB-compatible API backed by ClickHouse.
 *
 * NOTE: This adapter requires the @dotdo/chdb-mongo-compat package to be bundled.
 * Currently not available in this worker deployment.
 */
class ClickHouseMongoAdapter implements DatabaseAdapter {
  name: DatabaseType = 'clickhouse-mongo'

  async connect(): Promise<void> {
    throw new Error(
      'clickhouse-mongo adapter requires @dotdo/chdb-mongo-compat package. ' +
      'This package is not bundled in this worker. Use db4-mongo or sqlite instead.'
    )
  }

  async close(): Promise<void> {}
  async pointLookup(_c: string, _id: string): Promise<BenchDoc | null> { throw new Error('Not available') }
  async rangeScan(_c: string, _f: Record<string, unknown>, _l?: number): Promise<BenchDoc[]> { throw new Error('Not available') }
  async insert(_c: string, _d: BenchDoc): Promise<void> { throw new Error('Not available') }
  async batchInsert(_c: string, _d: BenchDoc[]): Promise<void> { throw new Error('Not available') }
  async update(_c: string, _id: string, _u: Record<string, unknown>): Promise<void> { throw new Error('Not available') }
  async delete(_c: string, _id: string): Promise<boolean> { throw new Error('Not available') }
  async transaction(_fn: () => Promise<void>): Promise<void> { throw new Error('Not available') }
  async loadData(_c: string, _d: BenchDoc[]): Promise<number> { throw new Error('Not available') }
  async getCollectionIds(_c: string): Promise<string[]> { throw new Error('Not available') }
}

/**
 * MergeTree MongoDB Query Adapter
 * Uses @anthropic-pocs/iceberg-mongo-query for MongoDB API backed by MergeTree engines.
 *
 * NOTE: This adapter requires the @anthropic-pocs/iceberg-mongo-query package to be bundled.
 * Currently not available in this worker deployment.
 */
class MergeTreeMongoAdapter implements DatabaseAdapter {
  name: DatabaseType = 'mergetree-mongo'

  async connect(): Promise<void> {
    throw new Error(
      'mergetree-mongo adapter requires @anthropic-pocs/iceberg-mongo-query package. ' +
      'This package is not bundled in this worker. Use db4-mongo or sqlite instead.'
    )
  }

  async close(): Promise<void> {}
  async pointLookup(_c: string, _id: string): Promise<BenchDoc | null> { throw new Error('Not available') }
  async rangeScan(_c: string, _f: Record<string, unknown>, _l?: number): Promise<BenchDoc[]> { throw new Error('Not available') }
  async insert(_c: string, _d: BenchDoc): Promise<void> { throw new Error('Not available') }
  async batchInsert(_c: string, _d: BenchDoc[]): Promise<void> { throw new Error('Not available') }
  async update(_c: string, _id: string, _u: Record<string, unknown>): Promise<void> { throw new Error('Not available') }
  async delete(_c: string, _id: string): Promise<boolean> { throw new Error('Not available') }
  async transaction(_fn: () => Promise<void>): Promise<void> { throw new Error('Not available') }
  async loadData(_c: string, _d: BenchDoc[]): Promise<number> { throw new Error('Not available') }
  async getCollectionIds(_c: string): Promise<string[]> { throw new Error('Not available') }
}

/**
 * SDB Adapter - NOT AVAILABLE for in-memory benchmarking
 *
 * SDB (@dotdo/sdb) requires a remote Durable Object connection and does not
 * support in-memory mode. The DB() client function requires:
 * - A schema definition
 * - A URL to connect to an SDB Durable Object endpoint
 *
 * For SDB benchmarks, use the sdb.do service directly with:
 *   const db = DB({ EntityType: { field: 'type', ... } }, { url: 'https://tenant.sdb.do' })
 *
 * This adapter is a placeholder that returns a clear error message.
 */
class SDBAdapter implements DatabaseAdapter {
  name: DatabaseType = 'sdb'

  async connect(): Promise<void> {
    throw new Error(
      'SDB adapter requires a remote Durable Object connection (sdb.do). ' +
      'In-memory benchmarking is not supported. Use the sdb.do service directly.'
    )
  }

  async close(): Promise<void> {}

  async pointLookup(_collection: string, _id: string): Promise<BenchDoc | null> {
    throw new Error('SDB requires remote connection - not available for in-memory benchmarks')
  }

  async rangeScan(_collection: string, _filter: Record<string, unknown>, _limit?: number): Promise<BenchDoc[]> {
    throw new Error('SDB requires remote connection - not available for in-memory benchmarks')
  }

  async insert(_collection: string, _doc: BenchDoc): Promise<void> {
    throw new Error('SDB requires remote connection - not available for in-memory benchmarks')
  }

  async batchInsert(_collection: string, _docs: BenchDoc[]): Promise<void> {
    throw new Error('SDB requires remote connection - not available for in-memory benchmarks')
  }

  async update(_collection: string, _id: string, _updates: Record<string, unknown>): Promise<void> {
    throw new Error('SDB requires remote connection - not available for in-memory benchmarks')
  }

  async delete(_collection: string, _id: string): Promise<boolean> {
    throw new Error('SDB requires remote connection - not available for in-memory benchmarks')
  }

  async transaction(_fn: () => Promise<void>): Promise<void> {
    throw new Error('SDB requires remote connection - not available for in-memory benchmarks')
  }

  async loadData(_collection: string, _docs: BenchDoc[]): Promise<number> {
    throw new Error('SDB requires remote connection - not available for in-memory benchmarks')
  }

  async getCollectionIds(_collection: string): Promise<string[]> {
    throw new Error('SDB requires remote connection - not available for in-memory benchmarks')
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
    case 'clickhouse-mongo':
      return new ClickHouseMongoAdapter()
    case 'mergetree-mongo':
      return new MergeTreeMongoAdapter()
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
    databases: ['db4', 'evodb', 'postgres', 'sqlite', 'db4-mongo', 'clickhouse-mongo', 'mergetree-mongo', 'sdb'],
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
    databases: ['db4', 'evodb', 'postgres', 'sqlite', 'db4-mongo', 'clickhouse-mongo', 'mergetree-mongo', 'sdb'],
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
  const validDatabases: DatabaseType[] = ['db4', 'evodb', 'postgres', 'sqlite', 'db4-mongo', 'clickhouse-mongo', 'mergetree-mongo', 'sdb']
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
      'clickhouse-mongo': c.env.CLICKHOUSE_MONGO_DO,
      'mergetree-mongo': c.env.MERGETREE_MONGO_DO,
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
  const validDatabases: DatabaseType[] = ['db4', 'evodb', 'postgres', 'sqlite', 'db4-mongo', 'clickhouse-mongo', 'mergetree-mongo', 'sdb']
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
      'clickhouse-mongo': c.env.CLICKHOUSE_MONGO_DO,
      'mergetree-mongo': c.env.MERGETREE_MONGO_DO,
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
  const databases: DatabaseType[] = ['db4', 'evodb', 'postgres', 'sqlite', 'db4-mongo', 'clickhouse-mongo', 'mergetree-mongo', 'sdb']

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
