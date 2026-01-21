import { bench, describe, beforeAll, afterEach } from 'vitest'
import { VFSInstrument, createMockStorage, createMockSqlStorage } from '../instrumentation/vfs-metrics'
import { StorageMetricsAggregator } from '../instrumentation/cloudflare-analytics'

/**
 * VFS Cost Benchmarks
 *
 * Compare actual blob operations across databases for the same logical operations.
 * Tests whether 2MB optimization is equally effective for all databases.
 *
 * Key questions to answer:
 * 1. Do all databases actually achieve similar blob efficiency?
 * 2. What's the write amplification for updates vs inserts?
 * 3. How do partial page writes affect blob rewrite frequency?
 *
 * Cloudflare DO Pricing:
 * - Row read:  $0.001 per 1M rows
 * - Row write: $1.00 per 1M rows
 *
 * The "2MB optimization" exploits per-row pricing:
 * - 1 row of 1KB = same cost as 1 row of 2MB
 * - Pack data into 2MB blobs for 2000x efficiency
 */

// =============================================================================
// Test Data
// =============================================================================

interface Thing {
  $id: string
  $type: string
  name: string
  status: string
  createdAt: string
  data?: Record<string, unknown>
}

function generateThing(id: string): Thing {
  return {
    $id: id,
    $type: 'Customer',
    name: `Customer ${id}`,
    status: 'active',
    createdAt: new Date().toISOString(),
    data: { tier: 'premium', region: 'us-west' }
  }
}

function generateThings(count: number): Thing[] {
  return Array.from({ length: count }, (_, i) =>
    generateThing(`thing-${String(i).padStart(5, '0')}`)
  )
}

// Size of a typical Thing in bytes (for amplification calculation)
const THING_SIZE_BYTES = 200

// =============================================================================
// Mock Database Adapters with VFS Instrumentation
// =============================================================================

/**
 * Mock DB4 adapter - pure TypeScript columnar store
 * Uses 2MB blob packing strategy
 */
class MockDB4Store {
  private instrument: VFSInstrument
  private storage: ReturnType<typeof createMockStorage>
  private data = new Map<string, Thing>()
  private blockSize = 2 * 1024 * 1024 // 2MB blocks

  constructor(instrument: VFSInstrument) {
    this.instrument = instrument
    this.storage = this.instrument.wrapStorage(createMockStorage())
  }

  async insert(things: Thing[]): Promise<void> {
    // DB4 packs multiple rows into 2MB columnar blocks
    const blockData = JSON.stringify(things)
    const logicalBytes = things.length * THING_SIZE_BYTES

    this.instrument.recordLogicalWrite(logicalBytes)

    // Simulate writing packed blob
    await this.storage.put('block:0', blockData)

    for (const thing of things) {
      this.data.set(thing.$id, thing)
    }
  }

  async get(id: string): Promise<Thing | undefined> {
    const thing = this.data.get(id)

    if (thing) {
      this.instrument.recordLogicalRead(THING_SIZE_BYTES)
      // DB4 would read the containing block
      await this.storage.get('block:0')
    }

    return thing
  }

  async update(id: string, updates: Partial<Thing>): Promise<void> {
    const existing = this.data.get(id)
    if (!existing) return

    this.instrument.recordLogicalWrite(THING_SIZE_BYTES)

    // Update in memory
    Object.assign(existing, updates)

    // Must rewrite the entire block (write amplification!)
    const allThings = Array.from(this.data.values())
    await this.storage.put('block:0', JSON.stringify(allThings))
  }

  preload(things: Thing[]): void {
    for (const thing of things) {
      this.data.set(thing.$id, thing)
    }
  }

  getInstrument(): VFSInstrument {
    return this.instrument
  }
}

/**
 * Mock EvoDB adapter - columnar shredding
 * Uses 2MB blocks with column-oriented storage
 */
class MockEvoDBStore {
  private instrument: VFSInstrument
  private storage: ReturnType<typeof createMockStorage>
  private data = new Map<string, Thing>()

  constructor(instrument: VFSInstrument) {
    this.instrument = instrument
    this.storage = this.instrument.wrapStorage(createMockStorage())
  }

  async insert(things: Thing[]): Promise<void> {
    const logicalBytes = things.length * THING_SIZE_BYTES
    this.instrument.recordLogicalWrite(logicalBytes)

    // EvoDB shreds columns - one blob per column
    const columns = {
      '$id': things.map(t => t.$id),
      '$type': things.map(t => t.$type),
      'name': things.map(t => t.name),
      'status': things.map(t => t.status),
      'createdAt': things.map(t => t.createdAt),
      'data': things.map(t => t.data),
    }

    // Write each column as a separate blob
    for (const [col, values] of Object.entries(columns)) {
      await this.storage.put(`col:${col}`, JSON.stringify(values))
    }

    for (const thing of things) {
      this.data.set(thing.$id, thing)
    }
  }

  async get(id: string): Promise<Thing | undefined> {
    this.instrument.recordLogicalRead(THING_SIZE_BYTES)

    // EvoDB reads only needed columns
    // For point lookup, still need all columns
    await this.storage.get('col:$id')

    return this.data.get(id)
  }

  async update(id: string, updates: Partial<Thing>): Promise<void> {
    const existing = this.data.get(id)
    if (!existing) return

    this.instrument.recordLogicalWrite(THING_SIZE_BYTES)

    // Only rewrite affected columns
    for (const key of Object.keys(updates)) {
      const allValues = Array.from(this.data.values()).map(t => (t as unknown as Record<string, unknown>)[key])
      await this.storage.put(`col:${key}`, JSON.stringify(allValues))
    }

    Object.assign(existing, updates)
  }

  preload(things: Thing[]): void {
    for (const thing of things) {
      this.data.set(thing.$id, thing)
    }
  }

  getInstrument(): VFSInstrument {
    return this.instrument
  }
}

/**
 * Mock PostgreSQL adapter - PGLite with VFS
 * Uses 8KB pages, packed into 2MB blobs (512 pages per blob)
 */
class MockPostgresStore {
  private instrument: VFSInstrument
  private storage: ReturnType<typeof createMockStorage>
  private sql: ReturnType<typeof createMockSqlStorage>
  private data = new Map<string, Thing>()
  private pageSize = 8 * 1024 // 8KB pages
  private blobSize = 2 * 1024 * 1024 // 2MB blobs

  constructor(instrument: VFSInstrument) {
    this.instrument = instrument
    this.storage = this.instrument.wrapStorage(createMockStorage())
    this.sql = this.instrument.wrapSql(createMockSqlStorage())
  }

  async insert(things: Thing[]): Promise<void> {
    const logicalBytes = things.length * THING_SIZE_BYTES
    this.instrument.recordLogicalWrite(logicalBytes)

    // Postgres writes to pages, VFS packs pages into blobs
    // Estimate: each thing spans ~1 page (with overhead)
    const pagesNeeded = Math.ceil(things.length * THING_SIZE_BYTES / this.pageSize)
    const blobsNeeded = Math.ceil(pagesNeeded * this.pageSize / this.blobSize)

    // Write blobs
    for (let i = 0; i < blobsNeeded; i++) {
      await this.storage.put(`blob:${i}`, new ArrayBuffer(this.blobSize))
    }

    // SQL layer also tracks rows
    for (const thing of things) {
      this.sql.exec('INSERT INTO things VALUES (?)', thing)
      this.data.set(thing.$id, thing)
    }
  }

  async get(id: string): Promise<Thing | undefined> {
    this.instrument.recordLogicalRead(THING_SIZE_BYTES)

    // Point lookup reads one page (but VFS loads entire blob)
    await this.storage.get('blob:0')

    this.sql.exec('SELECT * FROM things WHERE id = ?', id)

    return this.data.get(id)
  }

  async update(id: string, updates: Partial<Thing>): Promise<void> {
    const existing = this.data.get(id)
    if (!existing) return

    this.instrument.recordLogicalWrite(THING_SIZE_BYTES)

    // Update requires page rewrite -> blob rewrite
    await this.storage.put('blob:0', new ArrayBuffer(this.blobSize))

    this.sql.exec('UPDATE things SET data = ? WHERE id = ?', updates, id)

    Object.assign(existing, updates)
  }

  preload(things: Thing[]): void {
    for (const thing of things) {
      this.data.set(thing.$id, thing)
    }
  }

  getInstrument(): VFSInstrument {
    return this.instrument
  }
}

/**
 * Mock SQLite adapter - libsql with VFS
 * Uses 4KB pages, packed into 2MB blobs (512 pages per blob)
 */
class MockSQLiteStore {
  private instrument: VFSInstrument
  private storage: ReturnType<typeof createMockStorage>
  private sql: ReturnType<typeof createMockSqlStorage>
  private data = new Map<string, Thing>()
  private pageSize = 4 * 1024 // 4KB pages
  private blobSize = 2 * 1024 * 1024 // 2MB blobs

  constructor(instrument: VFSInstrument) {
    this.instrument = instrument
    this.storage = this.instrument.wrapStorage(createMockStorage())
    this.sql = this.instrument.wrapSql(createMockSqlStorage())
  }

  async insert(things: Thing[]): Promise<void> {
    const logicalBytes = things.length * THING_SIZE_BYTES
    this.instrument.recordLogicalWrite(logicalBytes)

    // SQLite uses 4KB pages
    const pagesNeeded = Math.ceil(things.length * THING_SIZE_BYTES / this.pageSize)
    const blobsNeeded = Math.ceil(pagesNeeded * this.pageSize / this.blobSize)

    for (let i = 0; i < blobsNeeded; i++) {
      await this.storage.put(`blob:${i}`, new ArrayBuffer(this.blobSize))
    }

    for (const thing of things) {
      this.sql.exec('INSERT INTO things VALUES (?)', thing)
      this.data.set(thing.$id, thing)
    }
  }

  async get(id: string): Promise<Thing | undefined> {
    this.instrument.recordLogicalRead(THING_SIZE_BYTES)

    await this.storage.get('blob:0')
    this.sql.exec('SELECT * FROM things WHERE id = ?', id)

    return this.data.get(id)
  }

  async update(id: string, updates: Partial<Thing>): Promise<void> {
    const existing = this.data.get(id)
    if (!existing) return

    this.instrument.recordLogicalWrite(THING_SIZE_BYTES)

    await this.storage.put('blob:0', new ArrayBuffer(this.blobSize))
    this.sql.exec('UPDATE things SET data = ? WHERE id = ?', updates, id)

    Object.assign(existing, updates)
  }

  preload(things: Thing[]): void {
    for (const thing of things) {
      this.data.set(thing.$id, thing)
    }
  }

  getInstrument(): VFSInstrument {
    return this.instrument
  }
}

/**
 * Mock DuckDB adapter - buffer registration
 * Uses columnar format with 2MB buffer registration
 */
class MockDuckDBStore {
  private instrument: VFSInstrument
  private storage: ReturnType<typeof createMockStorage>
  private data = new Map<string, Thing>()
  private bufferSize = 2 * 1024 * 1024 // 2MB buffers

  constructor(instrument: VFSInstrument) {
    this.instrument = instrument
    this.storage = this.instrument.wrapStorage(createMockStorage())
  }

  async insert(things: Thing[]): Promise<void> {
    const logicalBytes = things.length * THING_SIZE_BYTES
    this.instrument.recordLogicalWrite(logicalBytes)

    // DuckDB uses columnar buffers
    // Very efficient for bulk inserts
    const buffersNeeded = Math.ceil(logicalBytes / this.bufferSize)

    for (let i = 0; i < Math.max(1, buffersNeeded); i++) {
      await this.storage.put(`buffer:${i}`, new ArrayBuffer(this.bufferSize))
    }

    for (const thing of things) {
      this.data.set(thing.$id, thing)
    }
  }

  async get(id: string): Promise<Thing | undefined> {
    this.instrument.recordLogicalRead(THING_SIZE_BYTES)

    // DuckDB point lookups are less efficient (columnar optimized for scans)
    await this.storage.get('buffer:0')

    return this.data.get(id)
  }

  async update(id: string, updates: Partial<Thing>): Promise<void> {
    const existing = this.data.get(id)
    if (!existing) return

    this.instrument.recordLogicalWrite(THING_SIZE_BYTES)

    // DuckDB updates can be expensive (columnar rewrite)
    await this.storage.put('buffer:0', new ArrayBuffer(this.bufferSize))

    Object.assign(existing, updates)
  }

  preload(things: Thing[]): void {
    for (const thing of things) {
      this.data.set(thing.$id, thing)
    }
  }

  getInstrument(): VFSInstrument {
    return this.instrument
  }
}

// =============================================================================
// Benchmark Setup
// =============================================================================

let db4Instrument: VFSInstrument
let evodbInstrument: VFSInstrument
let postgresInstrument: VFSInstrument
let sqliteInstrument: VFSInstrument
let duckdbInstrument: VFSInstrument

let db4Store: MockDB4Store
let evodbStore: MockEvoDBStore
let postgresStore: MockPostgresStore
let sqliteStore: MockSQLiteStore
let duckdbStore: MockDuckDBStore

let testThings: Thing[]

const metricsAggregator = new StorageMetricsAggregator()

beforeAll(() => {
  // Create instruments
  db4Instrument = new VFSInstrument({ trackOperations: true })
  evodbInstrument = new VFSInstrument({ trackOperations: true })
  postgresInstrument = new VFSInstrument({ trackOperations: true })
  sqliteInstrument = new VFSInstrument({ trackOperations: true })
  duckdbInstrument = new VFSInstrument({ trackOperations: true })

  // Create stores
  db4Store = new MockDB4Store(db4Instrument)
  evodbStore = new MockEvoDBStore(evodbInstrument)
  postgresStore = new MockPostgresStore(postgresInstrument)
  sqliteStore = new MockSQLiteStore(sqliteInstrument)
  duckdbStore = new MockDuckDBStore(duckdbInstrument)

  // Generate test data
  testThings = generateThings(100)

  // Preload data for read benchmarks
  db4Store.preload(testThings)
  evodbStore.preload(testThings)
  postgresStore.preload(testThings)
  sqliteStore.preload(testThings)
  duckdbStore.preload(testThings)
})

afterEach(() => {
  // Log metrics after each benchmark
  // console.log('DB4:', db4Instrument.getMetrics())
})

// =============================================================================
// Write Amplification Benchmarks
// =============================================================================

describe('VFS Cost - Write Amplification', () => {
  /**
   * Measures blob writes for inserting 100 rows.
   * Lower blob count = better 2MB optimization.
   */

  bench('db4 insert 100 rows', async () => {
    db4Instrument.reset()
    const things = generateThings(100)
    await db4Store.insert(things)

    const metrics = db4Instrument.getMetrics()
    metricsAggregator.record('db4:insert100', metrics.rowsRead, metrics.blobsWritten, 0)
  })

  bench('evodb insert 100 rows', async () => {
    evodbInstrument.reset()
    const things = generateThings(100)
    await evodbStore.insert(things)

    const metrics = evodbInstrument.getMetrics()
    metricsAggregator.record('evodb:insert100', metrics.rowsRead, metrics.blobsWritten, 0)
  })

  bench('postgres insert 100 rows', async () => {
    postgresInstrument.reset()
    const things = generateThings(100)
    await postgresStore.insert(things)

    const metrics = postgresInstrument.getMetrics()
    metricsAggregator.record('postgres:insert100', metrics.rowsRead, metrics.blobsWritten, 0)
  })

  bench('sqlite insert 100 rows', async () => {
    sqliteInstrument.reset()
    const things = generateThings(100)
    await sqliteStore.insert(things)

    const metrics = sqliteInstrument.getMetrics()
    metricsAggregator.record('sqlite:insert100', metrics.rowsRead, metrics.blobsWritten, 0)
  })

  bench('duckdb insert 100 rows', async () => {
    duckdbInstrument.reset()
    const things = generateThings(100)
    await duckdbStore.insert(things)

    const metrics = duckdbInstrument.getMetrics()
    metricsAggregator.record('duckdb:insert100', metrics.rowsRead, metrics.blobsWritten, 0)
  })
})

describe('VFS Cost - Update Amplification', () => {
  /**
   * Measures blob writes for updating a single row.
   * This reveals write amplification: updating 200 bytes
   * may require rewriting a 2MB blob.
   */

  bench('db4 update single row', async () => {
    db4Instrument.reset()
    await db4Store.update('thing-00050', { status: 'inactive' })

    const metrics = db4Instrument.getMetrics()
    metricsAggregator.record('db4:update1', metrics.rowsRead, metrics.blobsWritten, 0)
  })

  bench('evodb update single row', async () => {
    evodbInstrument.reset()
    await evodbStore.update('thing-00050', { status: 'inactive' })

    const metrics = evodbInstrument.getMetrics()
    metricsAggregator.record('evodb:update1', metrics.rowsRead, metrics.blobsWritten, 0)
  })

  bench('postgres update single row', async () => {
    postgresInstrument.reset()
    await postgresStore.update('thing-00050', { status: 'inactive' })

    const metrics = postgresInstrument.getMetrics()
    metricsAggregator.record('postgres:update1', metrics.rowsRead, metrics.blobsWritten, 0)
  })

  bench('sqlite update single row', async () => {
    sqliteInstrument.reset()
    await sqliteStore.update('thing-00050', { status: 'inactive' })

    const metrics = sqliteInstrument.getMetrics()
    metricsAggregator.record('sqlite:update1', metrics.rowsRead, metrics.blobsWritten, 0)
  })

  bench('duckdb update single row', async () => {
    duckdbInstrument.reset()
    await duckdbStore.update('thing-00050', { status: 'inactive' })

    const metrics = duckdbInstrument.getMetrics()
    metricsAggregator.record('duckdb:update1', metrics.rowsRead, metrics.blobsWritten, 0)
  })
})

// =============================================================================
// Read Pattern Benchmarks
// =============================================================================

describe('VFS Cost - Point Lookup Blobs', () => {
  /**
   * Measures blob reads for a single row lookup.
   * Ideally 1 blob read, but may vary by storage strategy.
   */

  bench('db4 point lookup', async () => {
    db4Instrument.reset()
    await db4Store.get('thing-00050')

    const metrics = db4Instrument.getMetrics()
    metricsAggregator.record('db4:pointLookup', metrics.blobsRead, 0, 0)
  })

  bench('evodb point lookup', async () => {
    evodbInstrument.reset()
    await evodbStore.get('thing-00050')

    const metrics = evodbInstrument.getMetrics()
    metricsAggregator.record('evodb:pointLookup', metrics.blobsRead, 0, 0)
  })

  bench('postgres point lookup', async () => {
    postgresInstrument.reset()
    await postgresStore.get('thing-00050')

    const metrics = postgresInstrument.getMetrics()
    metricsAggregator.record('postgres:pointLookup', metrics.blobsRead, 0, 0)
  })

  bench('sqlite point lookup', async () => {
    sqliteInstrument.reset()
    await sqliteStore.get('thing-00050')

    const metrics = sqliteInstrument.getMetrics()
    metricsAggregator.record('sqlite:pointLookup', metrics.blobsRead, 0, 0)
  })

  bench('duckdb point lookup', async () => {
    duckdbInstrument.reset()
    await duckdbStore.get('thing-00050')

    const metrics = duckdbInstrument.getMetrics()
    metricsAggregator.record('duckdb:pointLookup', metrics.blobsRead, 0, 0)
  })
})

// =============================================================================
// Batch Operations Comparison
// =============================================================================

describe('VFS Cost - Batch Insert Efficiency', () => {
  /**
   * Compare blob efficiency for batch vs individual inserts.
   * Batch should use fewer blobs.
   */

  bench('db4 batch insert 1000 rows', async () => {
    db4Instrument.reset()
    const things = generateThings(1000)
    await db4Store.insert(things)

    const metrics = db4Instrument.getMetrics()
    // Expect ~1 blob for 1000 rows (packed into 2MB)
    metricsAggregator.record('db4:batch1000', 0, metrics.blobsWritten, 0)
  })

  bench('postgres batch insert 1000 rows', async () => {
    postgresInstrument.reset()
    const things = generateThings(1000)
    await postgresStore.insert(things)

    const metrics = postgresInstrument.getMetrics()
    // Expect ~1-2 blobs (pages packed)
    metricsAggregator.record('postgres:batch1000', 0, metrics.blobsWritten, 0)
  })

  bench('duckdb batch insert 1000 rows', async () => {
    duckdbInstrument.reset()
    const things = generateThings(1000)
    await duckdbStore.insert(things)

    const metrics = duckdbInstrument.getMetrics()
    // DuckDB columnar should be very efficient
    metricsAggregator.record('duckdb:batch1000', 0, metrics.blobsWritten, 0)
  })
})

// =============================================================================
// Metrics Summary
// =============================================================================

describe('VFS Cost - Metrics Summary', () => {
  bench('print metrics summary', () => {
    const summary = metricsAggregator.getSummary()

    console.log('\n=== VFS Cost Analysis ===')
    console.log(`Total samples: ${summary.totalSamples}`)
    console.log(`Avg rows read: ${summary.avgRowsRead.toFixed(2)}`)
    console.log(`Avg rows written: ${summary.avgRowsWritten.toFixed(2)}`)
    console.log(`Estimated read cost: $${summary.estimatedReadCost.toFixed(6)}`)
    console.log(`Estimated write cost: $${summary.estimatedWriteCost.toFixed(6)}`)

    console.log('\nBy Operation:')
    for (const [op, stats] of Object.entries(summary.byOperation)) {
      console.log(`  ${op}: ${stats.count} samples, avg ${stats.avgRowsWritten.toFixed(2)} writes`)
    }
  })

  bench('compare amplification', () => {
    console.log('\n=== Write Amplification Comparison ===')

    const databases = [
      { name: 'db4', instrument: db4Instrument },
      { name: 'evodb', instrument: evodbInstrument },
      { name: 'postgres', instrument: postgresInstrument },
      { name: 'sqlite', instrument: sqliteInstrument },
      { name: 'duckdb', instrument: duckdbInstrument },
    ]

    for (const { name, instrument } of databases) {
      const metrics = instrument.getMetrics()
      console.log(`${name}:`)
      console.log(`  Blobs written: ${metrics.blobsWritten}`)
      console.log(`  Bytes written: ${(metrics.bytesWritten / 1024).toFixed(2)} KB`)
      console.log(`  Write amplification: ${metrics.writeAmplification.toFixed(2)}x`)
    }
  })
})

// =============================================================================
// Cost Projection
// =============================================================================

describe('VFS Cost - Monthly Projection', () => {
  /**
   * Project monthly costs based on measured blob efficiency.
   *
   * Scenario: 10K active users
   * - 100 reads/user/day
   * - 10 writes/user/day
   */

  bench('monthly cost projection', () => {
    const USERS = 10_000
    const READS_PER_DAY = 100
    const WRITES_PER_DAY = 10
    const DAYS = 30

    const monthlyReads = USERS * READS_PER_DAY * DAYS
    const monthlyWrites = USERS * WRITES_PER_DAY * DAYS

    const PRICING = {
      readPer1M: 0.001,  // $0.001 per 1M rows read
      writePer1M: 1.00,  // $1.00 per 1M rows written
    }

    console.log('\n=== Monthly Cost Projection ===')
    console.log(`Users: ${USERS.toLocaleString()}`)
    console.log(`Monthly reads: ${monthlyReads.toLocaleString()}`)
    console.log(`Monthly writes: ${monthlyWrites.toLocaleString()}`)

    // Naive: 1 row per operation
    const naiveReadCost = (monthlyReads / 1_000_000) * PRICING.readPer1M
    const naiveWriteCost = (monthlyWrites / 1_000_000) * PRICING.writePer1M
    const naiveTotal = naiveReadCost + naiveWriteCost

    console.log(`\nNaive (1 row per op):`)
    console.log(`  Read:  $${naiveReadCost.toFixed(2)}`)
    console.log(`  Write: $${naiveWriteCost.toFixed(2)}`)
    console.log(`  Total: $${naiveTotal.toFixed(2)}`)

    // Optimized: 100 rows packed per blob (conservative)
    const packingRatio = 100
    const optReadCost = (monthlyReads / packingRatio / 1_000_000) * PRICING.readPer1M
    const optWriteCost = (monthlyWrites / packingRatio / 1_000_000) * PRICING.writePer1M
    const optTotal = optReadCost + optWriteCost

    console.log(`\nOptimized (100 rows/blob):`)
    console.log(`  Read:  $${optReadCost.toFixed(4)}`)
    console.log(`  Write: $${optWriteCost.toFixed(4)}`)
    console.log(`  Total: $${optTotal.toFixed(4)}`)

    console.log(`\nSavings: $${(naiveTotal - optTotal).toFixed(2)}/month (${((1 - optTotal/naiveTotal) * 100).toFixed(1)}%)`)
  })
})
