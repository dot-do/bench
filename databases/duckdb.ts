/**
 * @dotdo/duckdb Database Adapter
 *
 * DuckDB-compatible store using DuckDB WASM.
 * WASM is lazy-loaded on first use for optimal cold start.
 * Optimized for analytical queries and columnar storage.
 */

// Types
export interface Thing {
  id: string
  name: string
  status: 'active' | 'inactive' | 'pending' | 'archived'
  created_at: string
}

export interface Relationship {
  id: string
  subject: string
  predicate: string
  object: string
  created_at: string
}

export interface QueryResult<T = unknown> {
  rows: T[]
  rowCount: number
  columns: { name: string; type: string }[]
}

export interface DuckDBStore {
  // SQL query execution
  query<T = unknown>(sql: string, params?: unknown[]): Promise<QueryResult<T>>

  // Prepared statements for repeated queries
  prepare(sql: string): Promise<PreparedStatement>

  // Lifecycle
  close(): Promise<void>
}

export interface PreparedStatement {
  run(params?: unknown[]): Promise<QueryResult>
  all<T = unknown>(params?: unknown[]): Promise<T[]>
  close(): Promise<void>
}

// Module-level WASM instance cache
let duckdbInstance: unknown = null
let duckdbConnection: unknown = null

/**
 * Create a new DuckDB store instance.
 * Lazy-loads DuckDB WASM on first instantiation.
 *
 * NOTE: Requires @duckdb/duckdb-wasm to be installed.
 * This adapter is a placeholder for WASM database benchmarks.
 */
export async function createDuckDBStore(): Promise<DuckDBStore> {
  // Lazy import DuckDB WASM - this is the expensive operation
  // Try @duckdb/duckdb-wasm (the real package) first, fall back to error
  let duckdb: {
    selectBundle: (bundles: unknown) => Promise<{ mainModule: string; mainWorker: string }>
    DUCKDB_BUNDLES: { mvp: { mainModule: string; mainWorker: string }; eh: { mainModule: string; mainWorker: string } }
    ConsoleLogger: new () => unknown
    AsyncDuckDB: new (logger: unknown, worker: Worker) => unknown
  }
  try {
    duckdb = await import('@duckdb/duckdb-wasm')
  } catch {
    throw new Error(
      'DuckDB WASM adapter requires @duckdb/duckdb-wasm package. ' +
        'Install with: pnpm add @duckdb/duckdb-wasm'
    )
  }

  // Reuse WASM instance if available (for warm benchmarks)
  if (!duckdbInstance) {
    // Initialize DuckDB WASM
    const bundle = await duckdb.selectBundle({
      mvp: { mainModule: duckdb.DUCKDB_BUNDLES.mvp.mainModule, mainWorker: duckdb.DUCKDB_BUNDLES.mvp.mainWorker },
      eh: { mainModule: duckdb.DUCKDB_BUNDLES.eh.mainModule, mainWorker: duckdb.DUCKDB_BUNDLES.eh.mainWorker },
    })

    const worker = new Worker(bundle.mainWorker)
    const logger = new duckdb.ConsoleLogger()

    duckdbInstance = new duckdb.AsyncDuckDB(logger, worker)
    await (duckdbInstance as { instantiate: (module: string) => Promise<void> }).instantiate(bundle.mainModule)

    duckdbConnection = await (duckdbInstance as { connect: () => Promise<unknown> }).connect()
  }

  const conn = duckdbConnection as {
    query: <T>(sql: string) => Promise<{ toArray: () => T[] }>
    prepare: (sql: string) => Promise<unknown>
    close: () => Promise<void>
  }

  return {
    async query<T = unknown>(sql: string, params?: unknown[]): Promise<QueryResult<T>> {
      // DuckDB WASM uses positional parameters with $1, $2, etc.
      let processedSql = sql
      if (params && params.length > 0) {
        // Replace ? placeholders with $N for DuckDB
        let paramIndex = 0
        processedSql = sql.replace(/\?/g, () => `$${++paramIndex}`)

        // Create a prepared statement for parameterized queries
        const stmt = await conn.prepare(processedSql)
        const stmtTyped = stmt as { query: (...args: unknown[]) => Promise<{ toArray: () => T[] }> }
        const result = await stmtTyped.query(...params)
        const rows = result.toArray()

        return {
          rows,
          rowCount: rows.length,
          columns: [], // Would need to extract from result schema
        }
      }

      const result = await conn.query<T>(processedSql)
      const rows = result.toArray()

      return {
        rows,
        rowCount: rows.length,
        columns: [],
      }
    },

    async prepare(sql: string): Promise<PreparedStatement> {
      const stmt = await conn.prepare(sql)
      const stmtTyped = stmt as {
        query: (...args: unknown[]) => Promise<{ toArray: () => unknown[] }>
        close: () => Promise<void>
      }

      return {
        async run(params?: unknown[]): Promise<QueryResult> {
          const result = await stmtTyped.query(...(params || []))
          const rows = result.toArray()
          return { rows, rowCount: rows.length, columns: [] }
        },

        async all<T = unknown>(params?: unknown[]): Promise<T[]> {
          const result = await stmtTyped.query(...(params || []))
          return result.toArray() as T[]
        },

        async close(): Promise<void> {
          await stmtTyped.close()
        },
      }
    },

    async close(): Promise<void> {
      // For benchmarks, we keep the WASM instance alive
      // In production, you'd close connection and terminate worker
      return Promise.resolve()
    },
  }
}

/**
 * Seed the store with 1000 test "things" with various statuses.
 */
export async function seedTestData(store: DuckDBStore): Promise<void> {
  // Create tables - DuckDB uses slightly different syntax
  await store.query(`
    CREATE TABLE IF NOT EXISTS things (
      id VARCHAR PRIMARY KEY,
      name VARCHAR NOT NULL,
      status VARCHAR NOT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
  `)

  await store.query(`
    CREATE TABLE IF NOT EXISTS relationships (
      id VARCHAR PRIMARY KEY,
      subject VARCHAR NOT NULL,
      predicate VARCHAR NOT NULL,
      object VARCHAR NOT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
  `)

  // Create indexes for query benchmarks
  await store.query(`CREATE INDEX IF NOT EXISTS idx_things_status ON things(status)`)
  await store.query(`CREATE INDEX IF NOT EXISTS idx_relationships_subject ON relationships(subject)`)

  const statuses: Thing['status'][] = ['active', 'inactive', 'pending', 'archived']

  // DuckDB is optimized for batch inserts, so we build a large INSERT
  const thingValues: string[] = []
  for (let i = 0; i < 1000; i++) {
    const id = `thing-${String(i).padStart(3, '0')}`
    const name = `Thing ${i}`
    const status = statuses[i % statuses.length]
    const created_at = new Date(Date.now() - i * 60000).toISOString()
    thingValues.push(`('${id}', '${name}', '${status}', '${created_at}')`)
  }

  await store.query(`
    INSERT OR IGNORE INTO things (id, name, status, created_at)
    VALUES ${thingValues.join(', ')}
  `)

  // Batch insert relationships
  const relValues: string[] = []
  for (let i = 0; i < 500; i++) {
    const id = `rel-${String(i).padStart(3, '0')}`
    const subjectIdx = i % 1000
    const objectIdx = (i + 1) % 1000
    const subject = `thing-${String(subjectIdx).padStart(3, '0')}`
    const object = `thing-${String(objectIdx).padStart(3, '0')}`
    const predicate = 'relates_to'
    const created_at = new Date().toISOString()
    relValues.push(`('${id}', '${subject}', '${predicate}', '${object}', '${created_at}')`)
  }

  await store.query(`
    INSERT OR IGNORE INTO relationships (id, subject, predicate, object, created_at)
    VALUES ${relValues.join(', ')}
  `)
}

/**
 * Restore store state from DO storage (for hibernation benchmarks).
 * For DuckDB, this typically involves restoring from Parquet or database file.
 */
export async function restoreFromStorage(
  store: DuckDBStore,
  storage: Map<string, ArrayBuffer>
): Promise<void> {
  // DuckDB can read from Parquet files or persist to a file
  // For DO hibernation, you would export to Parquet and store in DO storage

  // Check if we have stored Parquet files
  const thingsParquet = storage.get('duckdb:things.parquet')
  const relationshipsParquet = storage.get('duckdb:relationships.parquet')

  if (thingsParquet && relationshipsParquet) {
    // In a real implementation, you would:
    // 1. Write parquet buffers to virtual filesystem
    // 2. Load tables from parquet files
    // await store.query("CREATE TABLE things AS SELECT * FROM read_parquet('things.parquet')")

    // For now, we just re-seed the data
    await seedTestData(store)
  }
}
