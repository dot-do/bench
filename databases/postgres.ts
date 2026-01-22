/**
 * @dotdo/postgres Database Adapter
 *
 * PostgreSQL-compatible store using PGLite WASM.
 * WASM is lazy-loaded on first use for optimal cold start.
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
  fields: { name: string; dataTypeID: number }[]
}

export interface PostgresStore {
  // SQL query execution
  query<T = unknown>(sql: string, params?: unknown[]): Promise<QueryResult<T>>

  // Transaction support
  transaction<T>(fn: (tx: PostgresStore) => Promise<T>): Promise<T>

  // Lifecycle
  close(): Promise<void>
}

// Module-level WASM instance cache
let pgliteInstance: unknown = null

/**
 * Create a new Postgres store instance.
 * Lazy-loads PGLite WASM on first instantiation.
 *
 * NOTE: Requires @electric-sql/pglite to be installed.
 * This adapter is a placeholder for WASM database benchmarks.
 */
export async function createPostgresStore(): Promise<PostgresStore> {
  // Lazy import PGLite WASM - this is the expensive operation
  // Try @electric-sql/pglite (the real package) first, fall back to error
  let PGlite: new () => unknown
  try {
    const pglite = await import('@electric-sql/pglite')
    PGlite = pglite.PGlite
  } catch {
    throw new Error(
      'Postgres WASM adapter requires @electric-sql/pglite package. ' +
        'Install with: pnpm add @electric-sql/pglite'
    )
  }

  // Reuse WASM instance if available (for warm benchmarks)
  if (!pgliteInstance) {
    pgliteInstance = new PGlite()
    await (pgliteInstance as { waitReady: () => Promise<void> }).waitReady()
  }

  const db = pgliteInstance as {
    query: <T>(sql: string, params?: unknown[]) => Promise<QueryResult<T>>
    transaction: <T>(fn: (tx: unknown) => Promise<T>) => Promise<T>
    close: () => Promise<void>
  }

  return {
    async query<T = unknown>(sql: string, params?: unknown[]): Promise<QueryResult<T>> {
      return db.query<T>(sql, params)
    },

    async transaction<T>(fn: (tx: PostgresStore) => Promise<T>): Promise<T> {
      return db.transaction(async (tx) => {
        const txStore: PostgresStore = {
          query: (sql, params) => (tx as typeof db).query(sql, params),
          transaction: () => {
            throw new Error('Nested transactions not supported')
          },
          close: async () => {},
        }
        return fn(txStore)
      })
    },

    async close(): Promise<void> {
      // For benchmarks, we keep the WASM instance alive
      // In production, you'd call db.close()
      return Promise.resolve()
    },
  }
}

/**
 * Seed the store with 1000 test "things" with various statuses.
 */
export async function seedTestData(store: PostgresStore): Promise<void> {
  // Create tables
  await store.query(`
    CREATE TABLE IF NOT EXISTS things (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      status TEXT NOT NULL CHECK (status IN ('active', 'inactive', 'pending', 'archived')),
      created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
    )
  `)

  await store.query(`
    CREATE TABLE IF NOT EXISTS relationships (
      id TEXT PRIMARY KEY,
      subject TEXT NOT NULL REFERENCES things(id),
      predicate TEXT NOT NULL,
      object TEXT NOT NULL REFERENCES things(id),
      created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
    )
  `)

  // Create indexes for query benchmarks
  await store.query(`CREATE INDEX IF NOT EXISTS idx_things_status ON things(status)`)
  await store.query(`CREATE INDEX IF NOT EXISTS idx_relationships_subject ON relationships(subject)`)

  const statuses: Thing['status'][] = ['active', 'inactive', 'pending', 'archived']

  // Batch insert things
  await store.transaction(async (tx) => {
    for (let i = 0; i < 1000; i++) {
      const id = `thing-${String(i).padStart(3, '0')}`
      const name = `Thing ${i}`
      const status = statuses[i % statuses.length]
      const created_at = new Date(Date.now() - i * 60000).toISOString()

      await tx.query(
        'INSERT INTO things (id, name, status, created_at) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING',
        [id, name, status, created_at]
      )
    }
  })

  // Batch insert relationships
  await store.transaction(async (tx) => {
    for (let i = 0; i < 500; i++) {
      const id = `rel-${String(i).padStart(3, '0')}`
      const subjectIdx = i % 1000
      const objectIdx = (i + 1) % 1000
      const subject = `thing-${String(subjectIdx).padStart(3, '0')}`
      const object = `thing-${String(objectIdx).padStart(3, '0')}`
      const predicate = 'relates_to'
      const created_at = new Date().toISOString()

      await tx.query(
        'INSERT INTO relationships (id, subject, predicate, object, created_at) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO NOTHING',
        [id, subject, predicate, object, created_at]
      )
    }
  })
}

/**
 * Restore store state from DO storage (for hibernation benchmarks).
 * For SQL databases, this typically involves restoring the database file.
 */
export async function restoreFromStorage(
  store: PostgresStore,
  storage: Map<string, ArrayBuffer>
): Promise<void> {
  // PGLite uses an in-memory database by default
  // For DO hibernation, you would persist the database to storage
  // and restore it here by re-initializing PGLite with the stored data

  // Check if we have a stored database snapshot
  const dbSnapshot = storage.get('pglite:snapshot')
  if (dbSnapshot) {
    // In a real implementation, you would restore from the snapshot
    // For now, we just re-seed the data
    await seedTestData(store)
  }
}
