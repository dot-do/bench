/**
 * @dotdo/postgres Database Adapter
 *
 * PostgreSQL-compatible store using PGLite WASM.
 * WASM is lazy-loaded on first use for optimal cold start.
 *
 * This adapter supports both Node.js and Cloudflare Workers environments:
 * - In Node.js: Uses @electric-sql/pglite with URL-based WASM loading
 * - In Workers: Requires wasmModule and fsBundle to be passed via createPostgresStoreWithAssets()
 *
 * ## Cloudflare Workers Usage
 *
 * The "Invalid URL string" error occurs when trying to use standard PGLite
 * in Cloudflare Workers because `import.meta.url` is undefined in Workers.
 *
 * To use PGLite in Workers, you must:
 * 1. Import WASM assets statically at bundle time
 * 2. Use createPostgresStoreWithAssets() with wasmModule and fsBundle
 *
 * Example:
 * ```typescript
 * import pgliteWasm from './pglite-assets/pglite.wasm'
 * import pgliteData from './pglite-assets/pglite.data'
 * import { createPostgresStoreWithAssets } from '../databases/postgres.js'
 *
 * const store = await createPostgresStoreWithAssets({
 *   wasmModule: pgliteWasm,
 *   fsBundle: pgliteData,
 * })
 * ```
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

/**
 * Options for creating a Workers-compatible PostgresStore
 */
export interface WorkersPostgresStoreOptions {
  /**
   * Pre-compiled WASM module (required in Workers)
   * Import at bundle time: import pgliteWasm from './pglite.wasm'
   */
  wasmModule: WebAssembly.Module

  /**
   * Filesystem bundle (pglite.data) containing PostgreSQL data files
   * Can be ArrayBuffer, Blob, ReadableStream, or typed array
   * Import at bundle time: import pgliteData from './pglite.data'
   */
  fsBundle: ArrayBuffer | Blob | ReadableStream<Uint8Array> | ArrayBufferView

  /**
   * Database name (default: 'postgres')
   */
  database?: string

  /**
   * Enable debug logging
   */
  debug?: boolean
}

// Module-level WASM instance cache
let pgliteInstance: unknown = null

/**
 * Detect if running in Cloudflare Workers environment
 * Workers have navigator.userAgent containing "Cloudflare-Workers" or lack import.meta.url
 */
function isCloudflareWorkers(): boolean {
  // Check for Cloudflare Workers global
  // @ts-expect-error - caches is a Workers-specific global
  if (typeof caches !== 'undefined' && typeof caches.default !== 'undefined') {
    return true
  }
  // Check navigator.userAgent for Workers
  if (typeof navigator !== 'undefined' && navigator.userAgent?.includes('Cloudflare-Workers')) {
    return true
  }
  return false
}

/**
 * Create a PGLite instance for Node.js environment.
 * Uses @electric-sql/pglite with standard URL-based WASM loading.
 */
async function createNodePGLiteInstance(): Promise<unknown> {
  const { PGlite } = await import('@electric-sql/pglite')
  const instance = new PGlite()
  await (instance as { waitReady: Promise<void> }).waitReady
  return instance
}

/**
 * Create a PGLite instance with pre-compiled WASM assets.
 * This is the only way to use PGLite in Cloudflare Workers.
 */
async function createPGLiteWithAssets(options: WorkersPostgresStoreOptions): Promise<unknown> {
  const { PGlite } = await import('@electric-sql/pglite')

  // Convert fsBundle to Blob if needed (PGlite expects Blob for fsBundle)
  let fsBundleBlob: Blob
  if (options.fsBundle instanceof Blob) {
    fsBundleBlob = options.fsBundle
  } else if (options.fsBundle instanceof ArrayBuffer) {
    fsBundleBlob = new Blob([options.fsBundle])
  } else if (options.fsBundle instanceof ReadableStream) {
    const response = new Response(options.fsBundle)
    const buffer = await response.arrayBuffer()
    fsBundleBlob = new Blob([buffer])
  } else if (ArrayBuffer.isView(options.fsBundle)) {
    // Convert ArrayBufferView to ArrayBuffer slice for Blob constructor compatibility
    const view = options.fsBundle
    const buffer = view.buffer.slice(view.byteOffset, view.byteOffset + view.byteLength) as ArrayBuffer
    fsBundleBlob = new Blob([buffer])
  } else {
    throw new Error('fsBundle must be ArrayBuffer, Blob, ReadableStream, or ArrayBufferView')
  }

  // Create PGLite instance with pre-compiled WASM
  const instance = await PGlite.create({
    wasmModule: options.wasmModule,
    fsBundle: fsBundleBlob,
    // Use in-memory storage for Workers (no filesystem access)
    dataDir: undefined,
  })

  return instance
}

/**
 * Create a PostgresStore from an existing PGLite instance
 */
function createStoreFromInstance(instance: unknown): PostgresStore {
  const db = instance as {
    query: <T>(sql: string, params?: unknown[]) => Promise<{ rows: T[]; fields?: { name: string; dataTypeID: number }[]; affectedRows?: number }>
    transaction: <T>(fn: (tx: unknown) => Promise<T>) => Promise<T>
    close: () => Promise<void>
  }

  return {
    async query<T = unknown>(sql: string, params?: unknown[]): Promise<QueryResult<T>> {
      const result = await db.query<T>(sql, params)
      return {
        rows: result.rows,
        rowCount: result.rows.length,
        fields: result.fields ?? [],
      }
    },

    async transaction<T>(fn: (tx: PostgresStore) => Promise<T>): Promise<T> {
      return db.transaction(async (tx) => {
        const txStore: PostgresStore = {
          query: async <R = unknown>(sql: string, params?: unknown[]): Promise<QueryResult<R>> => {
            const result = await (tx as typeof db).query<R>(sql, params)
            return {
              rows: result.rows,
              rowCount: result.rows.length,
              fields: result.fields ?? [],
            }
          },
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
 * Create a new Postgres store instance for Cloudflare Workers.
 *
 * This function MUST be used in Cloudflare Workers because standard PGLite
 * initialization uses URL resolution which fails in Workers.
 *
 * @param options - WASM assets and configuration
 * @returns PostgresStore instance
 *
 * @example
 * ```typescript
 * // In your Worker, import WASM assets statically:
 * import pgliteWasm from './pglite-assets/pglite.wasm'
 * import pgliteData from './pglite-assets/pglite.data'
 *
 * const store = await createPostgresStoreWithAssets({
 *   wasmModule: pgliteWasm,
 *   fsBundle: pgliteData,
 * })
 * ```
 */
export async function createPostgresStoreWithAssets(
  options: WorkersPostgresStoreOptions
): Promise<PostgresStore> {
  // Always create a new instance when assets are provided
  // (don't use the global cache since different workers might have different assets)
  const instance = await createPGLiteWithAssets(options)
  return createStoreFromInstance(instance)
}

/**
 * Create a new Postgres store instance.
 * Lazy-loads PGLite WASM on first instantiation.
 *
 * **WARNING**: This function only works in Node.js. In Cloudflare Workers,
 * use `createPostgresStoreWithAssets()` instead.
 *
 * This function will throw an error if called in a Workers environment
 * because PGLite's URL-based WASM loading doesn't work in Workers.
 */
export async function createPostgresStore(): Promise<PostgresStore> {
  // Check if we're in Workers - if so, throw a helpful error
  if (isCloudflareWorkers()) {
    throw new Error(
      'createPostgresStore() cannot be used in Cloudflare Workers. ' +
        'Use createPostgresStoreWithAssets() instead with pre-compiled WASM assets. ' +
        'See the module documentation for usage examples.'
    )
  }

  // Reuse WASM instance if available (for warm benchmarks)
  if (!pgliteInstance) {
    try {
      pgliteInstance = await createNodePGLiteInstance()
    } catch (error) {
      const err = error as Error
      throw new Error(
        `Failed to create PGLite: ${err.message}. ` +
          'Ensure @electric-sql/pglite is installed. ' +
          'Install with: pnpm add @electric-sql/pglite'
      )
    }
  }

  return createStoreFromInstance(pgliteInstance)
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
