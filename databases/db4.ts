/**
 * @dotdo/db4 Database Adapter
 *
 * Pure TypeScript document store - no WASM required.
 * Optimized for cold start performance and minimal bundle size.
 */

// Types
export interface Thing {
  id: string
  name: string
  status: 'active' | 'inactive' | 'pending' | 'archived'
  created_at: string
}

export interface QueryOptions {
  where?: Record<string, unknown>
  limit?: number
  offset?: number
}

export interface JoinQueryOptions {
  from: string
  join?: {
    table: string
    on: string
  }
  limit?: number
}

export interface DB4Store {
  // Document operations
  get(collection: string, id: string): Promise<Thing | null>
  list(collection: string, options?: QueryOptions): Promise<Thing[]>
  count(collection: string, options?: QueryOptions): Promise<number>
  set(collection: string, id: string, data: Omit<Thing, 'id'>): Promise<void>
  delete(collection: string, id: string): Promise<boolean>

  // SQL-like query for compatibility
  query(sql: string | JoinQueryOptions): Promise<unknown[]>

  // Lifecycle
  close(): Promise<void>
}

/**
 * Create a new DB4 store instance.
 * Pure TypeScript - no WASM loading required.
 */
export async function createDB4Store(): Promise<DB4Store> {
  // Lazy import the actual implementation
  const { DB4 } = await import('@dotdo/db4')

  const db = new DB4()

  return {
    async get(collection: string, id: string): Promise<Thing | null> {
      return db.get<Thing>(collection, id)
    },

    async list(collection: string, options?: QueryOptions): Promise<Thing[]> {
      return db.list<Thing>(collection, options)
    },

    async count(collection: string, options?: QueryOptions): Promise<number> {
      return db.count(collection, options)
    },

    async set(collection: string, id: string, data: Omit<Thing, 'id'>): Promise<void> {
      return db.set(collection, id, data)
    },

    async delete(collection: string, id: string): Promise<boolean> {
      return db.delete(collection, id)
    },

    async query(sql: string | JoinQueryOptions): Promise<unknown[]> {
      return db.query(sql)
    },

    async close(): Promise<void> {
      return db.close()
    },
  }
}

/**
 * Seed the store with 1000 test "things" with various statuses.
 */
export async function seedTestData(store: DB4Store): Promise<void> {
  const statuses: Thing['status'][] = ['active', 'inactive', 'pending', 'archived']

  const promises: Promise<void>[] = []

  for (let i = 0; i < 1000; i++) {
    const id = `thing-${String(i).padStart(3, '0')}`
    const thing: Omit<Thing, 'id'> = {
      name: `Thing ${i}`,
      status: statuses[i % statuses.length],
      created_at: new Date(Date.now() - i * 60000).toISOString(),
    }
    promises.push(store.set('things', id, thing))
  }

  // Also seed relationships for join benchmarks
  for (let i = 0; i < 500; i++) {
    const subjectIdx = i % 1000
    const objectIdx = (i + 1) % 1000
    promises.push(
      store.set('relationships', `rel-${String(i).padStart(3, '0')}`, {
        subject: `thing-${String(subjectIdx).padStart(3, '0')}`,
        predicate: 'relates_to',
        object: `thing-${String(objectIdx).padStart(3, '0')}`,
        created_at: new Date().toISOString(),
      } as unknown as Omit<Thing, 'id'>)
    )
  }

  await Promise.all(promises)
}

/**
 * Restore store state from DO storage (for hibernation benchmarks).
 * DB4 stores state in memory, so this restores from serialized storage.
 */
export async function restoreFromStorage(
  store: DB4Store,
  storage: Map<string, ArrayBuffer>
): Promise<void> {
  // In a real DO, state would be serialized to storage
  // This simulates restoring that state on wake
  const decoder = new TextDecoder()

  for (const [key, buffer] of storage) {
    const data = JSON.parse(decoder.decode(buffer))
    const [collection, id] = key.split(':')
    if (collection && id) {
      await store.set(collection, id, data)
    }
  }
}
