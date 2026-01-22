/**
 * @dotdo/evodb Database Adapter
 *
 * Pure TypeScript event-sourced document store - no WASM required.
 * Provides a fluent query builder API.
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

export interface QueryBuilder<T> {
  where(field: string, op: '=' | '!=' | '>' | '<' | '>=' | '<=', value: unknown): QueryBuilder<T>
  join(table: string, leftField: string, rightField: string): QueryBuilder<T>
  limit(n: number): QueryBuilder<T>
  offset(n: number): QueryBuilder<T>
  orderBy(field: string, direction?: 'asc' | 'desc'): QueryBuilder<T>
  all(): Promise<T[]>
  first(): Promise<T | null>
  count(): Promise<number>
}

export interface EvoDBStore {
  // Document operations
  get(collection: string, id: string): Promise<Thing | null>
  set(collection: string, id: string, data: Omit<Thing, 'id'>): Promise<void>
  delete(collection: string, id: string): Promise<boolean>

  // Query builder
  query<T = Thing>(collection: string): QueryBuilder<T>

  // Lifecycle
  close(): Promise<void>
}

/**
 * Create a new EvoDB store instance.
 * Pure TypeScript - no WASM loading required.
 */
export async function createEvoDBStore(): Promise<EvoDBStore> {
  // Lazy import the actual implementation
  const { EvoDB } = await import('@dotdo/evodb')

  const db = new EvoDB()

  return {
    async get(collection: string, id: string): Promise<Thing | null> {
      return db.get<Thing>(collection, id)
    },

    async set(collection: string, id: string, data: Omit<Thing, 'id'>): Promise<void> {
      return db.set(collection, id, data)
    },

    async delete(collection: string, id: string): Promise<boolean> {
      return db.delete(collection, id)
    },

    query<T = Thing>(collection: string): QueryBuilder<T> {
      return db.query<T>(collection) as QueryBuilder<T>
    },

    async close(): Promise<void> {
      return db.close()
    },
  }
}

/**
 * Seed the store with 1000 test "things" with various statuses.
 */
export async function seedTestData(store: EvoDBStore): Promise<void> {
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
 * EvoDB is event-sourced, so this replays events from storage.
 */
export async function restoreFromStorage(
  store: EvoDBStore,
  storage: Map<string, ArrayBuffer>
): Promise<void> {
  // In a real DO, events would be stored and replayed
  // This simulates restoring state on wake
  const decoder = new TextDecoder()

  for (const [key, buffer] of storage) {
    const data = JSON.parse(decoder.decode(buffer))
    const [collection, id] = key.split(':')
    if (collection && id) {
      await store.set(collection, id, data)
    }
  }
}
