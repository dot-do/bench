/**
 * @dotdo/mongodb Database Adapter
 *
 * MongoDB-compatible store using PostgreSQL/DocumentDB backend.
 * Connects to DocumentDB (PGLite WASM) for SQL-based document storage.
 */

import type { MongoClient, Collection, Db, Document, Filter, FindOptions, InsertOneResult, UpdateResult, DeleteResult, AggregateOptions } from '@dotdo/mongodb'

// Dataset size configuration
export type DatasetSize = 'small' | 'medium' | 'large' | 'xlarge'

export const DATASET_SIZES: Record<DatasetSize, { things: number; relationships: number }> = {
  small: { things: 100, relationships: 50 },
  medium: { things: 1000, relationships: 500 },
  large: { things: 10000, relationships: 5000 },
  xlarge: { things: 100000, relationships: 50000 },
}

// Types
export interface Thing {
  _id: string
  name: string
  status: 'active' | 'inactive' | 'pending' | 'archived'
  category?: string
  tags?: string[]
  metadata?: Record<string, unknown>
  created_at: Date
  updated_at?: Date
  [key: string]: unknown // Index signature for Document compatibility
}

export interface Relationship {
  _id: string
  subject: string
  predicate: string
  object: string
  weight?: number
  created_at: Date
  [key: string]: unknown // Index signature for Document compatibility
}

export interface MongoPostgresStore {
  // MongoDB Client interface
  client: MongoClient

  // Database access
  db(name?: string): Db

  // Collection access
  collection<T extends Document = Document>(name: string): Collection<T>

  // Convenience methods for benchmarks
  things: Collection<Thing>
  relationships: Collection<Relationship>

  // Lifecycle
  close(): Promise<void>
}

// Module-level client cache
let mongoClient: MongoClient | null = null

/**
 * Create a new MongoDB store instance using PostgreSQL/DocumentDB backend.
 * Lazy-loads PGLite WASM on first instantiation.
 */
export async function createMongoPostgresStore(): Promise<MongoPostgresStore> {
  // Lazy import the actual implementation
  const { MongoClient } = await import('@dotdo/mongodb')

  // Reuse client if available (for warm benchmarks)
  if (!mongoClient) {
    // DocumentDB connection string uses pglite:// protocol
    mongoClient = new MongoClient('documentdb://pglite:memory')
    await mongoClient.connect()
  }

  const db = mongoClient.db('bench')

  return {
    client: mongoClient,

    db(name = 'bench'): Db {
      return mongoClient!.db(name)
    },

    collection<T extends Document = Document>(name: string): Collection<T> {
      return db.collection<T>(name)
    },

    get things(): Collection<Thing> {
      return db.collection<Thing>('things')
    },

    get relationships(): Collection<Relationship> {
      return db.collection<Relationship>('relationships')
    },

    async close(): Promise<void> {
      // For benchmarks, we keep the client alive
      // In production, you'd call mongoClient.close()
      return Promise.resolve()
    },
  }
}

/**
 * Seed the store with test data based on size parameter.
 */
export async function seedTestData(store: MongoPostgresStore, size: DatasetSize = 'medium'): Promise<void> {
  const { things: thingCount, relationships: relCount } = DATASET_SIZES[size]
  const statuses: Thing['status'][] = ['active', 'inactive', 'pending', 'archived']
  const categories = ['electronics', 'clothing', 'food', 'services', 'software']
  const tagOptions = ['featured', 'new', 'sale', 'premium', 'limited']

  // Create indexes for query benchmarks
  // DocumentDB translates these to PostgreSQL indexes
  await store.things.createIndex({ status: 1 })
  await store.things.createIndex({ category: 1 })
  await store.things.createIndex({ 'metadata.priority': 1 })
  await store.things.createIndex({ tags: 1 })
  await store.relationships.createIndex({ subject: 1 })
  await store.relationships.createIndex({ predicate: 1 })

  // Batch insert things
  const thingDocs: Thing[] = []
  for (let i = 0; i < thingCount; i++) {
    const id = `thing-${String(i).padStart(6, '0')}`
    thingDocs.push({
      _id: id,
      name: `Thing ${i}`,
      status: statuses[i % statuses.length],
      category: categories[i % categories.length],
      tags: [tagOptions[i % tagOptions.length], tagOptions[(i + 1) % tagOptions.length]],
      metadata: {
        priority: i % 10,
        score: Math.random() * 100,
        flags: { active: i % 2 === 0, verified: i % 3 === 0 },
      },
      created_at: new Date(Date.now() - i * 60000),
    })

    // Insert in batches of 1000
    if (thingDocs.length >= 1000) {
      await store.things.insertMany(thingDocs)
      thingDocs.length = 0
    }
  }

  // Insert remaining things
  if (thingDocs.length > 0) {
    await store.things.insertMany(thingDocs)
  }

  // Batch insert relationships
  const relDocs: Relationship[] = []
  for (let i = 0; i < relCount; i++) {
    const subjectIdx = i % thingCount
    const objectIdx = (i + 1) % thingCount
    relDocs.push({
      _id: `rel-${String(i).padStart(6, '0')}`,
      subject: `thing-${String(subjectIdx).padStart(6, '0')}`,
      predicate: i % 2 === 0 ? 'relates_to' : 'depends_on',
      object: `thing-${String(objectIdx).padStart(6, '0')}`,
      weight: Math.random(),
      created_at: new Date(),
    })

    // Insert in batches of 1000
    if (relDocs.length >= 1000) {
      await store.relationships.insertMany(relDocs)
      relDocs.length = 0
    }
  }

  // Insert remaining relationships
  if (relDocs.length > 0) {
    await store.relationships.insertMany(relDocs)
  }
}

/**
 * Restore store state from DO storage (for hibernation benchmarks).
 * DocumentDB uses PGLite WASM, so this restores from serialized storage.
 */
export async function restoreFromStorage(
  store: MongoPostgresStore,
  storage: Map<string, ArrayBuffer>
): Promise<void> {
  const decoder = new TextDecoder()

  for (const [key, buffer] of storage) {
    const data = JSON.parse(decoder.decode(buffer))
    const [collection, id] = key.split(':')

    if (collection === 'things' && id) {
      await store.things.replaceOne({ _id: id }, data, { upsert: true })
    } else if (collection === 'relationships' && id) {
      await store.relationships.replaceOne({ _id: id }, data, { upsert: true })
    }
  }
}

/**
 * Clear all data from the store (for benchmark isolation).
 */
export async function clearStore(store: MongoPostgresStore): Promise<void> {
  await store.things.deleteMany({})
  await store.relationships.deleteMany({})
}
