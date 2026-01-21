import { bench, describe, beforeAll, afterAll } from 'vitest'
import type { GraphDBStore } from '../databases/graphdb'
import type { SDBStore } from '../databases/sdb'
import type { DB4Store } from '../databases/db4'

/**
 * Graph Database Benchmarks
 *
 * Compares graph operations across:
 * - @dotdo/graphdb (triple store)
 * - @dotdo/sdb (document/graph hybrid)
 * - db4 (graph paradigm via document store)
 *
 * Operations tested:
 * - Triple/relationship insert
 * - 1-hop and 2-hop traversal
 * - Pattern matching queries
 * - Reverse traversal (backlinks)
 * - Batch operations
 */

// Cached store instances
let graphdbStore: GraphDBStore | null = null
let sdbStore: SDBStore | null = null
let db4Store: DB4Store | null = null

async function getGraphDBStore(): Promise<GraphDBStore> {
  if (!graphdbStore) {
    const { createGraphDBStore, seedTestData } = await import('../databases/graphdb')
    graphdbStore = await createGraphDBStore()
    await seedTestData(graphdbStore, 'medium')
  }
  return graphdbStore
}

async function getSDBStore(): Promise<SDBStore> {
  if (!sdbStore) {
    const { createSDBStore, seedTestData } = await import('../databases/sdb')
    sdbStore = await createSDBStore()
    await seedTestData(sdbStore, 'medium')
  }
  return sdbStore
}

async function getDB4Store(): Promise<DB4Store> {
  if (!db4Store) {
    const { createDB4Store, seedTestData } = await import('../databases/db4')
    db4Store = await createDB4Store()
    await seedTestData(db4Store)
  }
  return db4Store
}

// Cleanup after all tests
afterAll(async () => {
  if (graphdbStore) await graphdbStore.close()
  if (sdbStore) sdbStore.close()
  if (db4Store) await db4Store.close()
})

// ============================================================================
// Triple/Relationship Insert Benchmarks
// ============================================================================

describe('Graph - Triple Insert', () => {
  bench('graphdb triple insert', async () => {
    const store = await getGraphDBStore()
    const id = `bench-${Date.now()}-${Math.random().toString(36).slice(2)}`
    await store.insertTriple({
      subject: `user:${id}`,
      predicate: 'follows',
      object: { type: 'REF', value: 'user:target-1' },
    })
  })

  bench('sdb relationship insert', async () => {
    const store = await getSDBStore()
    const id = `bench-${Date.now()}-${Math.random().toString(36).slice(2)}`
    await store.create('relationships', {
      $id: `rel-${id}`,
      subject: `thing-0001`,
      predicate: 'relates_to',
      object: `thing-0002`,
    })
  })

  bench('db4 relationship insert', async () => {
    const store = await getDB4Store()
    const id = `bench-${Date.now()}-${Math.random().toString(36).slice(2)}`
    await store.set('relationships', `rel-${id}`, {
      subject: 'thing-001',
      predicate: 'relates_to',
      object: 'thing-002',
      created_at: new Date().toISOString(),
    } as any)
  })
})

describe('Graph - Batch Triple Insert (100 triples)', () => {
  bench('graphdb batch triple insert', async () => {
    const store = await getGraphDBStore()
    const prefix = `batch-${Date.now()}`
    const triples = Array.from({ length: 100 }, (_, i) => ({
      subject: `${prefix}-user:${i}`,
      predicate: 'follows',
      object: { type: 'REF' as const, value: `${prefix}-user:${(i + 1) % 100}` },
    }))
    await store.insertTriples(triples)
  })

  bench('sdb batch relationship insert', async () => {
    const store = await getSDBStore()
    const prefix = `batch-${Date.now()}`
    const items = Array.from({ length: 100 }, (_, i) => ({
      $id: `${prefix}-rel-${i}`,
      data: {
        subject: `thing-${String(i % 1000).padStart(4, '0')}`,
        predicate: 'relates_to',
        object: `thing-${String((i + 1) % 1000).padStart(4, '0')}`,
      },
    }))
    await store.bulkCreate('relationships', items)
  })
})

// ============================================================================
// Graph Traversal Benchmarks
// ============================================================================

describe('Graph - 1-Hop Traversal', () => {
  bench('graphdb 1-hop traverse', async () => {
    const store = await getGraphDBStore()
    await store.traverse('thing-0001', 'relates_to', { maxDepth: 1, limit: 100 })
  })

  bench('sdb 1-hop traverse', async () => {
    const store = await getSDBStore()
    await store.traverse('thing-0001', 'relates_to', { limit: 100 })
  })

  bench('db4 1-hop query (join)', async () => {
    const store = await getDB4Store()
    await store.query({
      from: 'things',
      join: { table: 'relationships', on: 'things.id = relationships.subject' },
      limit: 100,
    })
  })
})

describe('Graph - 2-Hop Traversal', () => {
  bench('graphdb 2-hop traverse', async () => {
    const store = await getGraphDBStore()
    await store.pathTraverse('thing-0001', ['relates_to', 'relates_to'], { limit: 100 })
  })

  bench('sdb 2-hop traverse (manual)', async () => {
    const store = await getSDBStore()
    // First hop
    const firstHop = await store.traverse('thing-0001', 'relates_to', { limit: 50 })
    // Second hop for each result
    const secondHopPromises = firstHop.slice(0, 10).map((thing) =>
      store.traverse(thing.$id, 'relates_to', { limit: 10 })
    )
    await Promise.all(secondHopPromises)
  })

  bench('db4 2-hop query (nested join)', async () => {
    const store = await getDB4Store()
    // First get relationships
    const result = await store.query({
      from: 'relationships',
      join: { table: 'relationships', on: 'relationships.object = relationships.subject' },
      limit: 100,
    })
  })
})

describe('Graph - Reverse Traversal (Backlinks)', () => {
  bench('graphdb reverse traverse', async () => {
    const store = await getGraphDBStore()
    await store.reverseTraverse('thing-0002', 'relates_to', { limit: 100 })
  })

  bench('sdb reverse traverse (query)', async () => {
    const store = await getSDBStore()
    // Find all relationships pointing to this thing
    const result = await store.find('relationships', { object: 'thing-0002' }, { limit: 100 })
  })

  bench('db4 reverse query', async () => {
    const store = await getDB4Store()
    await store.list('relationships', { where: { object: 'thing-002' }, limit: 100 })
  })
})

// ============================================================================
// Pattern Matching Benchmarks
// ============================================================================

describe('Graph - Pattern Matching (Type Query)', () => {
  bench('graphdb type query', async () => {
    const store = await getGraphDBStore()
    await store.query('type:Thing')
  })

  bench('sdb type list', async () => {
    const store = await getSDBStore()
    await store.list('things', { limit: 100 })
  })

  bench('db4 type list', async () => {
    const store = await getDB4Store()
    await store.list('things', { limit: 100 })
  })
})

describe('Graph - Pattern Matching (Predicate + Value)', () => {
  bench('graphdb predicate query', async () => {
    const store = await getGraphDBStore()
    await store.query('status:active')
  })

  bench('sdb filter query', async () => {
    const store = await getSDBStore()
    await store.find('things', { status: 'active' }, { limit: 100 })
  })

  bench('db4 filter query', async () => {
    const store = await getDB4Store()
    await store.list('things', { where: { status: 'active' }, limit: 100 })
  })
})

describe('Graph - Triple Retrieval by Subject', () => {
  bench('graphdb get triples by subject', async () => {
    const store = await getGraphDBStore()
    await store.getTriples('thing-0001')
  })

  bench('sdb get document + relationships', async () => {
    const store = await getSDBStore()
    const thing = await store.get('things', 'thing-0001')
    const rels = await store.find('relationships', { subject: 'thing-0001' })
  })

  bench('db4 get document + relationships', async () => {
    const store = await getDB4Store()
    const thing = await store.get('things', 'thing-001')
    const rels = await store.list('relationships', { where: { subject: 'thing-001' } })
  })
})

// ============================================================================
// Batch Read Benchmarks
// ============================================================================

describe('Graph - Batch Entity Read (100 entities)', () => {
  bench('graphdb batch get', async () => {
    const store = await getGraphDBStore()
    const ids = Array.from({ length: 100 }, (_, i) => `thing-${String(i).padStart(4, '0')}`)
    await store.batchGet(ids)
  })

  bench('sdb batch get (parallel)', async () => {
    const store = await getSDBStore()
    const ids = Array.from({ length: 100 }, (_, i) => `thing-${String(i).padStart(4, '0')}`)
    await Promise.all(ids.map((id) => store.get('things', id)))
  })
})

// ============================================================================
// Statistics Benchmarks
// ============================================================================

describe('Graph - Statistics', () => {
  bench('graphdb count', async () => {
    const store = await getGraphDBStore()
    await store.count()
  })

  bench('graphdb stats', async () => {
    const store = await getGraphDBStore()
    await store.getStats()
  })

  bench('sdb count', async () => {
    const store = await getSDBStore()
    await store.count('things')
  })

  bench('db4 count', async () => {
    const store = await getDB4Store()
    await store.count('things')
  })
})

// ============================================================================
// Entity CRUD Benchmarks (comparing graph vs document paradigm)
// ============================================================================

describe('Graph - Entity CRUD', () => {
  bench('graphdb entity insert', async () => {
    const store = await getGraphDBStore()
    const id = `bench-entity-${Date.now()}-${Math.random().toString(36).slice(2)}`
    await store.insert({
      $id: id,
      $type: 'BenchEntity',
      name: 'Benchmark Entity',
      status: 'active',
    })
  })

  bench('sdb entity create', async () => {
    const store = await getSDBStore()
    const id = `bench-entity-${Date.now()}-${Math.random().toString(36).slice(2)}`
    await store.create('things', {
      $id: id,
      name: 'Benchmark Entity',
      status: 'active',
    })
  })

  bench('db4 entity set', async () => {
    const store = await getDB4Store()
    const id = `bench-entity-${Date.now()}-${Math.random().toString(36).slice(2)}`
    await store.set('things', id, {
      name: 'Benchmark Entity',
      status: 'active',
      created_at: new Date().toISOString(),
    })
  })
})

describe('Graph - Entity Read', () => {
  bench('graphdb entity get', async () => {
    const store = await getGraphDBStore()
    await store.get('thing-0001')
  })

  bench('sdb entity get', async () => {
    const store = await getSDBStore()
    await store.get('things', 'thing-0001')
  })

  bench('db4 entity get', async () => {
    const store = await getDB4Store()
    await store.get('things', 'thing-001')
  })
})

describe('Graph - Entity Update', () => {
  bench('graphdb entity update', async () => {
    const store = await getGraphDBStore()
    await store.update('thing-0001', { status: 'updated' })
  })

  bench('sdb entity update', async () => {
    const store = await getSDBStore()
    await store.update('things', 'thing-0001', { status: 'updated' })
  })
})
