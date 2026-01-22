import { bench, describe, beforeAll, afterAll } from 'vitest'
import type { MongDB4Store } from './mongo-db4'
import type { MongoPostgresStore } from './mongo-postgres'
import type { MongoClickHouseStore } from './mongo-clickhouse'

/**
 * MongoDB Adapter Benchmarks
 *
 * Compares performance between:
 * - @db4/mongo: Pure TypeScript, zero WASM
 * - @dotdo/mongodb: PostgreSQL/DocumentDB backend (PGLite WASM)
 * - mongo-clickhouse: ClickHouse backend for analytics workloads
 *
 * Tests cover:
 * - Point lookup performance
 * - Range queries
 * - Aggregation pipeline
 * - Insert throughput
 * - Update operations
 */

// Cached store instances for warm benchmarks
let mongoDB4Store: MongDB4Store | null = null
let mongoPostgresStore: MongoPostgresStore | null = null
let mongoClickHouseStore: MongoClickHouseStore | null = null

async function getWarmMongoDB4Store(): Promise<MongDB4Store> {
  if (!mongoDB4Store) {
    const { createMongoDB4Store, seedTestData } = await import('./mongo-db4')
    mongoDB4Store = await createMongoDB4Store()
    await seedTestData(mongoDB4Store, 'medium')
  }
  return mongoDB4Store
}

async function getWarmMongoPostgresStore(): Promise<MongoPostgresStore> {
  if (!mongoPostgresStore) {
    const { createMongoPostgresStore, seedTestData } = await import('./mongo-postgres')
    mongoPostgresStore = await createMongoPostgresStore()
    await seedTestData(mongoPostgresStore, 'medium')
  }
  return mongoPostgresStore
}

async function getWarmMongoClickHouseStore(): Promise<MongoClickHouseStore> {
  if (!mongoClickHouseStore) {
    const { createMongoClickHouseStore, seedTestData } = await import('./mongo-clickhouse')
    mongoClickHouseStore = await createMongoClickHouseStore()
    await seedTestData(mongoClickHouseStore, 'medium')
  }
  return mongoClickHouseStore
}

// ============================================================================
// Cold Start Benchmarks
// ============================================================================

describe('MongoDB Cold Start', () => {
  bench('@db4/mongo cold start', async () => {
    const { createMongoDB4Store } = await import('./mongo-db4')
    const store = await createMongoDB4Store()
    await store.things.findOne({ _id: 'thing-000001' })
    await store.close()
  }, { iterations: 10, warmupIterations: 0 })

  bench('@dotdo/mongodb cold start (PGLite WASM)', async () => {
    const { createMongoPostgresStore } = await import('./mongo-postgres')
    const store = await createMongoPostgresStore()
    await store.things.findOne({ _id: 'thing-000001' })
    await store.close()
  }, { iterations: 10, warmupIterations: 0 })

  bench('mongo-clickhouse cold start (ClickHouse)', async () => {
    const { createMongoClickHouseStore } = await import('./mongo-clickhouse')
    const store = await createMongoClickHouseStore()
    await store.things.findOne({ _id: 'thing-000001' })
    await store.close()
  }, { iterations: 10, warmupIterations: 0 })
})

// ============================================================================
// Point Lookup Benchmarks
// ============================================================================

describe('MongoDB Point Lookup', () => {
  bench('@db4/mongo findOne by _id', async () => {
    const store = await getWarmMongoDB4Store()
    await store.things.findOne({ _id: 'thing-000500' })
  })

  bench('@dotdo/mongodb findOne by _id', async () => {
    const store = await getWarmMongoPostgresStore()
    await store.things.findOne({ _id: 'thing-000500' })
  })

  bench('mongo-clickhouse findOne by _id', async () => {
    const store = await getWarmMongoClickHouseStore()
    await store.things.findOne({ _id: 'thing-000500' })
  })

  bench('@db4/mongo findOne by indexed field', async () => {
    const store = await getWarmMongoDB4Store()
    await store.things.findOne({ status: 'active', category: 'electronics' })
  })

  bench('@dotdo/mongodb findOne by indexed field', async () => {
    const store = await getWarmMongoPostgresStore()
    await store.things.findOne({ status: 'active', category: 'electronics' })
  })

  bench('mongo-clickhouse findOne by indexed field', async () => {
    const store = await getWarmMongoClickHouseStore()
    await store.things.findOne({ status: 'active', category: 'electronics' })
  })
})

// ============================================================================
// Range Query Benchmarks
// ============================================================================

describe('MongoDB Range Queries', () => {
  bench('@db4/mongo find with filter (100 docs)', async () => {
    const store = await getWarmMongoDB4Store()
    await store.things.find({ status: 'active' }).limit(100).toArray()
  })

  bench('@dotdo/mongodb find with filter (100 docs)', async () => {
    const store = await getWarmMongoPostgresStore()
    await store.things.find({ status: 'active' }).limit(100).toArray()
  })

  bench('mongo-clickhouse find with filter (100 docs)', async () => {
    const store = await getWarmMongoClickHouseStore()
    await store.things.find({ status: 'active' }).limit(100).toArray()
  })

  bench('@db4/mongo find with multiple conditions', async () => {
    const store = await getWarmMongoDB4Store()
    await store.things.find({
      status: 'active',
      category: 'electronics',
      'metadata.priority': { $gte: 5 },
    }).limit(50).toArray()
  })

  bench('@dotdo/mongodb find with multiple conditions', async () => {
    const store = await getWarmMongoPostgresStore()
    await store.things.find({
      status: 'active',
      category: 'electronics',
      'metadata.priority': { $gte: 5 },
    }).limit(50).toArray()
  })

  bench('mongo-clickhouse find with multiple conditions', async () => {
    const store = await getWarmMongoClickHouseStore()
    await store.things.find({
      status: 'active',
      category: 'electronics',
      'metadata.priority': { $gte: 5 },
    }).limit(50).toArray()
  })

  bench('@db4/mongo find with $in operator', async () => {
    const store = await getWarmMongoDB4Store()
    await store.things.find({
      status: { $in: ['active', 'pending'] },
    }).limit(100).toArray()
  })

  bench('@dotdo/mongodb find with $in operator', async () => {
    const store = await getWarmMongoPostgresStore()
    await store.things.find({
      status: { $in: ['active', 'pending'] },
    }).limit(100).toArray()
  })

  bench('mongo-clickhouse find with $in operator', async () => {
    const store = await getWarmMongoClickHouseStore()
    await store.things.find({
      status: { $in: ['active', 'pending'] },
    }).limit(100).toArray()
  })

  bench('@db4/mongo find with sort', async () => {
    const store = await getWarmMongoDB4Store()
    await store.things.find({ status: 'active' })
      .sort({ created_at: -1 })
      .limit(50)
      .toArray()
  })

  bench('@dotdo/mongodb find with sort', async () => {
    const store = await getWarmMongoPostgresStore()
    await store.things.find({ status: 'active' })
      .sort({ created_at: -1 })
      .limit(50)
      .toArray()
  })

  bench('mongo-clickhouse find with sort', async () => {
    const store = await getWarmMongoClickHouseStore()
    await store.things.find({ status: 'active' })
      .sort({ created_at: -1 })
      .limit(50)
      .toArray()
  })
})

// ============================================================================
// Aggregation Pipeline Benchmarks
// ============================================================================

describe('MongoDB Aggregation Pipeline', () => {
  bench('@db4/mongo count documents', async () => {
    const store = await getWarmMongoDB4Store()
    await store.things.countDocuments({ status: 'active' })
  })

  bench('@dotdo/mongodb count documents', async () => {
    const store = await getWarmMongoPostgresStore()
    await store.things.countDocuments({ status: 'active' })
  })

  bench('mongo-clickhouse count documents', async () => {
    const store = await getWarmMongoClickHouseStore()
    await store.things.countDocuments({ status: 'active' })
  })

  bench('@db4/mongo aggregate $group by status', async () => {
    const store = await getWarmMongoDB4Store()
    await store.things.aggregate([
      { $group: { _id: '$status', count: { $sum: 1 } } },
    ]).toArray()
  })

  bench('@dotdo/mongodb aggregate $group by status', async () => {
    const store = await getWarmMongoPostgresStore()
    await store.things.aggregate([
      { $group: { _id: '$status', count: { $sum: 1 } } },
    ]).toArray()
  })

  bench('mongo-clickhouse aggregate $group by status', async () => {
    const store = await getWarmMongoClickHouseStore()
    await store.things.aggregate([
      { $group: { _id: '$status', count: { $sum: 1 } } },
    ]).toArray()
  })

  bench('@db4/mongo aggregate $match + $group', async () => {
    const store = await getWarmMongoDB4Store()
    await store.things.aggregate([
      { $match: { status: 'active' } },
      { $group: { _id: '$category', count: { $sum: 1 }, avgScore: { $avg: '$metadata.score' } } },
    ]).toArray()
  })

  bench('@dotdo/mongodb aggregate $match + $group', async () => {
    const store = await getWarmMongoPostgresStore()
    await store.things.aggregate([
      { $match: { status: 'active' } },
      { $group: { _id: '$category', count: { $sum: 1 }, avgScore: { $avg: '$metadata.score' } } },
    ]).toArray()
  })

  bench('mongo-clickhouse aggregate $match + $group', async () => {
    const store = await getWarmMongoClickHouseStore()
    await store.things.aggregate([
      { $match: { status: 'active' } },
      { $group: { _id: '$category', count: { $sum: 1 }, avgScore: { $avg: '$metadata.score' } } },
    ]).toArray()
  })

  bench('@db4/mongo aggregate $lookup (join)', async () => {
    const store = await getWarmMongoDB4Store()
    await store.things.aggregate([
      { $match: { status: 'active' } },
      { $limit: 50 },
      {
        $lookup: {
          from: 'relationships',
          localField: '_id',
          foreignField: 'subject',
          as: 'relations',
        },
      },
    ]).toArray()
  })

  bench('@dotdo/mongodb aggregate $lookup (join)', async () => {
    const store = await getWarmMongoPostgresStore()
    await store.things.aggregate([
      { $match: { status: 'active' } },
      { $limit: 50 },
      {
        $lookup: {
          from: 'relationships',
          localField: '_id',
          foreignField: 'subject',
          as: 'relations',
        },
      },
    ]).toArray()
  })

  bench('mongo-clickhouse aggregate $lookup (join)', async () => {
    const store = await getWarmMongoClickHouseStore()
    // Note: ClickHouse $lookup has limitations compared to MongoDB
    await store.things.aggregate([
      { $match: { status: 'active' } },
      { $limit: 50 },
      {
        $lookup: {
          from: 'relationships',
          localField: '_id',
          foreignField: 'subject',
          as: 'relations',
        },
      },
    ]).toArray()
  })

  bench('@db4/mongo aggregate complex pipeline', async () => {
    const store = await getWarmMongoDB4Store()
    await store.things.aggregate([
      { $match: { status: { $in: ['active', 'pending'] } } },
      { $addFields: { priorityLevel: { $cond: [{ $gte: ['$metadata.priority', 7] }, 'high', 'normal'] } } },
      { $group: { _id: { category: '$category', level: '$priorityLevel' }, count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 10 },
    ]).toArray()
  })

  bench('@dotdo/mongodb aggregate complex pipeline', async () => {
    const store = await getWarmMongoPostgresStore()
    await store.things.aggregate([
      { $match: { status: { $in: ['active', 'pending'] } } },
      { $addFields: { priorityLevel: { $cond: [{ $gte: ['$metadata.priority', 7] }, 'high', 'normal'] } } },
      { $group: { _id: { category: '$category', level: '$priorityLevel' }, count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 10 },
    ]).toArray()
  })

  bench('mongo-clickhouse aggregate complex pipeline', async () => {
    const store = await getWarmMongoClickHouseStore()
    // ClickHouse excels at complex analytical queries
    await store.things.aggregate([
      { $match: { status: { $in: ['active', 'pending'] } } },
      { $group: { _id: '$category', count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 10 },
    ]).toArray()
  })
})

// ============================================================================
// Insert Throughput Benchmarks
// ============================================================================

describe('MongoDB Insert Throughput', () => {
  let insertCounter = 0

  bench('@db4/mongo insertOne', async () => {
    const store = await getWarmMongoDB4Store()
    insertCounter++
    await store.things.insertOne({
      _id: `bench-insert-db4-${insertCounter}`,
      name: `Bench Insert ${insertCounter}`,
      status: 'pending',
      category: 'test',
      created_at: new Date(),
    })
  })

  bench('@dotdo/mongodb insertOne', async () => {
    const store = await getWarmMongoPostgresStore()
    insertCounter++
    await store.things.insertOne({
      _id: `bench-insert-pg-${insertCounter}`,
      name: `Bench Insert ${insertCounter}`,
      status: 'pending',
      category: 'test',
      created_at: new Date(),
    })
  })

  bench('mongo-clickhouse insertOne', async () => {
    const store = await getWarmMongoClickHouseStore()
    insertCounter++
    await store.things.insertOne({
      _id: `bench-insert-ch-${insertCounter}`,
      name: `Bench Insert ${insertCounter}`,
      status: 'pending',
      category: 'test',
      created_at: new Date(),
    })
  })

  bench('@db4/mongo insertMany (100 docs)', async () => {
    const store = await getWarmMongoDB4Store()
    const docs = Array.from({ length: 100 }, (_, i) => ({
      _id: `bench-batch-db4-${insertCounter++}`,
      name: `Batch Insert ${i}`,
      status: 'pending' as const,
      category: 'test',
      created_at: new Date(),
    }))
    await store.things.insertMany(docs)
  }, { iterations: 50 })

  bench('@dotdo/mongodb insertMany (100 docs)', async () => {
    const store = await getWarmMongoPostgresStore()
    const docs = Array.from({ length: 100 }, (_, i) => ({
      _id: `bench-batch-pg-${insertCounter++}`,
      name: `Batch Insert ${i}`,
      status: 'pending' as const,
      category: 'test',
      created_at: new Date(),
    }))
    await store.things.insertMany(docs)
  }, { iterations: 50 })

  bench('mongo-clickhouse insertMany (100 docs)', async () => {
    const store = await getWarmMongoClickHouseStore()
    const docs = Array.from({ length: 100 }, (_, i) => ({
      _id: `bench-batch-ch-${insertCounter++}`,
      name: `Batch Insert ${i}`,
      status: 'pending' as const,
      category: 'test',
      created_at: new Date(),
    }))
    await store.things.insertMany(docs)
  }, { iterations: 50 })

  // ClickHouse-specific: large batch insert (ClickHouse excels at bulk loads)
  bench('mongo-clickhouse insertMany (1000 docs)', async () => {
    const store = await getWarmMongoClickHouseStore()
    const docs = Array.from({ length: 1000 }, (_, i) => ({
      _id: `bench-bulk-ch-${insertCounter++}`,
      name: `Bulk Insert ${i}`,
      status: 'pending' as const,
      category: 'test',
      created_at: new Date(),
    }))
    await store.things.insertMany(docs)
  }, { iterations: 20 })
})

// ============================================================================
// Update Operations Benchmarks
// ============================================================================

describe('MongoDB Update Operations', () => {
  bench('@db4/mongo updateOne', async () => {
    const store = await getWarmMongoDB4Store()
    await store.things.updateOne(
      { _id: 'thing-000100' },
      { $set: { updated_at: new Date() } }
    )
  })

  bench('@dotdo/mongodb updateOne', async () => {
    const store = await getWarmMongoPostgresStore()
    await store.things.updateOne(
      { _id: 'thing-000100' },
      { $set: { updated_at: new Date() } }
    )
  })

  bench('mongo-clickhouse updateOne', async () => {
    const store = await getWarmMongoClickHouseStore()
    // Note: ClickHouse mutations are async, not ideal for OLTP
    await store.things.updateOne(
      { _id: 'thing-000100' },
      { $set: { updated_at: new Date() } }
    )
  })

  bench('@db4/mongo updateOne with $inc', async () => {
    const store = await getWarmMongoDB4Store()
    await store.things.updateOne(
      { _id: 'thing-000100' },
      { $inc: { 'metadata.priority': 1 } }
    )
  })

  bench('@dotdo/mongodb updateOne with $inc', async () => {
    const store = await getWarmMongoPostgresStore()
    await store.things.updateOne(
      { _id: 'thing-000100' },
      { $inc: { 'metadata.priority': 1 } }
    )
  })

  bench('@db4/mongo updateMany', async () => {
    const store = await getWarmMongoDB4Store()
    await store.things.updateMany(
      { status: 'pending', category: 'test' },
      { $set: { status: 'active' } }
    )
  })

  bench('@dotdo/mongodb updateMany', async () => {
    const store = await getWarmMongoPostgresStore()
    await store.things.updateMany(
      { status: 'pending', category: 'test' },
      { $set: { status: 'active' } }
    )
  })

  bench('mongo-clickhouse updateMany', async () => {
    const store = await getWarmMongoClickHouseStore()
    await store.things.updateMany(
      { status: 'pending', category: 'test' },
      { $set: { status: 'active' } }
    )
  })

  bench('@db4/mongo findOneAndUpdate', async () => {
    const store = await getWarmMongoDB4Store()
    await store.things.findOneAndUpdate(
      { _id: 'thing-000200' },
      { $set: { updated_at: new Date() } },
      { returnDocument: 'after' }
    )
  })

  bench('@dotdo/mongodb findOneAndUpdate', async () => {
    const store = await getWarmMongoPostgresStore()
    await store.things.findOneAndUpdate(
      { _id: 'thing-000200' },
      { $set: { updated_at: new Date() } },
      { returnDocument: 'after' }
    )
  })

  bench('mongo-clickhouse findOneAndUpdate', async () => {
    const store = await getWarmMongoClickHouseStore()
    await store.things.findOneAndUpdate(
      { _id: 'thing-000200' },
      { $set: { updated_at: new Date() } },
      { returnDocument: 'after' }
    )
  })
})

// ============================================================================
// Delete Operations Benchmarks
// ============================================================================

describe('MongoDB Delete Operations', () => {
  let deleteCounter = 0

  bench('@db4/mongo deleteOne', async () => {
    const store = await getWarmMongoDB4Store()
    // Insert then delete to ensure we have something to delete
    const id = `bench-delete-db4-${deleteCounter++}`
    await store.things.insertOne({
      _id: id,
      name: 'To Delete',
      status: 'pending',
      created_at: new Date(),
    })
    await store.things.deleteOne({ _id: id })
  })

  bench('@dotdo/mongodb deleteOne', async () => {
    const store = await getWarmMongoPostgresStore()
    const id = `bench-delete-pg-${deleteCounter++}`
    await store.things.insertOne({
      _id: id,
      name: 'To Delete',
      status: 'pending',
      created_at: new Date(),
    })
    await store.things.deleteOne({ _id: id })
  })

  bench('mongo-clickhouse deleteOne', async () => {
    const store = await getWarmMongoClickHouseStore()
    // Note: ClickHouse DELETE is async mutation, not ideal for OLTP
    const id = `bench-delete-ch-${deleteCounter++}`
    await store.things.insertOne({
      _id: id,
      name: 'To Delete',
      status: 'pending',
      created_at: new Date(),
    })
    await store.things.deleteOne({ _id: id })
  })
})

// ============================================================================
// Projection and Field Selection Benchmarks
// ============================================================================

describe('MongoDB Projection', () => {
  bench('@db4/mongo find with projection', async () => {
    const store = await getWarmMongoDB4Store()
    await store.things.find(
      { status: 'active' },
      { projection: { _id: 1, name: 1, status: 1 } }
    ).limit(100).toArray()
  })

  bench('@dotdo/mongodb find with projection', async () => {
    const store = await getWarmMongoPostgresStore()
    await store.things.find(
      { status: 'active' },
      { projection: { _id: 1, name: 1, status: 1 } }
    ).limit(100).toArray()
  })

  bench('mongo-clickhouse find with projection', async () => {
    const store = await getWarmMongoClickHouseStore()
    // ClickHouse columnar storage makes projections very efficient
    await store.things.find(
      { status: 'active' },
      { projection: { _id: 1, name: 1, status: 1 } }
    ).limit(100).toArray()
  })

  bench('@db4/mongo find with exclusion projection', async () => {
    const store = await getWarmMongoDB4Store()
    await store.things.find(
      { status: 'active' },
      { projection: { metadata: 0, tags: 0 } }
    ).limit(100).toArray()
  })

  bench('@dotdo/mongodb find with exclusion projection', async () => {
    const store = await getWarmMongoPostgresStore()
    await store.things.find(
      { status: 'active' },
      { projection: { metadata: 0, tags: 0 } }
    ).limit(100).toArray()
  })

  bench('mongo-clickhouse find with exclusion projection', async () => {
    const store = await getWarmMongoClickHouseStore()
    await store.things.find(
      { status: 'active' },
      { projection: { metadata: 0, tags: 0 } }
    ).limit(100).toArray()
  })
})

// ============================================================================
// Distinct and Estimated Count Benchmarks
// ============================================================================

describe('MongoDB Distinct and Count', () => {
  bench('@db4/mongo distinct', async () => {
    const store = await getWarmMongoDB4Store()
    await store.things.distinct('category', { status: 'active' })
  })

  bench('@dotdo/mongodb distinct', async () => {
    const store = await getWarmMongoPostgresStore()
    await store.things.distinct('category', { status: 'active' })
  })

  bench('mongo-clickhouse distinct', async () => {
    const store = await getWarmMongoClickHouseStore()
    await store.things.distinct('category', { status: 'active' })
  })

  bench('@db4/mongo estimatedDocumentCount', async () => {
    const store = await getWarmMongoDB4Store()
    await store.things.estimatedDocumentCount()
  })

  bench('@dotdo/mongodb estimatedDocumentCount', async () => {
    const store = await getWarmMongoPostgresStore()
    await store.things.estimatedDocumentCount()
  })

  bench('mongo-clickhouse estimatedDocumentCount', async () => {
    const store = await getWarmMongoClickHouseStore()
    await store.things.estimatedDocumentCount()
  })
})

// ============================================================================
// ClickHouse-Specific Analytics Benchmarks
// ============================================================================

describe('MongoDB ClickHouse Analytics (OLAP)', () => {
  bench('mongo-clickhouse raw SQL aggregation', async () => {
    const store = await getWarmMongoClickHouseStore()
    // Direct SQL access for complex analytics
    await store.query(`
      SELECT status, count() as cnt, avg(length(name)) as avg_name_len
      FROM things
      GROUP BY status
      ORDER BY cnt DESC
    `)
  })

  bench('mongo-clickhouse large scan (all records)', async () => {
    const store = await getWarmMongoClickHouseStore()
    // ClickHouse excels at scanning large datasets
    await store.things.find({}).limit(1000).toArray()
  })

  bench('mongo-clickhouse multi-column aggregation', async () => {
    const store = await getWarmMongoClickHouseStore()
    await store.things.aggregate([
      { $group: { _id: '$status', count: { $sum: 1 } } },
    ]).toArray()
  })
})
