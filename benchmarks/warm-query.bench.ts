import { bench, describe, beforeAll } from 'vitest'

/**
 * Warm Query Benchmarks
 *
 * Measures query latency with:
 * - WASM already instantiated
 * - Database initialized
 * - No data cached (first query of this data)
 */

describe('Warm Query - Point Lookup', () => {
  // Single row by primary key

  bench('db4 point lookup', async () => {
    const store = await getWarmDB4Store()
    await store.get('things', 'thing-001')
  })

  bench('evodb point lookup', async () => {
    const store = await getWarmEvoDBStore()
    await store.get('things', 'thing-001')
  })

  bench('postgres point lookup', async () => {
    const store = await getWarmPostgresStore()
    await store.query('SELECT * FROM things WHERE id = $1', ['thing-001'])
  })

  bench('sqlite point lookup', async () => {
    const store = await getWarmSQLiteStore()
    await store.query('SELECT * FROM things WHERE id = ?', ['thing-001'])
  })

  bench('duckdb point lookup', async () => {
    const store = await getWarmDuckDBStore()
    await store.query('SELECT * FROM things WHERE id = ?', ['thing-001'])
  })
})

describe('Warm Query - Range Scan (100 rows)', () => {
  bench('db4 range scan', async () => {
    const store = await getWarmDB4Store()
    await store.list('things', { limit: 100, where: { status: 'active' } })
  })

  bench('evodb range scan', async () => {
    const store = await getWarmEvoDBStore()
    await store.query('things').where('status', '=', 'active').limit(100).all()
  })

  bench('postgres range scan', async () => {
    const store = await getWarmPostgresStore()
    await store.query('SELECT * FROM things WHERE status = $1 LIMIT 100', ['active'])
  })

  bench('sqlite range scan', async () => {
    const store = await getWarmSQLiteStore()
    await store.query('SELECT * FROM things WHERE status = ? LIMIT 100', ['active'])
  })

  bench('duckdb range scan', async () => {
    const store = await getWarmDuckDBStore()
    await store.query('SELECT * FROM things WHERE status = ? LIMIT 100', ['active'])
  })
})

describe('Warm Query - Aggregation (COUNT)', () => {
  bench('db4 count', async () => {
    const store = await getWarmDB4Store()
    await store.count('things', { where: { status: 'active' } })
  })

  bench('evodb count', async () => {
    const store = await getWarmEvoDBStore()
    await store.query('things').where('status', '=', 'active').count()
  })

  bench('postgres count', async () => {
    const store = await getWarmPostgresStore()
    await store.query('SELECT COUNT(*) FROM things WHERE status = $1', ['active'])
  })

  bench('sqlite count', async () => {
    const store = await getWarmSQLiteStore()
    await store.query('SELECT COUNT(*) FROM things WHERE status = ?', ['active'])
  })

  bench('duckdb count', async () => {
    const store = await getWarmDuckDBStore()
    await store.query('SELECT COUNT(*) FROM things WHERE status = ?', ['active'])
  })
})

describe('Warm Query - Join (2 tables)', () => {
  bench('db4 join', async () => {
    const store = await getWarmDB4Store()
    await store.query({
      from: 'things',
      join: { table: 'relationships', on: 'things.id = relationships.subject' },
      limit: 100
    })
  })

  bench('evodb join', async () => {
    const store = await getWarmEvoDBStore()
    await store.query('things')
      .join('relationships', 'things.id', 'relationships.subject')
      .limit(100)
      .all()
  })

  bench('postgres join', async () => {
    const store = await getWarmPostgresStore()
    await store.query(`
      SELECT t.*, r.predicate, r.object
      FROM things t
      JOIN relationships r ON t.id = r.subject
      LIMIT 100
    `)
  })

  bench('sqlite join', async () => {
    const store = await getWarmSQLiteStore()
    await store.query(`
      SELECT t.*, r.predicate, r.object
      FROM things t
      JOIN relationships r ON t.id = r.subject
      LIMIT 100
    `)
  })

  bench('duckdb join', async () => {
    const store = await getWarmDuckDBStore()
    await store.query(`
      SELECT t.*, r.predicate, r.object
      FROM things t
      JOIN relationships r ON t.id = r.subject
      LIMIT 100
    `)
  })
})

// Cached store instances for warm benchmarks
let db4Store: any, evodbStore: any, postgresStore: any, sqliteStore: any, duckdbStore: any

async function getWarmDB4Store() {
  if (!db4Store) {
    const { createDB4Store, seedTestData } = await import('../databases/db4')
    db4Store = await createDB4Store()
    await seedTestData(db4Store)
  }
  return db4Store
}

async function getWarmEvoDBStore() {
  if (!evodbStore) {
    const { createEvoDBStore, seedTestData } = await import('../databases/evodb')
    evodbStore = await createEvoDBStore()
    await seedTestData(evodbStore)
  }
  return evodbStore
}

async function getWarmPostgresStore() {
  if (!postgresStore) {
    const { createPostgresStore, seedTestData } = await import('../databases/postgres')
    postgresStore = await createPostgresStore()
    await seedTestData(postgresStore)
  }
  return postgresStore
}

async function getWarmSQLiteStore() {
  if (!sqliteStore) {
    const { createSQLiteStore, seedTestData } = await import('../databases/sqlite')
    sqliteStore = await createSQLiteStore()
    await seedTestData(sqliteStore)
  }
  return sqliteStore
}

async function getWarmDuckDBStore() {
  if (!duckdbStore) {
    const { createDuckDBStore, seedTestData } = await import('../databases/duckdb')
    duckdbStore = await createDuckDBStore()
    await seedTestData(duckdbStore)
  }
  return duckdbStore
}
