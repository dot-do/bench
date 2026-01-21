import { bench, describe, beforeAll } from 'vitest'

/**
 * Hot Cache Benchmarks
 *
 * Measures query latency when everything is cached:
 * - DO SQLite data fully in memory (no disk I/O)
 * - Edge cache populated (Workers KV / Cache API)
 * - Query result memoization active
 *
 * These benchmarks target sub-millisecond operations since all data
 * is already in memory and no I/O is required.
 */

// ============================================================================
// Test Data Setup
// ============================================================================

const TEST_RECORDS = 1000
const TEST_KEY = 'thing-500' // Middle of dataset for realistic lookup

interface Thing {
  id: string
  name: string
  status: string
  value: number
  metadata: Record<string, unknown>
  createdAt: string
}

function generateTestData(): Thing[] {
  return Array.from({ length: TEST_RECORDS }, (_, i) => ({
    id: `thing-${String(i).padStart(3, '0')}`,
    name: `Test Thing ${i}`,
    status: i % 3 === 0 ? 'active' : i % 3 === 1 ? 'pending' : 'archived',
    value: Math.random() * 1000,
    metadata: { index: i, tags: ['test', `batch-${Math.floor(i / 100)}`] },
    createdAt: new Date(Date.now() - i * 60000).toISOString(),
  }))
}

// ============================================================================
// Mock Implementations for Benchmarking
// ============================================================================

/**
 * Simulates a hot SQLite in-memory lookup.
 * In real DO SQLite, this would be data already in SQLite's page cache.
 */
class HotSQLiteStore {
  private data: Map<string, Thing>

  constructor(records: Thing[]) {
    this.data = new Map(records.map((r) => [r.id, r]))
  }

  get(id: string): Thing | undefined {
    return this.data.get(id)
  }

  query(predicate: (t: Thing) => boolean, limit = 100): Thing[] {
    const results: Thing[] = []
    for (const thing of this.data.values()) {
      if (predicate(thing)) {
        results.push(thing)
        if (results.length >= limit) break
      }
    }
    return results
  }

  count(predicate: (t: Thing) => boolean): number {
    let count = 0
    for (const thing of this.data.values()) {
      if (predicate(thing)) count++
    }
    return count
  }
}

/**
 * Simulates Cache API / KV cache hit.
 * Returns pre-serialized JSON response.
 */
class EdgeCache {
  private cache: Map<string, string>

  constructor() {
    this.cache = new Map()
  }

  set(key: string, value: unknown, _ttl?: number): void {
    this.cache.set(key, JSON.stringify(value))
  }

  get(key: string): string | undefined {
    return this.cache.get(key)
  }

  getJSON<T>(key: string): T | undefined {
    const raw = this.cache.get(key)
    return raw ? JSON.parse(raw) : undefined
  }

  has(key: string): boolean {
    return this.cache.has(key)
  }
}

/**
 * LRU Cache implementation for memoization benchmarks.
 */
class LRUCache<K, V> {
  private cache: Map<K, V>
  private readonly maxSize: number

  constructor(maxSize: number) {
    this.cache = new Map()
    this.maxSize = maxSize
  }

  get(key: K): V | undefined {
    const value = this.cache.get(key)
    if (value !== undefined) {
      // Move to end (most recently used)
      this.cache.delete(key)
      this.cache.set(key, value)
    }
    return value
  }

  set(key: K, value: V): void {
    if (this.cache.has(key)) {
      this.cache.delete(key)
    } else if (this.cache.size >= this.maxSize) {
      // Remove oldest (first) entry
      const firstKey = this.cache.keys().next().value
      if (firstKey !== undefined) {
        this.cache.delete(firstKey)
      }
    }
    this.cache.set(key, value)
  }

  has(key: K): boolean {
    return this.cache.has(key)
  }
}

// ============================================================================
// Shared State (pre-warmed in beforeAll)
// ============================================================================

let testData: Thing[]
let db4Store: HotSQLiteStore
let evodbStore: HotSQLiteStore
let postgresStore: HotSQLiteStore
let sqliteStore: HotSQLiteStore
let duckdbStore: HotSQLiteStore
let edgeCache: EdgeCache
let kvCache: EdgeCache
let lruCache: LRUCache<string, Thing>
let weakMapCache: WeakMap<object, Thing>
let requestScopedCache: Map<string, Thing>
let weakMapKeys: Map<string, object>

// ============================================================================
// Hot Cache - DO SQLite In-Memory
// ============================================================================

describe('Hot Cache - DO SQLite In-Memory', () => {
  beforeAll(() => {
    // Pre-warm: Data already read, in SQLite's page cache
    testData = generateTestData()

    // Each database gets its own hot store (simulates real DO isolation)
    db4Store = new HotSQLiteStore(testData)
    evodbStore = new HotSQLiteStore(testData)
    postgresStore = new HotSQLiteStore(testData)
    sqliteStore = new HotSQLiteStore(testData)
    duckdbStore = new HotSQLiteStore(testData)

    // Pre-warm by accessing data multiple times
    for (let i = 0; i < 100; i++) {
      db4Store.get(TEST_KEY)
      evodbStore.get(TEST_KEY)
      postgresStore.get(TEST_KEY)
      sqliteStore.get(TEST_KEY)
      duckdbStore.get(TEST_KEY)
    }
  })

  bench(
    'db4 hot lookup',
    () => {
      db4Store.get(TEST_KEY)
    },
    { warmupIterations: 1000, iterations: 10000 }
  )

  bench(
    'evodb hot lookup',
    () => {
      evodbStore.get(TEST_KEY)
    },
    { warmupIterations: 1000, iterations: 10000 }
  )

  bench(
    'postgres hot lookup',
    () => {
      postgresStore.get(TEST_KEY)
    },
    { warmupIterations: 1000, iterations: 10000 }
  )

  bench(
    'sqlite hot lookup',
    () => {
      sqliteStore.get(TEST_KEY)
    },
    { warmupIterations: 1000, iterations: 10000 }
  )

  bench(
    'duckdb hot lookup',
    () => {
      duckdbStore.get(TEST_KEY)
    },
    { warmupIterations: 1000, iterations: 10000 }
  )

  bench(
    'db4 hot range scan (100 rows)',
    () => {
      db4Store.query((t) => t.status === 'active', 100)
    },
    { warmupIterations: 100, iterations: 1000 }
  )

  bench(
    'sqlite hot range scan (100 rows)',
    () => {
      sqliteStore.query((t) => t.status === 'active', 100)
    },
    { warmupIterations: 100, iterations: 1000 }
  )

  bench(
    'db4 hot count',
    () => {
      db4Store.count((t) => t.status === 'active')
    },
    { warmupIterations: 100, iterations: 1000 }
  )

  bench(
    'sqlite hot count',
    () => {
      sqliteStore.count((t) => t.status === 'active')
    },
    { warmupIterations: 100, iterations: 1000 }
  )
})

// ============================================================================
// Hot Cache - Edge Cache Hit
// ============================================================================

describe('Hot Cache - Edge Cache Hit', () => {
  beforeAll(() => {
    testData = generateTestData()

    // Pre-populate edge caches (simulates Cache API and KV)
    edgeCache = new EdgeCache()
    kvCache = new EdgeCache()

    // Cache individual records
    for (const record of testData) {
      edgeCache.set(`/api/things/${record.id}`, record)
      kvCache.set(`things:${record.id}`, record)
    }

    // Cache list responses
    const activeThings = testData.filter((t) => t.status === 'active').slice(0, 100)
    edgeCache.set('/api/things?status=active&limit=100', { data: activeThings, total: activeThings.length })
    kvCache.set('things:list:active:100', { data: activeThings, total: activeThings.length })

    // Cache aggregation responses
    edgeCache.set('/api/things/count?status=active', { count: testData.filter((t) => t.status === 'active').length })

    // Pre-warm caches
    for (let i = 0; i < 100; i++) {
      edgeCache.get(`/api/things/${TEST_KEY}`)
      kvCache.get(`things:${TEST_KEY}`)
    }
  })

  bench(
    'cache API hit',
    () => {
      // Simulates Cache API match() returning cached Response
      edgeCache.get(`/api/things/${TEST_KEY}`)
    },
    { warmupIterations: 1000, iterations: 10000 }
  )

  bench(
    'cache API hit (with JSON parse)',
    () => {
      // Simulates full cache hit including JSON deserialization
      edgeCache.getJSON(`/api/things/${TEST_KEY}`)
    },
    { warmupIterations: 1000, iterations: 10000 }
  )

  bench(
    'KV cache hit',
    () => {
      // Simulates KV.get() returning cached value
      kvCache.get(`things:${TEST_KEY}`)
    },
    { warmupIterations: 1000, iterations: 10000 }
  )

  bench(
    'KV cache hit (with JSON parse)',
    () => {
      // Simulates KV.get('key', { type: 'json' })
      kvCache.getJSON(`things:${TEST_KEY}`)
    },
    { warmupIterations: 1000, iterations: 10000 }
  )

  bench(
    'stale-while-revalidate pattern (cache hit path)',
    () => {
      // SWR: Return stale immediately, revalidate in background
      // This benchmarks the synchronous cache hit path only
      const cached = edgeCache.get(`/api/things/${TEST_KEY}`)
      const isStale = false // In real impl, check timestamp
      if (cached && !isStale) {
        // Use cached value - this is what we're measuring
        void cached
      }
      // Background revalidation would happen here (not measured)
    },
    { warmupIterations: 1000, iterations: 10000 }
  )

  bench(
    'cache API list response hit',
    () => {
      edgeCache.getJSON('/api/things?status=active&limit=100')
    },
    { warmupIterations: 100, iterations: 1000 }
  )

  bench(
    'cache API aggregation hit',
    () => {
      edgeCache.getJSON('/api/things/count?status=active')
    },
    { warmupIterations: 1000, iterations: 10000 }
  )
})

// ============================================================================
// Hot Cache - Memoization
// ============================================================================

describe('Hot Cache - Memoization', () => {
  beforeAll(() => {
    testData = generateTestData()

    // LRU Cache with 500 entry limit
    lruCache = new LRUCache(500)
    for (const record of testData.slice(0, 500)) {
      lruCache.set(record.id, record)
    }

    // WeakMap memoization (for object-keyed caches)
    weakMapCache = new WeakMap()
    weakMapKeys = new Map()
    for (const record of testData) {
      const key = { id: record.id }
      weakMapKeys.set(record.id, key)
      weakMapCache.set(key, record)
    }

    // Request-scoped cache (simple Map, cleared per request)
    requestScopedCache = new Map()
    for (const record of testData) {
      requestScopedCache.set(record.id, record)
    }

    // Pre-warm
    for (let i = 0; i < 100; i++) {
      lruCache.get(TEST_KEY)
      const weakKey = weakMapKeys.get(TEST_KEY)
      if (weakKey) weakMapCache.get(weakKey)
      requestScopedCache.get(TEST_KEY)
    }
  })

  bench(
    'LRU cache hit',
    () => {
      lruCache.get(TEST_KEY)
    },
    { warmupIterations: 1000, iterations: 10000 }
  )

  bench(
    'LRU cache hit (with existence check)',
    () => {
      if (lruCache.has(TEST_KEY)) {
        lruCache.get(TEST_KEY)
      }
    },
    { warmupIterations: 1000, iterations: 10000 }
  )

  bench(
    'WeakMap memoization',
    () => {
      const key = weakMapKeys.get(TEST_KEY)
      if (key) {
        weakMapCache.get(key)
      }
    },
    { warmupIterations: 1000, iterations: 10000 }
  )

  bench(
    'request-scoped cache',
    () => {
      requestScopedCache.get(TEST_KEY)
    },
    { warmupIterations: 1000, iterations: 10000 }
  )

  bench(
    'request-scoped cache (with existence check)',
    () => {
      if (requestScopedCache.has(TEST_KEY)) {
        requestScopedCache.get(TEST_KEY)
      }
    },
    { warmupIterations: 1000, iterations: 10000 }
  )

  // Memoized function pattern
  const memoizedGet = (() => {
    const cache = new Map<string, Thing>()
    return (id: string): Thing | undefined => {
      if (cache.has(id)) {
        return cache.get(id)
      }
      const value = requestScopedCache.get(id)
      if (value) {
        cache.set(id, value)
      }
      return value
    }
  })()

  // Pre-warm memoized function
  for (let i = 0; i < 100; i++) {
    memoizedGet(TEST_KEY)
  }

  bench(
    'memoized function (hot path)',
    () => {
      memoizedGet(TEST_KEY)
    },
    { warmupIterations: 1000, iterations: 10000 }
  )

  // Computed/derived value memoization
  const derivedCache = new Map<string, { value: number; computed: number }>()
  for (const record of testData) {
    derivedCache.set(record.id, {
      value: record.value,
      computed: record.value * 2 + Math.sqrt(record.value),
    })
  }

  bench(
    'derived value cache hit',
    () => {
      derivedCache.get(TEST_KEY)
    },
    { warmupIterations: 1000, iterations: 10000 }
  )
})

// ============================================================================
// Hot Cache - Combined Patterns
// ============================================================================

describe('Hot Cache - Combined Patterns', () => {
  beforeAll(() => {
    // Ensure all caches are initialized from previous suites
    if (!testData) testData = generateTestData()
    if (!requestScopedCache) {
      requestScopedCache = new Map()
      for (const record of testData) {
        requestScopedCache.set(record.id, record)
      }
    }
    if (!edgeCache) {
      edgeCache = new EdgeCache()
      for (const record of testData) {
        edgeCache.set(`/api/things/${record.id}`, record)
      }
    }
    if (!sqliteStore) {
      sqliteStore = new HotSQLiteStore(testData)
    }
  })

  bench(
    'L1 (memoization) -> hit',
    () => {
      // First check request-scoped cache
      const cached = requestScopedCache.get(TEST_KEY)
      if (cached) void cached // Use cached value
      // Would fall through to L2/L3 on miss
    },
    { warmupIterations: 1000, iterations: 10000 }
  )

  bench(
    'L1 miss -> L2 (edge) -> hit',
    () => {
      // Simulate L1 miss
      const l1Miss = new Map<string, Thing>()
      const cached = l1Miss.get(TEST_KEY)
      if (cached) void cached // Use cached value

      // Fall through to edge cache
      const edgeCached = edgeCache.getJSON<Thing>(`/api/things/${TEST_KEY}`)
      if (edgeCached) {
        l1Miss.set(TEST_KEY, edgeCached)
        void edgeCached // Use cached value
      }
    },
    { warmupIterations: 1000, iterations: 10000 }
  )

  bench(
    'L1 miss -> L2 miss -> L3 (sqlite) -> hit',
    () => {
      // Simulate L1 miss
      const l1Miss = new Map<string, Thing>()
      const l1Cached = l1Miss.get(TEST_KEY)
      if (l1Cached) void l1Cached // Use cached value

      // Simulate L2 miss
      const l2Miss = new EdgeCache()
      const l2Cached = l2Miss.getJSON<Thing>(`/api/things/${TEST_KEY}`)
      if (l2Cached) void l2Cached // Use cached value

      // Fall through to SQLite (hot)
      const dbResult = sqliteStore.get(TEST_KEY)
      if (dbResult) {
        l1Miss.set(TEST_KEY, dbResult)
        void dbResult // Use cached value
      }
    },
    { warmupIterations: 1000, iterations: 10000 }
  )

  bench(
    'cache stampede prevention (single-flight)',
    () => {
      // Simulates single-flight pattern where concurrent requests
      // coalesce into a single cache lookup
      const inflight = new Map<string, Promise<Thing | undefined>>()

      const singleFlight = (key: string): Promise<Thing | undefined> => {
        const existing = inflight.get(key)
        if (existing) return existing

        const promise = Promise.resolve(requestScopedCache.get(key))
        inflight.set(key, promise)

        // In real impl, would delete after resolution
        return promise
      }

      // Measure the cache hit path (not the promise resolution)
      singleFlight(TEST_KEY)
    },
    { warmupIterations: 1000, iterations: 10000 }
  )
})
