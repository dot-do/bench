import { bench, describe, beforeAll, afterAll } from 'vitest'

/**
 * Worker vs Durable Object Benchmarks
 *
 * Measures the overhead of:
 * 1. DO routing (Worker -> DO RPC call)
 * 2. DO state access vs stateless Worker
 * 3. DO alarm scheduling overhead
 * 4. DO WebSocket accept overhead
 * 5. DO storage.get vs sql.exec
 *
 * These benchmarks help understand when to use Workers vs DOs
 * and the cost of DO-specific features.
 */

// =============================================================================
// Mock Types and Utilities
// =============================================================================

interface Thing {
  $id: string
  $type: string
  name: string
  status: string
  createdAt: string
  data?: Record<string, unknown>
}

interface MockDOStorage {
  get(key: string): Promise<unknown>
  put(key: string, value: unknown): Promise<void>
  delete(key: string): Promise<boolean>
  list(options?: { prefix?: string; limit?: number }): Promise<Map<string, unknown>>
  transaction<T>(fn: () => Promise<T>): Promise<T>
}

interface MockSQLStorage {
  exec(query: string, ...params: unknown[]): { results: unknown[] }
}

interface MockDOState {
  id: { toString(): string }
  storage: MockDOStorage
  blockConcurrencyWhile<T>(fn: () => Promise<T>): Promise<T>
  waitUntil(promise: Promise<unknown>): void
}

interface MockAlarmScheduler {
  getAlarm(): Promise<number | null>
  setAlarm(time: number): Promise<void>
  deleteAlarm(): Promise<void>
}

// =============================================================================
// Test Data Generators
// =============================================================================

function generateThing(id: string): Thing {
  return {
    $id: id,
    $type: 'Customer',
    name: `Customer ${id}`,
    status: 'active',
    createdAt: new Date().toISOString(),
    data: { tier: 'premium', region: 'us-west' }
  }
}

function generateThings(count: number): Thing[] {
  return Array.from({ length: count }, (_, i) => generateThing(`thing-${String(i).padStart(5, '0')}`))
}

// =============================================================================
// Mock Implementations
// =============================================================================

/**
 * Simulates a stateless Worker handler
 * No persistence, just in-memory operations
 */
class MockWorkerHandler {
  private cache = new Map<string, Thing>()

  async pointLookup(id: string): Promise<Thing | undefined> {
    return this.cache.get(id)
  }

  async write(thing: Thing): Promise<void> {
    this.cache.set(thing.$id, thing)
  }

  async batchWrite(things: Thing[]): Promise<void> {
    for (const thing of things) {
      this.cache.set(thing.$id, thing)
    }
  }

  preload(things: Thing[]): void {
    for (const thing of things) {
      this.cache.set(thing.$id, thing)
    }
  }

  directCall(data: unknown): unknown {
    // Simulate direct function call (no RPC overhead)
    return { processed: true, data }
  }
}

/**
 * Simulates DO storage layer with realistic SQLite-backed storage
 */
class MockDOStorageImpl implements MockDOStorage {
  private kv = new Map<string, unknown>()
  private writeLatencyMs = 0.05 // ~50 microseconds for SQLite write

  async get(key: string): Promise<unknown> {
    return this.kv.get(key)
  }

  async put(key: string, value: unknown): Promise<void> {
    // Simulate SQLite write latency
    await this.simulateLatency(this.writeLatencyMs)
    this.kv.set(key, value)
  }

  async delete(key: string): Promise<boolean> {
    return this.kv.delete(key)
  }

  async list(options?: { prefix?: string; limit?: number }): Promise<Map<string, unknown>> {
    const result = new Map<string, unknown>()
    let count = 0
    for (const [key, value] of this.kv) {
      if (options?.prefix && !key.startsWith(options.prefix)) continue
      if (options?.limit && count >= options.limit) break
      result.set(key, value)
      count++
    }
    return result
  }

  async transaction<T>(fn: () => Promise<T>): Promise<T> {
    // Simulate transaction overhead
    await this.simulateLatency(0.01)
    const result = await fn()
    await this.simulateLatency(0.01)
    return result
  }

  private simulateLatency(ms: number): Promise<void> {
    // Use microtask for sub-millisecond "latency"
    return new Promise(resolve => {
      const iterations = Math.floor(ms * 1000)
      for (let i = 0; i < iterations; i++) {
        // Busy-wait to simulate I/O latency
        Math.random()
      }
      resolve()
    })
  }

  preload(things: Thing[]): void {
    for (const thing of things) {
      this.kv.set(`thing:${thing.$id}`, thing)
    }
  }
}

/**
 * Simulates DO SQL storage (SQLite via sql.exec)
 */
class MockSQLStorageImpl implements MockSQLStorage {
  private data: Thing[] = []

  exec(query: string, ...params: unknown[]): { results: unknown[] } {
    // Simulate basic SQL operations
    if (query.includes('SELECT')) {
      if (query.includes('WHERE')) {
        const id = params[0] as string
        const found = this.data.find(t => t.$id === id)
        return { results: found ? [found] : [] }
      }
      const limit = query.match(/LIMIT (\d+)/)?.[1]
      return { results: this.data.slice(0, limit ? parseInt(limit) : 100) }
    }
    if (query.includes('INSERT')) {
      this.data.push(params[0] as Thing)
      return { results: [] }
    }
    return { results: [] }
  }

  preload(things: Thing[]): void {
    this.data = [...things]
  }
}

/**
 * Simulates Durable Object with storage and features
 */
class MockDO {
  state: MockDOState
  storage: MockDOStorageImpl
  sql: MockSQLStorageImpl
  alarmScheduled: number | null = null
  private websocketLatencyMs = 0.02

  constructor() {
    this.storage = new MockDOStorageImpl()
    this.sql = new MockSQLStorageImpl()
    this.state = {
      id: { toString: () => 'mock-do-id' },
      storage: this.storage,
      blockConcurrencyWhile: async <T>(fn: () => Promise<T>) => fn(),
      waitUntil: (_promise: Promise<unknown>) => {}
    }
  }

  async pointLookup(id: string): Promise<Thing | undefined> {
    return await this.storage.get(`thing:${id}`) as Thing | undefined
  }

  async write(thing: Thing): Promise<void> {
    await this.storage.put(`thing:${thing.$id}`, thing)
  }

  async batchWrite(things: Thing[]): Promise<void> {
    await this.storage.transaction(async () => {
      for (const thing of things) {
        await this.storage.put(`thing:${thing.$id}`, thing)
      }
    })
  }

  // SQL-based operations
  sqlPointLookup(id: string): Thing | undefined {
    const result = this.sql.exec('SELECT * FROM things WHERE id = ?', id)
    return result.results[0] as Thing | undefined
  }

  // Alarm scheduling
  async scheduleAlarm(delayMs: number): Promise<void> {
    this.alarmScheduled = Date.now() + delayMs
    // Simulate alarm API call overhead
    await new Promise(resolve => setTimeout(resolve, 0))
  }

  async cancelAlarm(): Promise<void> {
    this.alarmScheduled = null
    await new Promise(resolve => setTimeout(resolve, 0))
  }

  async getAlarm(): Promise<number | null> {
    return this.alarmScheduled
  }

  // WebSocket accept
  async acceptWebSocket(_request: Request): Promise<{ accepted: boolean }> {
    // Simulate WebSocket accept overhead
    await this.simulateLatency(this.websocketLatencyMs)
    return { accepted: true }
  }

  private simulateLatency(ms: number): Promise<void> {
    return new Promise(resolve => {
      const iterations = Math.floor(ms * 1000)
      for (let i = 0; i < iterations; i++) {
        Math.random()
      }
      resolve()
    })
  }

  preload(things: Thing[]): void {
    this.storage.preload(things)
    this.sql.preload(things)
  }
}

/**
 * Simulates RPC overhead when calling Worker -> DO
 * This includes:
 * - Serialization of request
 * - Network hop (minimal in same colo)
 * - DO stub lookup
 * - Deserialization of response
 */
class MockDOStub {
  private do: MockDO
  private rpcOverheadMs = 0.1 // ~100 microseconds for same-colo RPC

  constructor(doInstance: MockDO) {
    this.do = doInstance
  }

  async fetch(request: Request): Promise<Response> {
    // Simulate RPC overhead
    await this.simulateRpcOverhead()

    // Parse URL and route
    const url = new URL(request.url)
    const path = url.pathname

    if (path.startsWith('/things/')) {
      const id = path.split('/')[2]
      const thing = await this.do.pointLookup(id)
      return new Response(JSON.stringify(thing), {
        headers: { 'content-type': 'application/json' }
      })
    }

    return new Response('Not found', { status: 404 })
  }

  // Direct RPC method calls (no HTTP overhead)
  async rpc<T>(method: string, ...args: unknown[]): Promise<T> {
    await this.simulateRpcOverhead()
    const fn = (this.do as unknown as Record<string, (...args: unknown[]) => Promise<T>>)[method]
    if (!fn) throw new Error(`Method ${method} not found`)
    return fn.apply(this.do, args)
  }

  private simulateRpcOverhead(): Promise<void> {
    return new Promise(resolve => {
      const iterations = Math.floor(this.rpcOverheadMs * 1000)
      for (let i = 0; i < iterations; i++) {
        Math.random()
      }
      resolve()
    })
  }
}

// =============================================================================
// Benchmark Setup
// =============================================================================

let workerHandler: MockWorkerHandler
let doInstance: MockDO
let doStub: MockDOStub
let testThings: Thing[]

beforeAll(() => {
  // Generate test data
  testThings = generateThings(1000)

  // Initialize handlers
  workerHandler = new MockWorkerHandler()
  workerHandler.preload(testThings)

  doInstance = new MockDO()
  doInstance.preload(testThings)

  doStub = new MockDOStub(doInstance)
})

afterAll(() => {
  // Cleanup
})

// =============================================================================
// Read Operation Benchmarks
// =============================================================================

describe('Worker vs DO - Read Operations', () => {
  /**
   * Worker point lookup: Direct in-memory Map access
   * Baseline for comparison - no persistence, no RPC
   */
  bench('worker point lookup (in-memory)', async () => {
    await workerHandler.pointLookup('thing-00500')
  })

  /**
   * DO point lookup via direct storage access
   * Includes KV-style storage lookup overhead
   */
  bench('DO point lookup (storage.get)', async () => {
    await doInstance.pointLookup('thing-00500')
  })

  /**
   * DO point lookup via SQL
   * Uses sql.exec for structured queries
   */
  bench('DO point lookup (sql.exec)', () => {
    doInstance.sqlPointLookup('thing-00500')
  })

  /**
   * DO point lookup via RPC stub
   * Full overhead: Worker -> DO RPC -> storage
   */
  bench('DO point lookup via RPC', async () => {
    await doStub.rpc('pointLookup', 'thing-00500')
  })

  /**
   * DO point lookup via fetch
   * Full HTTP-style overhead: parse URL, route, serialize response
   */
  bench('DO point lookup via fetch', async () => {
    const req = new Request('https://do.example.com/things/thing-00500')
    await doStub.fetch(req)
  })

  /**
   * Measures pure RPC overhead by comparing direct call vs RPC call
   */
  bench('RPC overhead measurement (direct call)', () => {
    workerHandler.directCall({ id: 'thing-00500' })
  })
})

// =============================================================================
// Write Operation Benchmarks
// =============================================================================

describe('Worker vs DO - Write Operations', () => {
  const newThing = generateThing('new-thing-001')
  const batchThings = generateThings(10)

  /**
   * Worker write: In-memory only, no persistence
   * Fastest possible write - lost on restart
   */
  bench('worker write (no persistence)', async () => {
    await workerHandler.write({ ...newThing, $id: `tmp-${Math.random()}` })
  })

  /**
   * DO write: Persisted to SQLite storage
   * Durable but includes write latency
   */
  bench('DO write (SQLite persistence)', async () => {
    await doInstance.write({ ...newThing, $id: `tmp-${Math.random()}` })
  })

  /**
   * DO transactional batch write
   * Multiple writes in a single transaction
   */
  bench('DO transactional batch write (10 items)', async () => {
    const items = batchThings.map(t => ({ ...t, $id: `batch-${Math.random()}` }))
    await doInstance.batchWrite(items)
  })

  /**
   * Worker batch write: In-memory only
   * Baseline for batch operations
   */
  bench('worker batch write (10 items, no persistence)', async () => {
    const items = batchThings.map(t => ({ ...t, $id: `batch-${Math.random()}` }))
    await workerHandler.batchWrite(items)
  })

  /**
   * DO write via RPC
   * Full overhead including RPC + persistence
   */
  bench('DO write via RPC', async () => {
    await doStub.rpc('write', { ...newThing, $id: `rpc-${Math.random()}` })
  })
})

// =============================================================================
// DO-Only Feature Benchmarks
// =============================================================================

describe('DO-Only Features', () => {
  /**
   * DO alarm scheduling
   * Sets a future alarm - unique to DOs
   */
  bench('DO alarm scheduling', async () => {
    await doInstance.scheduleAlarm(60000) // 1 minute from now
  })

  /**
   * DO alarm cancellation
   */
  bench('DO alarm cancellation', async () => {
    await doInstance.cancelAlarm()
  })

  /**
   * DO alarm check
   * Query current alarm state
   */
  bench('DO get scheduled alarm', async () => {
    await doInstance.getAlarm()
  })

  /**
   * DO WebSocket accept
   * Unique capability of Durable Objects
   */
  bench('DO websocket accept', async () => {
    const mockRequest = new Request('https://do.example.com/ws', {
      headers: { 'Upgrade': 'websocket' }
    })
    await doInstance.acceptWebSocket(mockRequest)
  })

  /**
   * Compare storage.get vs sql.exec for same operation
   */
  bench('DO storage.get vs sql.exec: storage.get', async () => {
    await doInstance.storage.get('thing:thing-00500')
  })

  bench('DO storage.get vs sql.exec: sql.exec', () => {
    doInstance.sql.exec('SELECT * FROM things WHERE id = ?', 'thing-00500')
  })
})

// =============================================================================
// RPC Overhead Isolation
// =============================================================================

describe('RPC Overhead Isolation', () => {
  /**
   * Baseline: Direct method call (no RPC)
   */
  bench('baseline: direct method call', async () => {
    await doInstance.pointLookup('thing-00500')
  })

  /**
   * With RPC: Same operation through stub
   * Difference = pure RPC overhead
   */
  bench('with RPC: stub.rpc() call', async () => {
    await doStub.rpc('pointLookup', 'thing-00500')
  })

  /**
   * With HTTP-style fetch: Full request/response cycle
   * Includes URL parsing, routing, serialization
   */
  bench('with fetch: full HTTP cycle', async () => {
    const req = new Request('https://do.example.com/things/thing-00500')
    const res = await doStub.fetch(req)
    await res.json()
  })
})

// =============================================================================
// Persistence Overhead Isolation
// =============================================================================

describe('Persistence Overhead Isolation', () => {
  /**
   * No persistence: Worker in-memory write
   */
  bench('no persistence: in-memory write', async () => {
    await workerHandler.write(generateThing(`mem-${Math.random()}`))
  })

  /**
   * With persistence: DO storage write
   * Difference = SQLite persistence overhead
   */
  bench('with persistence: DO storage.put', async () => {
    await doInstance.storage.put(`thing:persist-${Math.random()}`, generateThing('x'))
  })

  /**
   * With transaction: DO transactional write
   * Additional overhead from transaction boundaries
   */
  bench('with transaction: DO storage.transaction', async () => {
    await doInstance.storage.transaction(async () => {
      await doInstance.storage.put(`thing:tx-${Math.random()}`, generateThing('x'))
    })
  })
})

// =============================================================================
// Concurrent Access Patterns
// =============================================================================

describe('Concurrent Access Patterns', () => {
  /**
   * Worker: Concurrent reads (no coordination needed)
   */
  bench('worker: 10 concurrent reads', async () => {
    await Promise.all(
      Array.from({ length: 10 }, (_, i) =>
        workerHandler.pointLookup(`thing-${String(i * 100).padStart(5, '0')}`)
      )
    )
  })

  /**
   * DO: 10 concurrent reads
   * DOs serialize access, so this measures queuing overhead
   */
  bench('DO: 10 concurrent reads', async () => {
    await Promise.all(
      Array.from({ length: 10 }, (_, i) =>
        doInstance.pointLookup(`thing-${String(i * 100).padStart(5, '0')}`)
      )
    )
  })

  /**
   * DO via RPC: 10 concurrent reads
   * Each read incurs RPC overhead
   */
  bench('DO via RPC: 10 concurrent reads', async () => {
    await Promise.all(
      Array.from({ length: 10 }, (_, i) =>
        doStub.rpc('pointLookup', `thing-${String(i * 100).padStart(5, '0')}`)
      )
    )
  })
})

// =============================================================================
// State Size Impact
// =============================================================================

describe('State Size Impact', () => {
  const smallThing = { $id: 'small', $type: 'X', name: 'x', status: 'a', createdAt: '' }
  const largeThing = {
    ...smallThing,
    $id: 'large',
    data: {
      nested: Array.from({ length: 100 }, (_, i) => ({
        id: i,
        name: `Item ${i}`,
        values: Array.from({ length: 10 }, () => Math.random())
      }))
    }
  }

  /**
   * Small object write (~100 bytes)
   */
  bench('DO write: small object (~100B)', async () => {
    await doInstance.storage.put('small-obj', { ...smallThing, $id: `s-${Math.random()}` })
  })

  /**
   * Large object write (~10KB)
   */
  bench('DO write: large object (~10KB)', async () => {
    await doInstance.storage.put('large-obj', { ...largeThing, $id: `l-${Math.random()}` })
  })

  /**
   * Small object read
   */
  bench('DO read: small object', async () => {
    await doInstance.storage.get('small-obj')
  })

  /**
   * Large object read
   */
  bench('DO read: large object', async () => {
    await doInstance.storage.get('large-obj')
  })
})
