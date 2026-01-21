import { bench, describe, beforeAll } from 'vitest'

/**
 * Colo (Colocation) Benchmarks
 *
 * Measures latency differences based on geographic colocation of Workers and DOs.
 *
 * Key concepts:
 * - Same colo: Worker and DO in same datacenter (optimal)
 * - Cross-colo: Worker in one region, DO in another (adds RTT)
 * - DO placement: Determined by first request origin or locationHint
 *
 * Real-world RTT estimates (for simulation):
 * - Same colo: ~0.1-0.5ms
 * - Same continent: ~20-50ms
 * - Cross-continent: ~100-200ms
 * - Global (antipodal): ~150-300ms
 *
 * Note: Actual multi-region testing requires deployed Workers.
 * These benchmarks use simulation helpers to model cross-colo latency.
 */

// Simulated RTT values in milliseconds
const RTT = {
  SAME_COLO: 0.3,          // Same datacenter
  SAME_REGION: 5,           // Same region, different colo
  SAME_CONTINENT: 30,       // e.g., LAX to ORD
  CROSS_CONTINENT: 120,     // e.g., LAX to AMS
  ANTIPODAL: 200,           // e.g., NYC to Sydney
} as const

// Cloudflare colo codes for documentation
const COLOS = {
  // North America
  SJC: 'San Jose',
  LAX: 'Los Angeles',
  SEA: 'Seattle',
  ORD: 'Chicago',
  IAD: 'Washington DC',
  EWR: 'Newark',
  ATL: 'Atlanta',
  DFW: 'Dallas',
  // Europe
  AMS: 'Amsterdam',
  LHR: 'London',
  FRA: 'Frankfurt',
  CDG: 'Paris',
  // Asia Pacific
  NRT: 'Tokyo',
  SIN: 'Singapore',
  SYD: 'Sydney',
  HKG: 'Hong Kong',
} as const

/**
 * Simulation helper: adds artificial latency to model cross-colo access
 */
async function simulateRTT(ms: number): Promise<void> {
  if (ms > 0) {
    await new Promise(resolve => setTimeout(resolve, ms))
  }
}

/**
 * Simulation helper: models a DO stub.fetch() with network latency
 */
async function simulateDOFetch(
  operation: () => Promise<any>,
  rtt: number = RTT.SAME_COLO
): Promise<any> {
  // RTT is round-trip, so we split: request travel + operation + response travel
  await simulateRTT(rtt / 2)
  const result = await operation()
  await simulateRTT(rtt / 2)
  return result
}

/**
 * Mock DO storage operations for benchmarking
 */
class MockDOStorage {
  private data = new Map<string, any>()

  async get(key: string): Promise<any> {
    // Simulate local SQLite read (~0.05ms)
    return this.data.get(key)
  }

  async put(key: string, value: any): Promise<void> {
    // Simulate local SQLite write (~0.1ms)
    this.data.set(key, value)
  }

  async list(options?: { prefix?: string; limit?: number }): Promise<Map<string, any>> {
    const result = new Map<string, any>()
    let count = 0
    for (const [key, value] of this.data) {
      if (options?.prefix && !key.startsWith(options.prefix)) continue
      result.set(key, value)
      count++
      if (options?.limit && count >= options.limit) break
    }
    return result
  }

  async sql(query: string, params?: any[]): Promise<any[]> {
    // Simulate SQL execution (~0.1-1ms depending on query)
    return []
  }
}

/**
 * Mock DO instance for benchmarking
 */
class MockDO {
  storage: MockDOStorage
  id: string
  colo: keyof typeof COLOS

  constructor(id: string, colo: keyof typeof COLOS = 'SJC') {
    this.id = id
    this.colo = colo
    this.storage = new MockDOStorage()
  }

  async fetch(request: Request): Promise<Response> {
    // Simulate request handling
    const url = new URL(request.url)
    const path = url.pathname

    if (path === '/health') {
      return new Response(JSON.stringify({ status: 'ok', colo: this.colo }))
    }

    if (path.startsWith('/things/')) {
      const id = path.split('/')[2]
      const thing = await this.storage.get(`thing:${id}`)
      return new Response(JSON.stringify(thing || null))
    }

    return new Response(JSON.stringify({ colo: this.colo }))
  }
}

// Initialize mock instances
let localDO: MockDO
let remoteDOs: Map<keyof typeof COLOS, MockDO>

beforeAll(async () => {
  localDO = new MockDO('local-do', 'SJC')

  // Pre-populate with test data
  for (let i = 0; i < 1000; i++) {
    await localDO.storage.put(`thing:thing-${i.toString().padStart(4, '0')}`, {
      id: `thing-${i.toString().padStart(4, '0')}`,
      name: `Thing ${i}`,
      status: i % 2 === 0 ? 'active' : 'inactive',
      createdAt: Date.now() - i * 1000,
    })
  }

  // Create DOs in different colos for cross-region simulation
  remoteDOs = new Map([
    ['LAX', new MockDO('do-lax', 'LAX')],
    ['ORD', new MockDO('do-ord', 'ORD')],
    ['AMS', new MockDO('do-ams', 'AMS')],
    ['NRT', new MockDO('do-nrt', 'NRT')],
    ['SYD', new MockDO('do-syd', 'SYD')],
  ])

  // Populate remote DOs
  for (const [, do_] of remoteDOs) {
    for (let i = 0; i < 100; i++) {
      await do_.storage.put(`thing:thing-${i.toString().padStart(4, '0')}`, {
        id: `thing-${i.toString().padStart(4, '0')}`,
        name: `Thing ${i}`,
      })
    }
  }
})

// =============================================================================
// Same Datacenter Benchmarks
// =============================================================================

describe('Colo - Same Datacenter', () => {
  /**
   * Baseline: Worker and DO in same colo
   * This is the optimal scenario with minimal network latency
   */

  bench('local DO access (fetch)', async () => {
    await simulateDOFetch(async () => {
      return localDO.fetch(new Request('https://do/health'))
    }, RTT.SAME_COLO)
  })

  bench('local storage.get (single key)', async () => {
    await simulateDOFetch(async () => {
      return localDO.storage.get('thing:thing-0001')
    }, RTT.SAME_COLO)
  })

  bench('local storage.get (10 sequential)', async () => {
    await simulateDOFetch(async () => {
      const results = []
      for (let i = 0; i < 10; i++) {
        results.push(await localDO.storage.get(`thing:thing-${i.toString().padStart(4, '0')}`))
      }
      return results
    }, RTT.SAME_COLO)
  })

  bench('local storage.list (100 items)', async () => {
    await simulateDOFetch(async () => {
      return localDO.storage.list({ prefix: 'thing:', limit: 100 })
    }, RTT.SAME_COLO)
  })

  bench('local sql.exec (simple query)', async () => {
    await simulateDOFetch(async () => {
      return localDO.storage.sql('SELECT * FROM things WHERE id = ?', ['thing-0001'])
    }, RTT.SAME_COLO)
  })

  bench('local sql.exec (range query)', async () => {
    await simulateDOFetch(async () => {
      return localDO.storage.sql(
        'SELECT * FROM things WHERE status = ? LIMIT 100',
        ['active']
      )
    }, RTT.SAME_COLO)
  })
})

// =============================================================================
// Cross-Region Benchmarks (Simulated)
// =============================================================================

describe('Colo - Cross-Region (simulated)', () => {
  /**
   * These benchmarks simulate the latency impact of accessing a DO
   * that is located in a different region than the Worker handling the request.
   *
   * Real-world scenario: User in Europe hits a Worker in AMS,
   * but their DO was created and pinned to SJC.
   */

  bench('same-region DO access (~5ms RTT)', async () => {
    await simulateDOFetch(async () => {
      return remoteDOs.get('LAX')!.fetch(new Request('https://do/health'))
    }, RTT.SAME_REGION)
  })

  bench('same-continent DO access (~30ms RTT)', async () => {
    await simulateDOFetch(async () => {
      return remoteDOs.get('ORD')!.fetch(new Request('https://do/health'))
    }, RTT.SAME_CONTINENT)
  })

  bench('cross-continent DO access (~120ms RTT)', async () => {
    await simulateDOFetch(async () => {
      return remoteDOs.get('AMS')!.fetch(new Request('https://do/health'))
    }, RTT.CROSS_CONTINENT)
  })

  bench('antipodal DO access (~200ms RTT)', async () => {
    await simulateDOFetch(async () => {
      return remoteDOs.get('SYD')!.fetch(new Request('https://do/health'))
    }, RTT.ANTIPODAL)
  })

  bench('cross-continent with storage.get', async () => {
    await simulateDOFetch(async () => {
      return remoteDOs.get('AMS')!.storage.get('thing:thing-0001')
    }, RTT.CROSS_CONTINENT)
  })

  bench('antipodal with storage.list (100 items)', async () => {
    await simulateDOFetch(async () => {
      return remoteDOs.get('SYD')!.storage.list({ prefix: 'thing:', limit: 100 })
    }, RTT.ANTIPODAL)
  })
})

// =============================================================================
// RTT Impact Analysis
// =============================================================================

describe('Colo - RTT Impact on Operations', () => {
  /**
   * Demonstrates how RTT compounds with multiple DO calls.
   * Key insight: Batch operations or DO-side logic reduces RTT impact.
   */

  bench('1 DO call (same colo)', async () => {
    await simulateDOFetch(async () => localDO.storage.get('thing:thing-0001'), RTT.SAME_COLO)
  })

  bench('1 DO call (cross-continent)', async () => {
    await simulateDOFetch(async () => localDO.storage.get('thing:thing-0001'), RTT.CROSS_CONTINENT)
  })

  bench('5 sequential DO calls (same colo)', async () => {
    for (let i = 0; i < 5; i++) {
      await simulateDOFetch(async () => localDO.storage.get(`thing:thing-000${i}`), RTT.SAME_COLO)
    }
  })

  bench('5 sequential DO calls (cross-continent)', async () => {
    // This shows the pain of chatty protocols across regions
    // 5 calls * 120ms RTT = 600ms minimum
    for (let i = 0; i < 5; i++) {
      await simulateDOFetch(async () => localDO.storage.get(`thing:thing-000${i}`), RTT.CROSS_CONTINENT)
    }
  })

  bench('5 batched DO calls (cross-continent)', async () => {
    // Single round-trip with batched operation
    await simulateDOFetch(async () => {
      const results = []
      for (let i = 0; i < 5; i++) {
        results.push(await localDO.storage.get(`thing:thing-000${i}`))
      }
      return results
    }, RTT.CROSS_CONTINENT)
  })
})

// =============================================================================
// Distribution Patterns
// =============================================================================

describe('Colo - Distribution Patterns', () => {
  /**
   * Different DO distribution patterns have different latency characteristics.
   *
   * 1. DO per-user: User's DO is pinned to their first-request location
   *    - Pro: Always local for that user
   *    - Con: Cross-colo access if user travels or uses VPN
   *
   * 2. DO per-tenant: Tenant's DO in one location, all users access it
   *    - Pro: Consistent state, easier to manage
   *    - Con: Some users will always have high latency
   *
   * 3. Global singleton: One DO for entire app
   *    - Pro: Simplest model, strong consistency
   *    - Con: Worst-case latency for most users
   */

  bench('DO per-user pattern (user local)', async () => {
    // User's DO is in their local colo - best case
    const userId = 'user-001'
    await simulateDOFetch(async () => {
      return localDO.storage.get(`user:${userId}:profile`)
    }, RTT.SAME_COLO)
  })

  bench('DO per-user pattern (user traveling)', async () => {
    // User's DO is pinned to home region, but they're traveling
    // Simulates: User DO in SJC, user now in Europe
    const userId = 'user-001'
    await simulateDOFetch(async () => {
      return localDO.storage.get(`user:${userId}:profile`)
    }, RTT.CROSS_CONTINENT)
  })

  bench('DO per-tenant pattern (tenant local)', async () => {
    // Company's DO is in SF, employee in SF
    const tenantId = 'acme-corp'
    await simulateDOFetch(async () => {
      return localDO.storage.get(`tenant:${tenantId}:settings`)
    }, RTT.SAME_COLO)
  })

  bench('DO per-tenant pattern (employee remote)', async () => {
    // Company's DO is in SF, employee in Europe
    const tenantId = 'acme-corp'
    await simulateDOFetch(async () => {
      return localDO.storage.get(`tenant:${tenantId}:settings`)
    }, RTT.CROSS_CONTINENT)
  })

  bench('global singleton DO (local user)', async () => {
    // Global config DO, accessed by local user
    await simulateDOFetch(async () => {
      return localDO.storage.get('global:config')
    }, RTT.SAME_COLO)
  })

  bench('global singleton DO (remote user)', async () => {
    // Global config DO in US, accessed from Australia
    await simulateDOFetch(async () => {
      return localDO.storage.get('global:config')
    }, RTT.ANTIPODAL)
  })
})

// =============================================================================
// Location Hint Strategies
// =============================================================================

describe('Colo - Location Hint Strategies', () => {
  /**
   * DO placement strategies using idFromName vs locationHint
   *
   * idFromName: DO placed where first request originated
   * locationHint: Explicitly specify desired colo
   *
   * Use locationHint when:
   * - You know user's primary region
   * - You want to colocate with other services
   * - You're optimizing for specific user segments
   */

  bench('idFromName (natural placement)', async () => {
    // DO created based on first request origin
    // Simulates: First request came from local region
    await simulateDOFetch(async () => {
      return localDO.fetch(new Request('https://do/things/thing-0001'))
    }, RTT.SAME_COLO)
  })

  bench('locationHint: same region', async () => {
    // Explicitly placed in user's region
    await simulateDOFetch(async () => {
      return localDO.fetch(new Request('https://do/things/thing-0001'))
    }, RTT.SAME_COLO)
  })

  bench('locationHint: wrong region', async () => {
    // Explicitly placed in wrong region (e.g., compliance requirement)
    // Simulates: User in Australia, DO must be in EU for GDPR
    await simulateDOFetch(async () => {
      return remoteDOs.get('AMS')!.fetch(new Request('https://do/things/thing-0001'))
    }, RTT.ANTIPODAL)
  })

  bench('locationHint: regional hub strategy', async () => {
    // Use regional hubs (e.g., one DO per continent)
    // Simulates: US West user accessing US regional hub
    await simulateDOFetch(async () => {
      return localDO.fetch(new Request('https://do/things/thing-0001'))
    }, RTT.SAME_REGION)
  })
})

// =============================================================================
// P50/P95/P99 Latency Estimation
// =============================================================================

describe('Colo - Latency Percentile Simulation', () => {
  /**
   * Simulates latency distribution for different scenarios.
   * Real percentiles would require deployed Workers with actual measurements.
   *
   * Typical distribution factors:
   * - Base RTT variance: +/- 20%
   * - Congestion spikes: 2-5x normal
   * - Packet loss/retransmit: adds 1 RTT
   */

  bench('P50 latency simulation (same colo)', async () => {
    // P50 = median, typical case
    const variance = 0.2
    const latency = RTT.SAME_COLO * (1 + (Math.random() - 0.5) * variance)
    await simulateDOFetch(async () => localDO.storage.get('thing:thing-0001'), latency)
  })

  bench('P95 latency simulation (same colo)', async () => {
    // P95 = 95th percentile, occasional spike
    const spike = 2
    const latency = RTT.SAME_COLO * spike
    await simulateDOFetch(async () => localDO.storage.get('thing:thing-0001'), latency)
  })

  bench('P99 latency simulation (same colo)', async () => {
    // P99 = 99th percentile, rare spike (congestion, retransmit)
    const spike = 5
    const latency = RTT.SAME_COLO * spike
    await simulateDOFetch(async () => localDO.storage.get('thing:thing-0001'), latency)
  })

  bench('P50 latency simulation (cross-continent)', async () => {
    const variance = 0.2
    const latency = RTT.CROSS_CONTINENT * (1 + (Math.random() - 0.5) * variance)
    await simulateDOFetch(async () => localDO.storage.get('thing:thing-0001'), latency)
  })

  bench('P95 latency simulation (cross-continent)', async () => {
    const spike = 2
    const latency = RTT.CROSS_CONTINENT * spike
    await simulateDOFetch(async () => localDO.storage.get('thing:thing-0001'), latency)
  })

  bench('P99 latency simulation (cross-continent)', async () => {
    const spike = 3 // Less dramatic spike at higher base latency
    const latency = RTT.CROSS_CONTINENT * spike
    await simulateDOFetch(async () => localDO.storage.get('thing:thing-0001'), latency)
  })
})

// =============================================================================
// Optimization Strategies
// =============================================================================

describe('Colo - Optimization Strategies', () => {
  /**
   * Strategies to minimize cross-colo latency impact
   */

  bench('eager DO migration (simulated)', async () => {
    // Concept: Migrate DO closer to where requests are coming from
    // After migration, access becomes local
    // Note: DO migration is not currently supported by Cloudflare
    await simulateDOFetch(async () => localDO.storage.get('thing:thing-0001'), RTT.SAME_COLO)
  })

  bench('read replica pattern (simulated)', async () => {
    // Concept: Read from local replica, write to primary
    // Read is fast (local), write adds latency
    // Note: This would require custom implementation
    await simulateDOFetch(async () => localDO.storage.get('thing:thing-0001'), RTT.SAME_COLO)
  })

  bench('write-through cache (KV)', async () => {
    // Use KV as globally replicated cache in front of DO
    // KV read is local, DO write is async
    await simulateRTT(1) // KV read latency ~1ms
  })

  bench('edge cache (Cache API)', async () => {
    // Cache frequently accessed DO data at edge
    // Cache hit is instant, miss goes to DO
    await simulateRTT(0.1) // Cache hit latency
  })

  bench('predictive prefetch', async () => {
    // Prefetch likely-needed data during idle time
    // When user needs it, it's already in local cache
    await simulateRTT(0.1) // Already in local memory
  })
})
