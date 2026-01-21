import { bench, describe } from 'vitest'

/**
 * Colo (Colocation) Benchmarks - REAL Network Latency
 *
 * Measures actual round-trip latency to colo.do endpoints in different datacenters.
 *
 * Available colo.do endpoints:
 * - ord.colo.do - Chicago (ORD)
 * - iad.colo.do - Virginia (IAD)
 * - lhr.colo.do - London (LHR)
 * - ams.colo.do - Amsterdam (AMS)
 * - sin.colo.do - Singapore (SIN)
 * - syd.colo.do - Sydney (SYD)
 * - nrt.colo.do - Tokyo (NRT)
 * - lax.colo.do - Los Angeles (LAX)
 * - dfw.colo.do - Dallas (DFW)
 */

// Colo endpoints with their locations
const COLOS = {
  ord: 'Chicago',
  iad: 'Virginia',
  lhr: 'London',
  ams: 'Amsterdam',
  sin: 'Singapore',
  syd: 'Sydney',
  nrt: 'Tokyo',
  lax: 'Los Angeles',
  dfw: 'Dallas',
} as const

type ColoCode = keyof typeof COLOS

/**
 * Measure latency to a specific colo endpoint
 */
async function measureColoLatency(colo: ColoCode): Promise<number> {
  const start = performance.now()
  await fetch(`https://${colo}.colo.do/ping`)
  return performance.now() - start
}

/**
 * Find the closest colo by measuring RTT to all endpoints
 */
async function findClosestColo(): Promise<{ colo: ColoCode; latency: number; all: Record<ColoCode, number> }> {
  const results: Record<string, number> = {}

  // Measure all colos in parallel
  const measurements = await Promise.all(
    Object.keys(COLOS).map(async (colo) => {
      const latency = await measureColoLatency(colo as ColoCode)
      return { colo: colo as ColoCode, latency }
    })
  )

  let closest: { colo: ColoCode; latency: number } = { colo: 'ord', latency: Infinity }

  for (const { colo, latency } of measurements) {
    results[colo] = latency
    if (latency < closest.latency) {
      closest = { colo, latency }
    }
  }

  return { ...closest, all: results as Record<ColoCode, number> }
}

// =============================================================================
// Real Cross-Region Latency Benchmarks
// =============================================================================

describe('Colo - Real Cross-Region Latency', () => {
  /**
   * Measure actual RTT to each colo.do endpoint.
   * Results will vary based on your location.
   */

  bench('ORD (Chicago)', async () => {
    await fetch('https://ord.colo.do/ping')
  })

  bench('IAD (Virginia)', async () => {
    await fetch('https://iad.colo.do/ping')
  })

  bench('LHR (London)', async () => {
    await fetch('https://lhr.colo.do/ping')
  })

  bench('AMS (Amsterdam)', async () => {
    await fetch('https://ams.colo.do/ping')
  })

  bench('SIN (Singapore)', async () => {
    await fetch('https://sin.colo.do/ping')
  })

  bench('SYD (Sydney)', async () => {
    await fetch('https://syd.colo.do/ping')
  })

  bench('NRT (Tokyo)', async () => {
    await fetch('https://nrt.colo.do/ping')
  })

  bench('LAX (Los Angeles)', async () => {
    await fetch('https://lax.colo.do/ping')
  })

  bench('DFW (Dallas)', async () => {
    await fetch('https://dfw.colo.do/ping')
  })
})

// =============================================================================
// Closest Colo Discovery
// =============================================================================

describe('Colo - Closest Colo Discovery', () => {
  /**
   * Discover which colo is closest by measuring RTT to all endpoints.
   * This is useful for determining optimal DO placement.
   */

  bench('find closest colo (parallel measurement)', async () => {
    await findClosestColo()
  })

  bench('measure single colo (baseline)', async () => {
    await measureColoLatency('ord')
  })
})

// =============================================================================
// Sequential vs Parallel Fetch Comparison
// =============================================================================

describe('Colo - Sequential vs Parallel Fetches', () => {
  /**
   * Compare the performance of sequential vs parallel fetches to multiple colos.
   * Demonstrates the importance of parallelization for multi-region operations.
   */

  bench('3 colos sequential (ORD, IAD, LHR)', async () => {
    await fetch('https://ord.colo.do/ping')
    await fetch('https://iad.colo.do/ping')
    await fetch('https://lhr.colo.do/ping')
  })

  bench('3 colos parallel (ORD, IAD, LHR)', async () => {
    await Promise.all([
      fetch('https://ord.colo.do/ping'),
      fetch('https://iad.colo.do/ping'),
      fetch('https://lhr.colo.do/ping'),
    ])
  })

  bench('5 colos sequential (US + EU)', async () => {
    await fetch('https://ord.colo.do/ping')
    await fetch('https://iad.colo.do/ping')
    await fetch('https://lax.colo.do/ping')
    await fetch('https://lhr.colo.do/ping')
    await fetch('https://ams.colo.do/ping')
  })

  bench('5 colos parallel (US + EU)', async () => {
    await Promise.all([
      fetch('https://ord.colo.do/ping'),
      fetch('https://iad.colo.do/ping'),
      fetch('https://lax.colo.do/ping'),
      fetch('https://lhr.colo.do/ping'),
      fetch('https://ams.colo.do/ping'),
    ])
  })

  bench('all 9 colos sequential', async () => {
    for (const colo of Object.keys(COLOS)) {
      await fetch(`https://${colo}.colo.do/ping`)
    }
  })

  bench('all 9 colos parallel', async () => {
    await Promise.all(
      Object.keys(COLOS).map(colo => fetch(`https://${colo}.colo.do/ping`))
    )
  })
})

// =============================================================================
// Regional Grouping Benchmarks
// =============================================================================

describe('Colo - Regional Groups', () => {
  /**
   * Measure latency to regional groups of colos.
   * Useful for understanding regional latency characteristics.
   */

  bench('US colos parallel (ORD, IAD, LAX, DFW)', async () => {
    await Promise.all([
      fetch('https://ord.colo.do/ping'),
      fetch('https://iad.colo.do/ping'),
      fetch('https://lax.colo.do/ping'),
      fetch('https://dfw.colo.do/ping'),
    ])
  })

  bench('EU colos parallel (LHR, AMS)', async () => {
    await Promise.all([
      fetch('https://lhr.colo.do/ping'),
      fetch('https://ams.colo.do/ping'),
    ])
  })

  bench('APAC colos parallel (SIN, SYD, NRT)', async () => {
    await Promise.all([
      fetch('https://sin.colo.do/ping'),
      fetch('https://syd.colo.do/ping'),
      fetch('https://nrt.colo.do/ping'),
    ])
  })
})

// =============================================================================
// Distribution Pattern Benchmarks (Real Fetches)
// =============================================================================

describe('Colo - Distribution Patterns', () => {
  /**
   * Different DO distribution patterns have different latency characteristics.
   * These benchmarks use real fetches to demonstrate the patterns.
   *
   * 1. DO per-user: User's DO is pinned to their first-request location
   *    - Pro: Always local for that user
   *    - Con: Cross-colo access if user travels or uses VPN
   *
   * 2. DO per-tenant: Tenant's DO in one location, all users access it
   *    - Pro: Consistent state, easier to manage
   *    - Con: Some users will always have high latency
   *
   * 3. Regional hub: One DO per major region
   *    - Pro: Good balance of latency and consistency
   *    - Con: More complex to manage
   */

  bench('single region access (closest colo)', async () => {
    // Simulate accessing a DO in your closest colo
    // In production, you'd use findClosestColo() to determine this
    await fetch('https://ord.colo.do/ping')
  })

  bench('cross-continent access (US to EU)', async () => {
    // Simulate a US user accessing a DO pinned in EU
    await Promise.all([
      fetch('https://ord.colo.do/ping'),  // Local worker
      fetch('https://lhr.colo.do/ping'),  // Remote DO
    ])
  })

  bench('cross-continent access (US to APAC)', async () => {
    // Simulate a US user accessing a DO pinned in APAC
    await Promise.all([
      fetch('https://ord.colo.do/ping'),  // Local worker
      fetch('https://nrt.colo.do/ping'),  // Remote DO
    ])
  })

  bench('regional hub pattern - US hub', async () => {
    // Access US regional hub from multiple US locations
    await Promise.all([
      fetch('https://ord.colo.do/ping'),
      fetch('https://iad.colo.do/ping'),
      fetch('https://lax.colo.do/ping'),
    ])
  })

  bench('regional hub pattern - global hubs', async () => {
    // Access all regional hubs in parallel
    await Promise.all([
      fetch('https://ord.colo.do/ping'),  // US hub
      fetch('https://lhr.colo.do/ping'),  // EU hub
      fetch('https://nrt.colo.do/ping'),  // APAC hub
    ])
  })
})

// =============================================================================
// Repeated Request Benchmarks (Connection Reuse)
// =============================================================================

describe('Colo - Connection Reuse', () => {
  /**
   * Measure the impact of connection reuse on latency.
   * HTTP/2 and keep-alive can significantly reduce subsequent request latency.
   */

  bench('5 sequential requests to same colo', async () => {
    for (let i = 0; i < 5; i++) {
      await fetch('https://ord.colo.do/ping')
    }
  })

  bench('5 parallel requests to same colo', async () => {
    await Promise.all([
      fetch('https://ord.colo.do/ping'),
      fetch('https://ord.colo.do/ping'),
      fetch('https://ord.colo.do/ping'),
      fetch('https://ord.colo.do/ping'),
      fetch('https://ord.colo.do/ping'),
    ])
  })

  bench('10 sequential requests to same colo', async () => {
    for (let i = 0; i < 10; i++) {
      await fetch('https://ord.colo.do/ping')
    }
  })

  bench('10 parallel requests to same colo', async () => {
    await Promise.all(
      Array.from({ length: 10 }, () => fetch('https://ord.colo.do/ping'))
    )
  })
})

// =============================================================================
// Latency Consistency Benchmarks
// =============================================================================

describe('Colo - Latency Consistency', () => {
  /**
   * Measure latency consistency over multiple requests.
   * Real-world latency varies due to network conditions, congestion, etc.
   */

  bench('ORD consistency (single request)', async () => {
    await fetch('https://ord.colo.do/ping')
  })

  bench('LHR consistency (single request)', async () => {
    await fetch('https://lhr.colo.do/ping')
  })

  bench('NRT consistency (single request)', async () => {
    await fetch('https://nrt.colo.do/ping')
  })

  bench('SYD consistency (single request)', async () => {
    await fetch('https://syd.colo.do/ping')
  })
})

// =============================================================================
// Real-World Scenario Benchmarks
// =============================================================================

describe('Colo - Real-World Scenarios', () => {
  /**
   * Simulate real-world access patterns with actual network requests.
   */

  bench('API gateway pattern (route to closest)', async () => {
    // Simulate an API gateway that routes to the closest colo
    const { colo } = await findClosestColo()
    await fetch(`https://${colo}.colo.do/ping`)
  })

  bench('multi-region read (fan-out to 3 regions)', async () => {
    // Read from multiple regions and use fastest response
    const results = await Promise.all([
      fetch('https://ord.colo.do/ping').then(r => ({ region: 'ord', response: r })),
      fetch('https://lhr.colo.do/ping').then(r => ({ region: 'lhr', response: r })),
      fetch('https://nrt.colo.do/ping').then(r => ({ region: 'nrt', response: r })),
    ])
    // In real scenario, you'd use the first response
    return results
  })

  bench('primary-secondary pattern (write to primary, read from secondary)', async () => {
    // Write to primary (ORD), then read from secondary (IAD)
    await fetch('https://ord.colo.do/ping')  // Primary write
    await fetch('https://iad.colo.do/ping')  // Secondary read
  })

  bench('global singleton (fixed location)', async () => {
    // All requests go to a single global DO location
    await fetch('https://ord.colo.do/ping')
  })
})
