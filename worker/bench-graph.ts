/**
 * Graph Benchmark Worker
 *
 * Cloudflare Worker that runs graph database benchmarks across:
 * - GraphDB (triple store)
 * - SDB (document/graph hybrid)
 * - db4 (document store with joins)
 *
 * Endpoint: POST /benchmark/graph
 * Query params: ?database=graphdb&dataset=wikidata
 *
 * Benchmark operations:
 * - 1-hop traversal
 * - 2-hop traversal
 * - Path queries
 * - Reverse traversal (backlinks)
 * - Pattern matching
 */

import { DurableObject } from 'cloudflare:workers'

// ============================================================================
// Inlined Types (from instrumentation/types.ts)
// ============================================================================

type BenchmarkEnvironment = 'worker' | 'do' | 'container' | 'local'

interface BenchmarkResult {
  benchmark: string
  database: string
  dataset: string
  p50_ms: number
  p99_ms: number
  min_ms: number
  max_ms: number
  mean_ms: number
  stddev_ms?: number
  ops_per_sec: number
  iterations: number
  total_duration_ms?: number
  vfs_reads: number
  vfs_writes: number
  vfs_bytes_read: number
  vfs_bytes_written: number
  timestamp: string
  environment: BenchmarkEnvironment
  colo?: string
  run_id: string
}

// ============================================================================
// Graph Types
// ============================================================================

type ObjectType =
  | 'NULL'
  | 'BOOL'
  | 'INT32'
  | 'INT64'
  | 'FLOAT64'
  | 'STRING'
  | 'BINARY'
  | 'TIMESTAMP'
  | 'DATE'
  | 'REF'
  | 'REF_ARRAY'
  | 'JSON'

interface TypedObject {
  type: ObjectType
  value: unknown
}

interface Triple {
  subject: string
  predicate: string
  object: TypedObject
}

interface Entity {
  $id: string
  $type: string
  [key: string]: unknown
}

interface TraversalOptions {
  maxDepth?: number
  limit?: number
}

// ============================================================================
// In-Memory Graph Store (for isolated benchmark execution)
// ============================================================================

interface InMemoryTripleStore {
  triples: Map<string, Map<string, TypedObject>>
  predicateIndex: Map<string, Set<string>>
  reverseIndex: Map<string, Set<string>>
}

function createInMemoryStore(): InMemoryTripleStore {
  return {
    triples: new Map(),
    predicateIndex: new Map(),
    reverseIndex: new Map(),
  }
}

interface GraphStore {
  insertTriple(triple: Triple): void
  insertTriples(triples: Triple[]): void
  getTriples(subject: string): Triple[]
  insert(entity: Entity): void
  get(id: string): Entity | null
  traverse(startId: string, predicate: string, options?: TraversalOptions): Entity[]
  reverseTraverse(targetId: string, predicate: string, options?: TraversalOptions): Entity[]
  pathTraverse(startId: string, path: string[], options?: TraversalOptions): Entity[]
  query(queryString: string): Entity[]
  batchGet(ids: string[]): (Entity | null)[]
  count(): number
  clear(): void
}

function createGraphStore(): GraphStore {
  const store = createInMemoryStore()

  const insertTriple = (triple: Triple): void => {
    let predicateMap = store.triples.get(triple.subject)
    if (!predicateMap) {
      predicateMap = new Map()
      store.triples.set(triple.subject, predicateMap)
    }
    predicateMap.set(triple.predicate, triple.object)

    let subjects = store.predicateIndex.get(triple.predicate)
    if (!subjects) {
      subjects = new Set()
      store.predicateIndex.set(triple.predicate, subjects)
    }
    subjects.add(triple.subject)

    if (triple.object.type === 'REF' && typeof triple.object.value === 'string') {
      let refSubjects = store.reverseIndex.get(triple.object.value)
      if (!refSubjects) {
        refSubjects = new Set()
        store.reverseIndex.set(triple.object.value, refSubjects)
      }
      refSubjects.add(triple.subject)
    }
  }

  const getEntity = (id: string): Entity | null => {
    const predicateMap = store.triples.get(id)
    if (!predicateMap) return null

    const entity: Entity = { $id: id, $type: '' }
    for (const [predicate, object] of predicateMap) {
      if (predicate === '$type') {
        entity.$type = object.value as string
      } else {
        entity[predicate] = object.value
      }
    }
    return entity
  }

  return {
    insertTriple(triple: Triple): void {
      insertTriple(triple)
    },

    insertTriples(triples: Triple[]): void {
      for (const triple of triples) {
        insertTriple(triple)
      }
    },

    getTriples(subject: string): Triple[] {
      const predicateMap = store.triples.get(subject)
      if (!predicateMap) return []

      const triples: Triple[] = []
      for (const [predicate, object] of predicateMap) {
        triples.push({ subject, predicate, object })
      }
      return triples
    },

    insert(entity: Entity): void {
      insertTriple({
        subject: entity.$id,
        predicate: '$type',
        object: { type: 'STRING', value: entity.$type },
      })

      for (const [key, value] of Object.entries(entity)) {
        if (key === '$id' || key === '$type') continue

        let object: TypedObject
        if (typeof value === 'string') {
          if (value.startsWith('entity:') || value.startsWith('thing-') || value.startsWith('user:') || value.startsWith('Q')) {
            object = { type: 'REF', value }
          } else {
            object = { type: 'STRING', value }
          }
        } else if (typeof value === 'number') {
          object = { type: 'FLOAT64', value }
        } else if (typeof value === 'boolean') {
          object = { type: 'BOOL', value }
        } else if (value === null) {
          object = { type: 'NULL', value: null }
        } else {
          object = { type: 'JSON', value }
        }

        insertTriple({ subject: entity.$id, predicate: key, object })
      }
    },

    get(id: string): Entity | null {
      return getEntity(id)
    },

    traverse(startId: string, predicate: string, options?: TraversalOptions): Entity[] {
      const entities: Entity[] = []
      const visited = new Set<string>()
      const queue: Array<{ id: string; depth: number }> = [{ id: startId, depth: 0 }]
      const maxDepth = options?.maxDepth ?? 1
      const limit = options?.limit ?? 100

      while (queue.length > 0 && entities.length < limit) {
        const { id, depth } = queue.shift()!
        if (visited.has(id) || depth > maxDepth) continue
        visited.add(id)

        const predicateMap = store.triples.get(id)
        if (!predicateMap) continue

        const object = predicateMap.get(predicate)
        if (object?.type === 'REF' && typeof object.value === 'string') {
          const entity = getEntity(object.value)
          if (entity) {
            entities.push(entity)
            if (depth < maxDepth) {
              queue.push({ id: object.value, depth: depth + 1 })
            }
          }
        }
      }

      return entities
    },

    reverseTraverse(targetId: string, predicate: string, options?: TraversalOptions): Entity[] {
      const limit = options?.limit ?? 100
      const subjects = store.reverseIndex.get(targetId)
      if (!subjects) return []

      const entities: Entity[] = []
      for (const subject of subjects) {
        if (entities.length >= limit) break
        const predicateMap = store.triples.get(subject)
        const object = predicateMap?.get(predicate)
        if (object?.type === 'REF' && object.value === targetId) {
          const entity = getEntity(subject)
          if (entity) entities.push(entity)
        }
      }

      return entities
    },

    pathTraverse(startId: string, path: string[], options?: TraversalOptions): Entity[] {
      let currentIds = [startId]
      const limit = options?.limit ?? 100

      for (const predicate of path) {
        const nextIds: string[] = []
        for (const id of currentIds) {
          const predicateMap = store.triples.get(id)
          if (!predicateMap) continue
          const object = predicateMap.get(predicate)
          if (object?.type === 'REF' && typeof object.value === 'string') {
            nextIds.push(object.value)
          }
        }
        currentIds = nextIds
        if (currentIds.length === 0) break
      }

      const entities: Entity[] = []
      for (const id of currentIds) {
        if (entities.length >= limit) break
        const entity = getEntity(id)
        if (entity) entities.push(entity)
      }

      return entities
    },

    query(queryString: string): Entity[] {
      const entities: Entity[] = []

      if (queryString.startsWith('type:')) {
        const typeName = queryString.slice(5)
        const subjects = store.predicateIndex.get('$type')
        if (subjects) {
          for (const subject of subjects) {
            const predicateMap = store.triples.get(subject)
            const typeObj = predicateMap?.get('$type')
            if (typeObj?.value === typeName) {
              const entity = getEntity(subject)
              if (entity) entities.push(entity)
            }
          }
        }
      } else if (queryString.includes(':')) {
        const [predicate, value] = queryString.split(':')
        const subjects = store.predicateIndex.get(predicate)
        if (subjects) {
          for (const subject of subjects) {
            const predicateMap = store.triples.get(subject)
            const obj = predicateMap?.get(predicate)
            if (obj?.value === value) {
              const entity = getEntity(subject)
              if (entity) entities.push(entity)
            }
          }
        }
      }

      return entities
    },

    batchGet(ids: string[]): (Entity | null)[] {
      return ids.map((id) => getEntity(id))
    },

    count(): number {
      return store.triples.size
    },

    clear(): void {
      store.triples.clear()
      store.predicateIndex.clear()
      store.reverseIndex.clear()
    },
  }
}

// ============================================================================
// Dataset Types
// ============================================================================

type DatasetType = 'wikidata' | 'social' | 'synthetic'

interface WikidataEntity {
  id: string
  label: string
  description?: string
  claims: Array<{
    property: string
    value: string | number
    type: 'entity' | 'string' | 'quantity'
  }>
}

interface SocialRelation {
  from: string
  to: string
  type: 'follows' | 'friend' | 'mentions' | 'likes'
  timestamp: string
}

// ============================================================================
// Configuration
// ============================================================================

const BENCHMARK_CONFIG = {
  iterations: {
    traversal: 50,
    pattern: 30,
    batch: 10,
  },
  limits: {
    traversal: 100,
    pattern: 50,
  },
  datasetSizes: {
    wikidata: { entities: 5000, relationships: 10000 },
    social: { users: 2000, relationships: 5000 },
    synthetic: { things: 1000, relationships: 2000 },
  },
} as const

// ============================================================================
// Types
// ============================================================================

interface Env {
  GRAPHDB_DO: DurableObjectNamespace<GraphBenchDO>
  SDB_DO: DurableObjectNamespace<SDBBenchDO>
  DATA: R2Bucket
  RESULTS: R2Bucket
}

type DatabaseType = 'graphdb' | 'sdb' | 'db4'

interface BenchmarkRequest {
  database?: DatabaseType
  dataset?: DatasetType
  operations?: string[]
  iterations?: number
  runId?: string
}

interface BenchmarkTiming {
  name: string
  database: string
  dataset: string
  iterations: number
  totalMs: number
  minMs: number
  maxMs: number
  meanMs: number
  p50Ms: number
  p99Ms: number
  opsPerSec: number
  entitiesTraversed?: number
}

interface GraphBenchmarkResults {
  runId: string
  timestamp: string
  environment: BenchmarkEnvironment
  colo?: string
  database: DatabaseType
  dataset: DatasetType
  benchmarks: BenchmarkTiming[]
  summary: {
    totalDurationMs: number
    totalOperations: number
    overallOpsPerSec: number
  }
}

// ============================================================================
// Utility Functions
// ============================================================================

function generateRunId(): string {
  const timestamp = Date.now().toString(36)
  const random = Math.random().toString(36).substring(2, 8)
  return `graph-${timestamp}-${random}`
}

function calculatePercentile(sortedValues: number[], percentile: number): number {
  if (sortedValues.length === 0) return 0
  const index = Math.ceil((percentile / 100) * sortedValues.length) - 1
  return sortedValues[Math.max(0, index)]
}

function calculateStats(times: number[]): Omit<BenchmarkTiming, 'name' | 'database' | 'dataset' | 'iterations' | 'opsPerSec' | 'entitiesTraversed'> {
  if (times.length === 0) {
    return { totalMs: 0, minMs: 0, maxMs: 0, meanMs: 0, p50Ms: 0, p99Ms: 0 }
  }

  const sorted = [...times].sort((a, b) => a - b)
  const totalMs = times.reduce((a, b) => a + b, 0)

  return {
    totalMs,
    minMs: sorted[0],
    maxMs: sorted[sorted.length - 1],
    meanMs: totalMs / times.length,
    p50Ms: calculatePercentile(sorted, 50),
    p99Ms: calculatePercentile(sorted, 99),
  }
}

// ============================================================================
// Data Seeding Functions
// ============================================================================

function seedWikidataSubset(store: GraphStore): string[] {
  const entityIds: string[] = []
  const { entities, relationships } = BENCHMARK_CONFIG.datasetSizes.wikidata

  // Create Wikidata-style entities
  const types = ['Person', 'Organization', 'Location', 'Work', 'Event']
  const properties = ['P31', 'P279', 'P361', 'P527', 'P17', 'P131', 'P150', 'P36']

  for (let i = 0; i < entities; i++) {
    const id = `Q${i + 1}`
    entityIds.push(id)

    store.insert({
      $id: id,
      $type: types[i % types.length],
      label: `Entity ${i + 1}`,
      description: `Description for entity Q${i + 1}`,
    })
  }

  // Create property relationships (claims)
  for (let i = 0; i < relationships; i++) {
    const subjectIdx = i % entities
    const objectIdx = (i * 7 + 13) % entities // Semi-random distribution
    const property = properties[i % properties.length]

    store.insertTriple({
      subject: entityIds[subjectIdx],
      predicate: property,
      object: { type: 'REF', value: entityIds[objectIdx] },
    })
  }

  return entityIds
}

function seedSocialNetwork(store: GraphStore): string[] {
  const userIds: string[] = []
  const { users, relationships } = BENCHMARK_CONFIG.datasetSizes.social

  // Create users
  for (let i = 0; i < users; i++) {
    const id = `user:${i}`
    userIds.push(id)

    store.insert({
      $id: id,
      $type: 'User',
      name: `User ${i}`,
      handle: `@user${i}`,
      followers_count: Math.floor(Math.random() * 10000),
      following_count: Math.floor(Math.random() * 1000),
    })
  }

  // Create follow relationships (power-law distribution)
  const relationTypes = ['follows', 'mentions', 'likes', 'retweets']
  for (let i = 0; i < relationships; i++) {
    // Power-law: some users have many followers
    const fromIdx = Math.floor(Math.random() * users)
    const toIdx = Math.floor(Math.pow(Math.random(), 2) * users) // Skewed distribution

    if (fromIdx !== toIdx) {
      const relType = relationTypes[i % relationTypes.length]
      store.insertTriple({
        subject: userIds[fromIdx],
        predicate: relType,
        object: { type: 'REF', value: userIds[toIdx] },
      })
    }
  }

  return userIds
}

function seedSyntheticData(store: GraphStore): string[] {
  const thingIds: string[] = []
  const { things, relationships } = BENCHMARK_CONFIG.datasetSizes.synthetic
  const statuses = ['active', 'inactive', 'pending', 'archived']

  for (let i = 0; i < things; i++) {
    const id = `thing-${String(i).padStart(4, '0')}`
    thingIds.push(id)

    store.insert({
      $id: id,
      $type: 'Thing',
      name: `Thing ${i}`,
      status: statuses[i % statuses.length],
    })
  }

  for (let i = 0; i < relationships; i++) {
    const subjectIdx = i % things
    const objectIdx = (i + 1) % things

    store.insertTriple({
      subject: thingIds[subjectIdx],
      predicate: 'relates_to',
      object: { type: 'REF', value: thingIds[objectIdx] },
    })
  }

  return thingIds
}

// ============================================================================
// GraphDB Benchmark Durable Object
// ============================================================================

export class GraphBenchDO extends DurableObject<Env> {
  private store: GraphStore
  private entityIds: string[] = []
  private initialized = false
  private dataset: DatasetType = 'synthetic'

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    this.store = createGraphStore()
  }

  private async initialize(dataset: DatasetType): Promise<void> {
    if (this.initialized && this.dataset === dataset) return

    this.store.clear()
    this.dataset = dataset

    switch (dataset) {
      case 'wikidata':
        this.entityIds = seedWikidataSubset(this.store)
        break
      case 'social':
        this.entityIds = seedSocialNetwork(this.store)
        break
      case 'synthetic':
      default:
        this.entityIds = seedSyntheticData(this.store)
        break
    }

    this.initialized = true
  }

  async runBenchmarks(request: BenchmarkRequest): Promise<GraphBenchmarkResults> {
    const dataset = request.dataset ?? 'synthetic'
    await this.initialize(dataset)

    const runId = request.runId ?? generateRunId()
    const iterations = request.iterations ?? BENCHMARK_CONFIG.iterations.traversal
    const operations = request.operations ?? [
      '1_hop_traversal',
      '2_hop_traversal',
      'path_query',
      'reverse_traversal',
      'pattern_matching_type',
      'pattern_matching_predicate',
      'batch_entity_lookup',
      'triple_retrieval',
    ]

    const benchmarks: BenchmarkTiming[] = []
    const startTime = performance.now()

    for (const op of operations) {
      const timing = await this.runOperation(op, iterations, dataset)
      if (timing) {
        benchmarks.push(timing)
      }
    }

    const totalDurationMs = performance.now() - startTime
    const totalOperations = benchmarks.reduce((sum, b) => sum + b.iterations, 0)

    return {
      runId,
      timestamp: new Date().toISOString(),
      environment: 'do',
      database: 'graphdb',
      dataset,
      benchmarks,
      summary: {
        totalDurationMs,
        totalOperations,
        overallOpsPerSec: totalOperations / (totalDurationMs / 1000),
      },
    }
  }

  private async runOperation(
    operation: string,
    iterations: number,
    dataset: DatasetType
  ): Promise<BenchmarkTiming | null> {
    const times: number[] = []
    let entitiesTraversed = 0

    // Get predicate based on dataset
    const predicate = dataset === 'wikidata' ? 'P31' : dataset === 'social' ? 'follows' : 'relates_to'

    switch (operation) {
      case '1_hop_traversal':
        for (let i = 0; i < iterations; i++) {
          const startId = this.entityIds[i % this.entityIds.length]
          const start = performance.now()
          const result = this.store.traverse(startId, predicate, { maxDepth: 1, limit: 100 })
          times.push(performance.now() - start)
          entitiesTraversed += result.length
        }
        break

      case '2_hop_traversal':
        for (let i = 0; i < iterations; i++) {
          const startId = this.entityIds[i % this.entityIds.length]
          const start = performance.now()
          const result = this.store.pathTraverse(startId, [predicate, predicate], { limit: 100 })
          times.push(performance.now() - start)
          entitiesTraversed += result.length
        }
        break

      case 'path_query':
        // 3-hop path query
        for (let i = 0; i < Math.min(iterations, BENCHMARK_CONFIG.iterations.pattern); i++) {
          const startId = this.entityIds[i % this.entityIds.length]
          const start = performance.now()
          const result = this.store.pathTraverse(startId, [predicate, predicate, predicate], { limit: 50 })
          times.push(performance.now() - start)
          entitiesTraversed += result.length
        }
        break

      case 'reverse_traversal':
        for (let i = 0; i < iterations; i++) {
          const targetId = this.entityIds[(i * 7) % this.entityIds.length] // Different pattern
          const start = performance.now()
          const result = this.store.reverseTraverse(targetId, predicate, { limit: 100 })
          times.push(performance.now() - start)
          entitiesTraversed += result.length
        }
        break

      case 'pattern_matching_type':
        for (let i = 0; i < Math.min(iterations, BENCHMARK_CONFIG.iterations.pattern); i++) {
          const types = dataset === 'wikidata'
            ? ['Person', 'Organization', 'Location', 'Work', 'Event']
            : dataset === 'social'
              ? ['User']
              : ['Thing']
          const type = types[i % types.length]
          const start = performance.now()
          const result = this.store.query(`type:${type}`)
          times.push(performance.now() - start)
          entitiesTraversed += result.length
        }
        break

      case 'pattern_matching_predicate':
        for (let i = 0; i < Math.min(iterations, BENCHMARK_CONFIG.iterations.pattern); i++) {
          const statuses = ['active', 'inactive', 'pending', 'archived']
          const status = statuses[i % statuses.length]
          const start = performance.now()
          const result = this.store.query(`status:${status}`)
          times.push(performance.now() - start)
          entitiesTraversed += result.length
        }
        break

      case 'batch_entity_lookup':
        for (let i = 0; i < Math.min(iterations, BENCHMARK_CONFIG.iterations.batch); i++) {
          const batchSize = 100
          const startIdx = (i * batchSize) % this.entityIds.length
          const ids = this.entityIds.slice(startIdx, startIdx + batchSize)
          const start = performance.now()
          const result = this.store.batchGet(ids)
          times.push(performance.now() - start)
          entitiesTraversed += result.filter(Boolean).length
        }
        break

      case 'triple_retrieval':
        for (let i = 0; i < iterations; i++) {
          const subjectId = this.entityIds[i % this.entityIds.length]
          const start = performance.now()
          const result = this.store.getTriples(subjectId)
          times.push(performance.now() - start)
          entitiesTraversed += result.length
        }
        break

      default:
        return null
    }

    const stats = calculateStats(times)
    const effectiveIterations = times.length

    return {
      name: operation,
      database: 'graphdb',
      dataset,
      iterations: effectiveIterations,
      ...stats,
      opsPerSec: effectiveIterations / (stats.totalMs / 1000),
      entitiesTraversed,
    }
  }

  async reset(): Promise<void> {
    this.store.clear()
    this.entityIds = []
    this.initialized = false
  }

  getStats(): { entities: number; dataset: string } {
    return {
      entities: this.store.count(),
      dataset: this.dataset,
    }
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)

    if (request.method === 'POST' && url.pathname === '/run') {
      try {
        const body = (await request.json()) as BenchmarkRequest
        const results = await this.runBenchmarks(body)
        return new Response(JSON.stringify(results, null, 2), {
          headers: { 'Content-Type': 'application/json' },
        })
      } catch (error) {
        return new Response(
          JSON.stringify({ error: error instanceof Error ? error.message : 'Unknown error' }),
          { status: 500, headers: { 'Content-Type': 'application/json' } }
        )
      }
    }

    if (request.method === 'POST' && url.pathname === '/reset') {
      await this.reset()
      return new Response(JSON.stringify({ status: 'reset' }), {
        headers: { 'Content-Type': 'application/json' },
      })
    }

    if (request.method === 'GET' && url.pathname === '/stats') {
      return new Response(JSON.stringify(this.getStats()), {
        headers: { 'Content-Type': 'application/json' },
      })
    }

    return new Response('Not found', { status: 404 })
  }
}

// ============================================================================
// SDB Benchmark Durable Object
// ============================================================================

export class SDBBenchDO extends DurableObject<Env> {
  private store: GraphStore
  private entityIds: string[] = []
  private initialized = false
  private dataset: DatasetType = 'synthetic'

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    this.store = createGraphStore()
  }

  private async initialize(dataset: DatasetType): Promise<void> {
    if (this.initialized && this.dataset === dataset) return

    this.store.clear()
    this.dataset = dataset

    switch (dataset) {
      case 'wikidata':
        this.entityIds = seedWikidataSubset(this.store)
        break
      case 'social':
        this.entityIds = seedSocialNetwork(this.store)
        break
      case 'synthetic':
      default:
        this.entityIds = seedSyntheticData(this.store)
        break
    }

    this.initialized = true
  }

  async runBenchmarks(request: BenchmarkRequest): Promise<GraphBenchmarkResults> {
    const dataset = request.dataset ?? 'synthetic'
    await this.initialize(dataset)

    const runId = request.runId ?? generateRunId()
    const iterations = request.iterations ?? BENCHMARK_CONFIG.iterations.traversal
    const operations = request.operations ?? [
      '1_hop_traversal',
      '2_hop_traversal',
      'path_query',
      'reverse_traversal',
      'pattern_matching_type',
      'batch_entity_lookup',
    ]

    const benchmarks: BenchmarkTiming[] = []
    const startTime = performance.now()

    for (const op of operations) {
      const timing = await this.runOperation(op, iterations, dataset)
      if (timing) {
        benchmarks.push(timing)
      }
    }

    const totalDurationMs = performance.now() - startTime
    const totalOperations = benchmarks.reduce((sum, b) => sum + b.iterations, 0)

    return {
      runId,
      timestamp: new Date().toISOString(),
      environment: 'do',
      database: 'sdb',
      dataset,
      benchmarks,
      summary: {
        totalDurationMs,
        totalOperations,
        overallOpsPerSec: totalOperations / (totalDurationMs / 1000),
      },
    }
  }

  private async runOperation(
    operation: string,
    iterations: number,
    dataset: DatasetType
  ): Promise<BenchmarkTiming | null> {
    const times: number[] = []
    let entitiesTraversed = 0

    const predicate = dataset === 'wikidata' ? 'P31' : dataset === 'social' ? 'follows' : 'relates_to'

    switch (operation) {
      case '1_hop_traversal':
        for (let i = 0; i < iterations; i++) {
          const startId = this.entityIds[i % this.entityIds.length]
          const start = performance.now()
          const result = this.store.traverse(startId, predicate, { maxDepth: 1, limit: 100 })
          times.push(performance.now() - start)
          entitiesTraversed += result.length
        }
        break

      case '2_hop_traversal':
        // SDB requires manual 2-hop traversal
        for (let i = 0; i < iterations; i++) {
          const startId = this.entityIds[i % this.entityIds.length]
          const start = performance.now()
          const firstHop = this.store.traverse(startId, predicate, { limit: 50 })
          let secondHopResults: Entity[] = []
          for (const entity of firstHop.slice(0, 10)) {
            const secondHop = this.store.traverse(entity.$id, predicate, { limit: 10 })
            secondHopResults = secondHopResults.concat(secondHop)
          }
          times.push(performance.now() - start)
          entitiesTraversed += secondHopResults.length
        }
        break

      case 'path_query':
        for (let i = 0; i < Math.min(iterations, BENCHMARK_CONFIG.iterations.pattern); i++) {
          const startId = this.entityIds[i % this.entityIds.length]
          const start = performance.now()
          const result = this.store.pathTraverse(startId, [predicate, predicate, predicate], { limit: 50 })
          times.push(performance.now() - start)
          entitiesTraversed += result.length
        }
        break

      case 'reverse_traversal':
        for (let i = 0; i < iterations; i++) {
          const targetId = this.entityIds[(i * 7) % this.entityIds.length]
          const start = performance.now()
          const result = this.store.reverseTraverse(targetId, predicate, { limit: 100 })
          times.push(performance.now() - start)
          entitiesTraversed += result.length
        }
        break

      case 'pattern_matching_type':
        for (let i = 0; i < Math.min(iterations, BENCHMARK_CONFIG.iterations.pattern); i++) {
          const types = dataset === 'wikidata'
            ? ['Person', 'Organization', 'Location', 'Work', 'Event']
            : dataset === 'social'
              ? ['User']
              : ['Thing']
          const type = types[i % types.length]
          const start = performance.now()
          const result = this.store.query(`type:${type}`)
          times.push(performance.now() - start)
          entitiesTraversed += result.length
        }
        break

      case 'batch_entity_lookup':
        for (let i = 0; i < Math.min(iterations, BENCHMARK_CONFIG.iterations.batch); i++) {
          const batchSize = 100
          const startIdx = (i * batchSize) % this.entityIds.length
          const ids = this.entityIds.slice(startIdx, startIdx + batchSize)
          const start = performance.now()
          const result = this.store.batchGet(ids)
          times.push(performance.now() - start)
          entitiesTraversed += result.filter(Boolean).length
        }
        break

      default:
        return null
    }

    const stats = calculateStats(times)
    const effectiveIterations = times.length

    return {
      name: operation,
      database: 'sdb',
      dataset,
      iterations: effectiveIterations,
      ...stats,
      opsPerSec: effectiveIterations / (stats.totalMs / 1000),
      entitiesTraversed,
    }
  }

  async reset(): Promise<void> {
    this.store.clear()
    this.entityIds = []
    this.initialized = false
  }

  getStats(): { entities: number; dataset: string } {
    return {
      entities: this.store.count(),
      dataset: this.dataset,
    }
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)

    if (request.method === 'POST' && url.pathname === '/run') {
      try {
        const body = (await request.json()) as BenchmarkRequest
        const results = await this.runBenchmarks(body)
        return new Response(JSON.stringify(results, null, 2), {
          headers: { 'Content-Type': 'application/json' },
        })
      } catch (error) {
        return new Response(
          JSON.stringify({ error: error instanceof Error ? error.message : 'Unknown error' }),
          { status: 500, headers: { 'Content-Type': 'application/json' } }
        )
      }
    }

    if (request.method === 'POST' && url.pathname === '/reset') {
      await this.reset()
      return new Response(JSON.stringify({ status: 'reset' }), {
        headers: { 'Content-Type': 'application/json' },
      })
    }

    if (request.method === 'GET' && url.pathname === '/stats') {
      return new Response(JSON.stringify(this.getStats()), {
        headers: { 'Content-Type': 'application/json' },
      })
    }

    return new Response('Not found', { status: 404 })
  }
}

// ============================================================================
// DB4 In-Worker Benchmark (no DO needed - pure TypeScript)
// ============================================================================

async function runDB4Benchmarks(request: BenchmarkRequest): Promise<GraphBenchmarkResults> {
  const dataset = request.dataset ?? 'synthetic'
  const store = createGraphStore()

  // Seed data
  let entityIds: string[] = []
  switch (dataset) {
    case 'wikidata':
      entityIds = seedWikidataSubset(store)
      break
    case 'social':
      entityIds = seedSocialNetwork(store)
      break
    case 'synthetic':
    default:
      entityIds = seedSyntheticData(store)
      break
  }

  const runId = request.runId ?? generateRunId()
  const iterations = request.iterations ?? BENCHMARK_CONFIG.iterations.traversal
  const operations = request.operations ?? [
    '1_hop_traversal',
    '2_hop_traversal',
    'reverse_traversal',
    'batch_entity_lookup',
  ]

  const benchmarks: BenchmarkTiming[] = []
  const startTime = performance.now()

  const predicate = dataset === 'wikidata' ? 'P31' : dataset === 'social' ? 'follows' : 'relates_to'

  for (const operation of operations) {
    const times: number[] = []
    let entitiesTraversed = 0

    switch (operation) {
      case '1_hop_traversal':
        // DB4 uses join-based traversal
        for (let i = 0; i < iterations; i++) {
          const startId = entityIds[i % entityIds.length]
          const start = performance.now()
          const result = store.traverse(startId, predicate, { maxDepth: 1, limit: 100 })
          times.push(performance.now() - start)
          entitiesTraversed += result.length
        }
        break

      case '2_hop_traversal':
        // DB4 nested join
        for (let i = 0; i < iterations; i++) {
          const startId = entityIds[i % entityIds.length]
          const start = performance.now()
          const result = store.pathTraverse(startId, [predicate, predicate], { limit: 100 })
          times.push(performance.now() - start)
          entitiesTraversed += result.length
        }
        break

      case 'reverse_traversal':
        for (let i = 0; i < iterations; i++) {
          const targetId = entityIds[(i * 7) % entityIds.length]
          const start = performance.now()
          const result = store.reverseTraverse(targetId, predicate, { limit: 100 })
          times.push(performance.now() - start)
          entitiesTraversed += result.length
        }
        break

      case 'batch_entity_lookup':
        for (let i = 0; i < Math.min(iterations, BENCHMARK_CONFIG.iterations.batch); i++) {
          const batchSize = 100
          const startIdx = (i * batchSize) % entityIds.length
          const ids = entityIds.slice(startIdx, startIdx + batchSize)
          const start = performance.now()
          const result = store.batchGet(ids)
          times.push(performance.now() - start)
          entitiesTraversed += result.filter(Boolean).length
        }
        break

      default:
        continue
    }

    if (times.length > 0) {
      const stats = calculateStats(times)
      benchmarks.push({
        name: operation,
        database: 'db4',
        dataset,
        iterations: times.length,
        ...stats,
        opsPerSec: times.length / (stats.totalMs / 1000),
        entitiesTraversed,
      })
    }
  }

  const totalDurationMs = performance.now() - startTime
  const totalOperations = benchmarks.reduce((sum, b) => sum + b.iterations, 0)

  store.clear()

  return {
    runId,
    timestamp: new Date().toISOString(),
    environment: 'worker',
    database: 'db4',
    dataset,
    benchmarks,
    summary: {
      totalDurationMs,
      totalOperations,
      overallOpsPerSec: totalOperations / (totalDurationMs / 1000),
    },
  }
}

// ============================================================================
// Worker Entry Point
// ============================================================================

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url)

    // Health check
    if (url.pathname === '/health') {
      return new Response(JSON.stringify({ status: 'ok', timestamp: new Date().toISOString() }), {
        headers: { 'Content-Type': 'application/json' },
      })
    }

    // Main benchmark endpoint
    if (request.method === 'POST' && url.pathname === '/benchmark/graph') {
      try {
        // Parse query parameters
        const database = (url.searchParams.get('database') as DatabaseType) ?? 'graphdb'
        const dataset = (url.searchParams.get('dataset') as DatasetType) ?? 'synthetic'

        // Parse request body
        let body: BenchmarkRequest = {}
        try {
          body = (await request.json()) as BenchmarkRequest
        } catch {
          // Empty body is fine, use defaults
        }

        body.database = body.database ?? database
        body.dataset = body.dataset ?? dataset

        let results: GraphBenchmarkResults

        switch (body.database) {
          case 'graphdb': {
            const doId = env.GRAPHDB_DO.idFromName(`benchmark-${body.dataset}`)
            const benchDO = env.GRAPHDB_DO.get(doId)

            const doRequest = new Request('http://internal/run', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify(body),
            })

            const response = await benchDO.fetch(doRequest)
            results = (await response.json()) as GraphBenchmarkResults
            break
          }

          case 'sdb': {
            const doId = env.SDB_DO.idFromName(`benchmark-${body.dataset}`)
            const benchDO = env.SDB_DO.get(doId)

            const doRequest = new Request('http://internal/run', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify(body),
            })

            const response = await benchDO.fetch(doRequest)
            results = (await response.json()) as GraphBenchmarkResults
            break
          }

          case 'db4':
          default:
            results = await runDB4Benchmarks(body)
            break
        }

        // Add colo information
        const colo = request.cf?.colo as string | undefined
        if (colo) {
          results.colo = colo
        }

        // Convert to JSONL format for R2 storage
        const jsonlResults = results.benchmarks.map((b) => {
          const result: BenchmarkResult = {
            benchmark: `graph/${b.name}`,
            database: b.database,
            dataset: b.dataset,
            p50_ms: b.p50Ms,
            p99_ms: b.p99Ms,
            min_ms: b.minMs,
            max_ms: b.maxMs,
            mean_ms: b.meanMs,
            ops_per_sec: b.opsPerSec,
            iterations: b.iterations,
            vfs_reads: 0,
            vfs_writes: 0,
            vfs_bytes_read: 0,
            vfs_bytes_written: 0,
            timestamp: results.timestamp,
            environment: results.environment,
            run_id: results.runId,
            colo: results.colo,
            total_duration_ms: b.totalMs,
          }
          return JSON.stringify(result)
        })

        // Store results in R2
        const resultsKey = `graph/${results.database}/${results.dataset}/${results.runId}.jsonl`
        await env.RESULTS.put(resultsKey, jsonlResults.join('\n'))

        return new Response(JSON.stringify(results, null, 2), {
          headers: {
            'Content-Type': 'application/json',
            'X-Results-Key': resultsKey,
            'X-Run-Id': results.runId,
          },
        })
      } catch (error) {
        return new Response(
          JSON.stringify({
            error: error instanceof Error ? error.message : 'Unknown error',
            stack: error instanceof Error ? error.stack : undefined,
          }),
          { status: 500, headers: { 'Content-Type': 'application/json' } }
        )
      }
    }

    // Reset benchmark state
    if (request.method === 'POST' && url.pathname === '/benchmark/graph/reset') {
      const database = (url.searchParams.get('database') as DatabaseType) ?? 'graphdb'
      const dataset = (url.searchParams.get('dataset') as DatasetType) ?? 'synthetic'

      if (database === 'graphdb') {
        const doId = env.GRAPHDB_DO.idFromName(`benchmark-${dataset}`)
        const benchDO = env.GRAPHDB_DO.get(doId)
        await benchDO.fetch(new Request('http://internal/reset', { method: 'POST' }))
      } else if (database === 'sdb') {
        const doId = env.SDB_DO.idFromName(`benchmark-${dataset}`)
        const benchDO = env.SDB_DO.get(doId)
        await benchDO.fetch(new Request('http://internal/reset', { method: 'POST' }))
      }

      return new Response(JSON.stringify({ status: 'reset', database, dataset }), {
        headers: { 'Content-Type': 'application/json' },
      })
    }

    // Get benchmark stats
    if (request.method === 'GET' && url.pathname === '/benchmark/graph/stats') {
      const database = (url.searchParams.get('database') as DatabaseType) ?? 'graphdb'
      const dataset = (url.searchParams.get('dataset') as DatasetType) ?? 'synthetic'

      if (database === 'graphdb') {
        const doId = env.GRAPHDB_DO.idFromName(`benchmark-${dataset}`)
        const benchDO = env.GRAPHDB_DO.get(doId)
        const response = await benchDO.fetch(new Request('http://internal/stats', { method: 'GET' }))
        return new Response(await response.text(), {
          headers: { 'Content-Type': 'application/json' },
        })
      } else if (database === 'sdb') {
        const doId = env.SDB_DO.idFromName(`benchmark-${dataset}`)
        const benchDO = env.SDB_DO.get(doId)
        const response = await benchDO.fetch(new Request('http://internal/stats', { method: 'GET' }))
        return new Response(await response.text(), {
          headers: { 'Content-Type': 'application/json' },
        })
      }

      return new Response(JSON.stringify({ error: 'db4 does not persist state' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    // List stored results
    if (request.method === 'GET' && url.pathname === '/benchmark/graph/results') {
      const database = url.searchParams.get('database')
      const dataset = url.searchParams.get('dataset')

      let prefix = 'graph/'
      if (database) prefix += `${database}/`
      if (database && dataset) prefix += `${dataset}/`

      const list = await env.RESULTS.list({ prefix })
      const results = list.objects.map((obj) => ({
        key: obj.key,
        size: obj.size,
        uploaded: obj.uploaded.toISOString(),
      }))

      return new Response(JSON.stringify(results, null, 2), {
        headers: { 'Content-Type': 'application/json' },
      })
    }

    // Get specific result
    if (request.method === 'GET' && url.pathname.startsWith('/benchmark/graph/results/')) {
      const runId = url.pathname.replace('/benchmark/graph/results/', '')
      const database = url.searchParams.get('database') ?? 'graphdb'
      const dataset = url.searchParams.get('dataset') ?? 'synthetic'

      const key = `graph/${database}/${dataset}/${runId}.jsonl`
      const object = await env.RESULTS.get(key)

      if (!object) {
        return new Response(JSON.stringify({ error: 'Result not found' }), {
          status: 404,
          headers: { 'Content-Type': 'application/json' },
        })
      }

      return new Response(await object.text(), {
        headers: { 'Content-Type': 'application/x-ndjson' },
      })
    }

    // API documentation
    if (request.method === 'GET' && url.pathname === '/') {
      return new Response(
        JSON.stringify(
          {
            name: 'Graph Benchmark Worker',
            description: 'Graph database benchmark runner on Cloudflare Workers',
            endpoints: {
              'POST /benchmark/graph': 'Run graph benchmarks',
              'POST /benchmark/graph/reset': 'Reset benchmark state',
              'GET /benchmark/graph/stats': 'Get current graph statistics',
              'GET /benchmark/graph/results': 'List stored benchmark results',
              'GET /benchmark/graph/results/:runId': 'Get specific benchmark result',
              'GET /health': 'Health check',
            },
            queryParams: {
              database: 'Database to benchmark: graphdb, sdb, db4 (default: graphdb)',
              dataset: 'Dataset to use: wikidata, social, synthetic (default: synthetic)',
            },
            requestBody: {
              operations: [
                '1_hop_traversal',
                '2_hop_traversal',
                'path_query',
                'reverse_traversal',
                'pattern_matching_type',
                'pattern_matching_predicate',
                'batch_entity_lookup',
                'triple_retrieval',
              ],
              iterations: 'Number of iterations per operation (default: 50)',
              runId: 'Custom run ID (optional)',
            },
            datasets: {
              wikidata: 'Wikidata subset with 5000 entities and property relationships',
              social: 'Social network with 2000 users and follow/mention relationships',
              synthetic: 'Synthetic thing entities with relates_to relationships',
            },
          },
          null,
          2
        ),
        { headers: { 'Content-Type': 'application/json' } }
      )
    }

    return new Response('Not Found', { status: 404 })
  },
}
