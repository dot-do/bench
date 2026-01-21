/**
 * @dotdo/graphdb Database Adapter
 *
 * Graph database with:
 * - Triple store (subject, predicate, object)
 * - WebSocket connections with hibernation
 * - SQLite-backed DOs
 * - R2 lakehouse storage
 *
 * Architecture:
 * - BrokerDO: Hibernating WebSocket connections (95% cost savings)
 * - ShardDO: SQLite triple storage with typed object columns
 * - R2: CDC streaming with GraphCol format
 */

// Types aligned with graphdb core types
export interface Triple {
  subject: string
  predicate: string
  object: TypedObject
}

export type ObjectType =
  | 'NULL'
  | 'BOOL'
  | 'INT32'
  | 'INT64'
  | 'FLOAT64'
  | 'STRING'
  | 'BINARY'
  | 'TIMESTAMP'
  | 'DATE'
  | 'DURATION'
  | 'REF'
  | 'REF_ARRAY'
  | 'JSON'
  | 'GEO_POINT'
  | 'GEO_POLYGON'
  | 'GEO_LINESTRING'
  | 'VECTOR'

export interface TypedObject {
  type: ObjectType
  value: unknown
}

export interface Entity {
  $id: string
  $type: string
  [key: string]: unknown
}

export interface TraversalOptions {
  maxDepth?: number
  limit?: number
  predicateFilter?: string[]
}

export interface QueryResult {
  entities: Entity[]
  hasMore: boolean
  stats: {
    triplesScanned: number
    entitiesReturned: number
    durationMs: number
  }
}

export interface BatchResult<T> {
  results: T[]
  errors: Array<{ index: number; error: string }>
}

export type DatasetSize = 'small' | 'medium' | 'large'

export interface GraphDBStore {
  // Triple operations
  insertTriple(triple: Triple): Promise<void>
  insertTriples(triples: Triple[]): Promise<void>
  getTriples(subject: string): Promise<Triple[]>
  getTriplesByPredicate(predicate: string, limit?: number): Promise<Triple[]>
  deleteTriple(subject: string, predicate: string): Promise<boolean>

  // Entity operations (high-level)
  insert(entity: Entity): Promise<void>
  get(id: string): Promise<Entity | null>
  query(queryString: string): Promise<QueryResult>
  update(id: string, props: Record<string, unknown>): Promise<void>
  delete(id: string): Promise<boolean>

  // Graph traversal
  traverse(startId: string, predicate: string, options?: TraversalOptions): Promise<Entity[]>
  reverseTraverse(targetId: string, predicate: string, options?: TraversalOptions): Promise<Entity[]>
  pathTraverse(startId: string, path: string[], options?: TraversalOptions): Promise<Entity[]>

  // Batch operations
  batchGet(ids: string[]): Promise<BatchResult<Entity | null>>
  batchInsert(entities: Entity[]): Promise<BatchResult<void>>

  // Statistics
  count(): Promise<number>
  getStats(): Promise<{ triples: number; entities: number; predicates: number }>

  // Lifecycle
  close(): Promise<void>
}

// In-memory store for benchmarking
interface InMemoryTripleStore {
  triples: Map<string, Map<string, TypedObject>> // subject -> predicate -> object
  predicateIndex: Map<string, Set<string>> // predicate -> subjects
  reverseIndex: Map<string, Set<string>> // object (if REF) -> subjects
}

function createInMemoryStore(): InMemoryTripleStore {
  return {
    triples: new Map(),
    predicateIndex: new Map(),
    reverseIndex: new Map(),
  }
}

/**
 * Create a new GraphDB store instance.
 * For benchmarking, this uses an in-memory implementation.
 * In production, you would connect to the actual GraphDB worker via WebSocket.
 */
export async function createGraphDBStore(): Promise<GraphDBStore> {
  const store = createInMemoryStore()

  const insertTriple = async (triple: Triple): Promise<void> => {
    // Get or create subject map
    let predicateMap = store.triples.get(triple.subject)
    if (!predicateMap) {
      predicateMap = new Map()
      store.triples.set(triple.subject, predicateMap)
    }

    // Store the object
    predicateMap.set(triple.predicate, triple.object)

    // Update predicate index
    let subjects = store.predicateIndex.get(triple.predicate)
    if (!subjects) {
      subjects = new Set()
      store.predicateIndex.set(triple.predicate, subjects)
    }
    subjects.add(triple.subject)

    // Update reverse index for REF types
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
    async insertTriple(triple: Triple): Promise<void> {
      await insertTriple(triple)
    },

    async insertTriples(triples: Triple[]): Promise<void> {
      for (const triple of triples) {
        await insertTriple(triple)
      }
    },

    async getTriples(subject: string): Promise<Triple[]> {
      const predicateMap = store.triples.get(subject)
      if (!predicateMap) return []

      const triples: Triple[] = []
      for (const [predicate, object] of predicateMap) {
        triples.push({ subject, predicate, object })
      }
      return triples
    },

    async getTriplesByPredicate(predicate: string, limit?: number): Promise<Triple[]> {
      const subjects = store.predicateIndex.get(predicate)
      if (!subjects) return []

      const triples: Triple[] = []
      let count = 0
      for (const subject of subjects) {
        if (limit && count >= limit) break
        const predicateMap = store.triples.get(subject)
        const object = predicateMap?.get(predicate)
        if (object) {
          triples.push({ subject, predicate, object })
          count++
        }
      }
      return triples
    },

    async deleteTriple(subject: string, predicate: string): Promise<boolean> {
      const predicateMap = store.triples.get(subject)
      if (!predicateMap) return false

      const object = predicateMap.get(predicate)
      if (!object) return false

      predicateMap.delete(predicate)

      // Clean up indexes
      const subjects = store.predicateIndex.get(predicate)
      subjects?.delete(subject)

      if (object.type === 'REF' && typeof object.value === 'string') {
        const refSubjects = store.reverseIndex.get(object.value)
        refSubjects?.delete(subject)
      }

      return true
    },

    async insert(entity: Entity): Promise<void> {
      // Insert $type triple
      await insertTriple({
        subject: entity.$id,
        predicate: '$type',
        object: { type: 'STRING', value: entity.$type },
      })

      // Insert property triples
      for (const [key, value] of Object.entries(entity)) {
        if (key === '$id' || key === '$type') continue

        let object: TypedObject
        if (typeof value === 'string') {
          // Check if it looks like an entity reference
          if (value.startsWith('entity:') || value.startsWith('thing-') || value.startsWith('user:')) {
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

        await insertTriple({
          subject: entity.$id,
          predicate: key,
          object,
        })
      }
    },

    async get(id: string): Promise<Entity | null> {
      return getEntity(id)
    },

    async query(queryString: string): Promise<QueryResult> {
      const startTime = performance.now()
      const entities: Entity[] = []

      // Simple query parsing
      // Format: "type:TypeName" or "predicate:value"
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
      } else {
        // Return all entities
        for (const subject of store.triples.keys()) {
          const entity = getEntity(subject)
          if (entity) entities.push(entity)
        }
      }

      const durationMs = performance.now() - startTime

      return {
        entities,
        hasMore: false,
        stats: {
          triplesScanned: store.triples.size,
          entitiesReturned: entities.length,
          durationMs,
        },
      }
    },

    async update(id: string, props: Record<string, unknown>): Promise<void> {
      for (const [key, value] of Object.entries(props)) {
        let object: TypedObject
        if (typeof value === 'string') {
          object = { type: 'STRING', value }
        } else if (typeof value === 'number') {
          object = { type: 'FLOAT64', value }
        } else if (typeof value === 'boolean') {
          object = { type: 'BOOL', value }
        } else if (value === null) {
          object = { type: 'NULL', value: null }
        } else {
          object = { type: 'JSON', value }
        }

        await insertTriple({ subject: id, predicate: key, object })
      }
    },

    async delete(id: string): Promise<boolean> {
      const predicateMap = store.triples.get(id)
      if (!predicateMap) return false

      // Clean up indexes
      for (const [predicate, object] of predicateMap) {
        const subjects = store.predicateIndex.get(predicate)
        subjects?.delete(id)

        if (object.type === 'REF' && typeof object.value === 'string') {
          const refSubjects = store.reverseIndex.get(object.value)
          refSubjects?.delete(id)
        }
      }

      store.triples.delete(id)
      return true
    },

    async traverse(startId: string, predicate: string, options?: TraversalOptions): Promise<Entity[]> {
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

    async reverseTraverse(targetId: string, predicate: string, options?: TraversalOptions): Promise<Entity[]> {
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

    async pathTraverse(startId: string, path: string[], options?: TraversalOptions): Promise<Entity[]> {
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

    async batchGet(ids: string[]): Promise<BatchResult<Entity | null>> {
      const results: (Entity | null)[] = []
      const errors: Array<{ index: number; error: string }> = []

      for (let i = 0; i < ids.length; i++) {
        try {
          results.push(getEntity(ids[i]))
        } catch (e: unknown) {
          results.push(null)
          errors.push({ index: i, error: String(e) })
        }
      }

      return { results, errors }
    },

    async batchInsert(entities: Entity[]): Promise<BatchResult<void>> {
      const results: void[] = []
      const errors: Array<{ index: number; error: string }> = []

      for (let i = 0; i < entities.length; i++) {
        try {
          // Insert $type triple
          await insertTriple({
            subject: entities[i].$id,
            predicate: '$type',
            object: { type: 'STRING', value: entities[i].$type },
          })

          // Insert property triples
          for (const [key, value] of Object.entries(entities[i])) {
            if (key === '$id' || key === '$type') continue

            let object: TypedObject
            if (typeof value === 'string') {
              if (value.startsWith('entity:') || value.startsWith('thing-') || value.startsWith('user:')) {
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

            await insertTriple({ subject: entities[i].$id, predicate: key, object })
          }
          results.push(undefined)
        } catch (e: unknown) {
          errors.push({ index: i, error: String(e) })
        }
      }

      return { results, errors }
    },

    async count(): Promise<number> {
      return store.triples.size
    },

    async getStats(): Promise<{ triples: number; entities: number; predicates: number }> {
      let tripleCount = 0
      for (const predicateMap of store.triples.values()) {
        tripleCount += predicateMap.size
      }

      return {
        triples: tripleCount,
        entities: store.triples.size,
        predicates: store.predicateIndex.size,
      }
    },

    async close(): Promise<void> {
      store.triples.clear()
      store.predicateIndex.clear()
      store.reverseIndex.clear()
    },
  }
}

/**
 * Seed the store with test data based on dataset size.
 */
export async function seedTestData(store: GraphDBStore, size: DatasetSize = 'medium'): Promise<void> {
  const counts = {
    small: { users: 100, things: 500, relationships: 250 },
    medium: { users: 500, things: 1000, relationships: 500 },
    large: { users: 2000, things: 5000, relationships: 2500 },
  }

  const { users, things, relationships } = counts[size]
  const statuses = ['active', 'inactive', 'pending', 'archived']

  // Insert users
  for (let i = 0; i < users; i++) {
    await store.insert({
      $id: `user:${i}`,
      $type: 'User',
      name: `User ${i}`,
      email: `user${i}@example.com`,
      status: statuses[i % statuses.length],
      created_at: new Date(Date.now() - i * 60000).toISOString(),
    })
  }

  // Insert things
  for (let i = 0; i < things; i++) {
    await store.insert({
      $id: `thing-${String(i).padStart(4, '0')}`,
      $type: 'Thing',
      name: `Thing ${i}`,
      status: statuses[i % statuses.length],
      owner: `user:${i % users}`,
      created_at: new Date(Date.now() - i * 60000).toISOString(),
    })
  }

  // Insert relationships as triples
  for (let i = 0; i < relationships; i++) {
    const subjectIdx = i % things
    const objectIdx = (i + 1) % things

    await store.insertTriple({
      subject: `thing-${String(subjectIdx).padStart(4, '0')}`,
      predicate: 'relates_to',
      object: { type: 'REF', value: `thing-${String(objectIdx).padStart(4, '0')}` },
    })

    // Also create user->thing relationships
    await store.insertTriple({
      subject: `user:${i % users}`,
      predicate: 'owns',
      object: { type: 'REF', value: `thing-${String(i % things).padStart(4, '0')}` },
    })
  }
}

/**
 * Create a WebSocket-connected GraphDB client.
 * This connects to the actual graphdb worker for production use.
 */
export async function createGraphClient(wsUrl: string): Promise<GraphDBStore> {
  // Lazy import the actual client
  const { createGraphClient: createClient } = await import('@dotdo/graphdb/client')

  const client = createClient(wsUrl)

  // Wrap the client to match our store interface
  return {
    async insertTriple(triple: Triple): Promise<void> {
      await client.insert({
        $id: triple.subject,
        $type: 'Triple',
        predicate: triple.predicate,
        object: triple.object,
      })
    },

    async insertTriples(triples: Triple[]): Promise<void> {
      await client.batchInsert(
        triples.map((t) => ({
          $id: t.subject,
          $type: 'Triple',
          predicate: t.predicate,
          object: t.object,
        }))
      )
    },

    async getTriples(subject: string): Promise<Triple[]> {
      const result = await client.query(`subject:${subject}`)
      if ('entities' in result) {
        return result.entities.map((e: Entity) => ({
          subject: e.$id,
          predicate: (e as Record<string, unknown>)['predicate'] as string,
          object: (e as Record<string, unknown>)['object'] as TypedObject,
        }))
      }
      return []
    },

    async getTriplesByPredicate(_predicate: string, _limit?: number): Promise<Triple[]> {
      // This would need a proper query implementation
      return []
    },

    async deleteTriple(subject: string, _predicate: string): Promise<boolean> {
      await client.delete(subject)
      return true
    },

    async insert(entity: Entity): Promise<void> {
      await client.insert(entity)
    },

    async get(id: string): Promise<Entity | null> {
      const result = await client.query(id)
      if (result && '$id' in result) {
        return result as Entity
      }
      return null
    },

    async query(queryString: string): Promise<QueryResult> {
      const result = await client.query(queryString)
      if ('entities' in result) {
        return result as QueryResult
      }
      return {
        entities: result ? [result as Entity] : [],
        hasMore: false,
        stats: { triplesScanned: 0, entitiesReturned: result ? 1 : 0, durationMs: 0 },
      }
    },

    async update(id: string, props: Record<string, unknown>): Promise<void> {
      await client.update(id, props)
    },

    async delete(id: string): Promise<boolean> {
      await client.delete(id)
      return true
    },

    async traverse(startId: string, predicate: string, options?: TraversalOptions): Promise<Entity[]> {
      return client.traverse(startId, predicate, options)
    },

    async reverseTraverse(targetId: string, predicate: string, options?: TraversalOptions): Promise<Entity[]> {
      return client.reverseTraverse(targetId, predicate, options)
    },

    async pathTraverse(startId: string, path: string[], options?: TraversalOptions): Promise<Entity[]> {
      return client.pathTraverse(startId, path, options)
    },

    async batchGet(ids: string[]): Promise<BatchResult<Entity | null>> {
      return client.batchGet(ids)
    },

    async batchInsert(entities: Entity[]): Promise<BatchResult<void>> {
      return client.batchInsert(entities)
    },

    async count(): Promise<number> {
      // Would need stats endpoint
      return 0
    },

    async getStats(): Promise<{ triples: number; entities: number; predicates: number }> {
      return { triples: 0, entities: 0, predicates: 0 }
    },

    async close(): Promise<void> {
      client.close()
    },
  }
}
