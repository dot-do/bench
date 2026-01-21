/**
 * @dotdo/sdb Database Adapter
 *
 * Document/graph database with:
 * - Schema-based documents
 * - Graph relationships (-> syntax)
 * - WebSocket subscriptions
 * - React hooks
 *
 * Features:
 * - WebSocket-first with HTTP fallback
 * - Automatic promise pipelining via capnweb
 * - Chainable RpcPromise (db.Users.abc.posts.first.author)
 * - Server-side transforms (.map(), .filter(), .slice())
 * - Type-safe schema inference
 */

// Types aligned with sdb types
export interface Thing {
  $id: string
  $type: string
  [key: string]: unknown
}

export interface ListResponse<T> {
  data: T[]
  total: number
  hasMore: boolean
  cursor?: string
}

export interface CountResponse {
  count: number
}

export interface FilterOptions {
  [key: string]: unknown
}

export interface ListOptions {
  limit?: number
  offset?: number
  cursor?: string
  orderBy?: string
  order?: 'asc' | 'desc'
}

export interface SchemaDefinition {
  [typeName: string]: TypeSchema
}

export interface TypeSchema {
  [fieldName: string]: FieldType
}

export type FieldType =
  | 'string'
  | 'text'
  | 'number'
  | 'boolean'
  | 'date'
  | 'json'
  | `-> ${string}` // Forward reference
  | `<- ${string}` // Reverse reference
  | `-> ${string}[]` // Forward array reference
  | `<- ${string}[]` // Reverse array reference

export type DatasetSize = 'small' | 'medium' | 'large'

export interface SDBStore<T extends SchemaDefinition = SchemaDefinition> {
  // Collection operations
  collection<K extends keyof T>(name: K): CollectionProxy<InferEntity<T[K]>>

  // Direct thing operations
  get(type: string, id: string): Promise<Thing | null>
  create(type: string, data: Partial<Thing>): Promise<Thing>
  update(type: string, id: string, data: Partial<Thing>): Promise<Thing>
  delete(type: string, id: string): Promise<boolean>

  // List and query
  list(type: string, options?: ListOptions): Promise<ListResponse<Thing>>
  find(type: string, filter: FilterOptions, options?: ListOptions): Promise<ListResponse<Thing>>
  count(type: string, filter?: FilterOptions): Promise<CountResponse>

  // Relationship operations
  link(fromId: string, predicate: string, toId: string): Promise<void>
  unlink(fromId: string, predicate: string, toId: string): Promise<void>
  traverse(fromId: string, predicate: string, options?: ListOptions): Promise<Thing[]>

  // Batch operations
  batch(): BatchBuilder<T>
  bulkCreate(type: string, items: Array<{ $id: string; data: Partial<Thing> }>): Promise<{ buffered: number }>

  // Schema
  schema: T

  // Lifecycle
  close(): void
}

export interface CollectionProxy<T> {
  (id: string): Promise<T | null>
  (filter: FilterOptions): Promise<ListResponse<T>>

  list(options?: ListOptions): Promise<ListResponse<T>>
  get(id: string): Promise<T | null>
  create(data: Partial<T>): Promise<T>
  count(filter?: FilterOptions): Promise<CountResponse>
  find(filter: FilterOptions, options?: ListOptions): Promise<ListResponse<T>>

  // Transforms
  map<K extends keyof T>(key: K): Promise<Array<T[K]>>
  filter(fn: (item: T) => boolean): Promise<T[]>
  slice(start: number, end?: number): Promise<T[]>
  first: Promise<T | undefined>
  last: Promise<T | undefined>
}

export interface BatchBuilder<T extends SchemaDefinition = SchemaDefinition> {
  create<K extends keyof T>(type: K, data: Partial<InferEntity<T[K]>>): BatchBuilder<T>
  update<K extends keyof T>(type: K, id: string, data: Partial<InferEntity<T[K]>>): BatchBuilder<T>
  delete<K extends keyof T>(type: K, id: string): BatchBuilder<T>
  link(fromId: string, predicate: string, toId: string): BatchBuilder<T>
  unlink(fromId: string, predicate: string, toId: string): BatchBuilder<T>
  commit(): Promise<unknown[]>
}

// Type inference helper
type InferEntity<T extends TypeSchema> = Thing & {
  [K in keyof T]?: InferFieldType<T[K]>
}

type InferFieldType<F extends FieldType> = F extends 'string'
  ? string
  : F extends 'text'
    ? string
    : F extends 'number'
      ? number
      : F extends 'boolean'
        ? boolean
        : F extends 'date'
          ? string
          : F extends 'json'
            ? unknown
            : F extends `-> ${string}`
              ? Thing
              : F extends `<- ${string}`
                ? Thing[]
                : F extends `-> ${string}[]`
                  ? Thing[]
                  : F extends `<- ${string}[]`
                    ? Thing[]
                    : unknown

// In-memory store for benchmarking
interface InMemoryDocStore {
  documents: Map<string, Map<string, Thing>> // type -> id -> document
  relationships: Map<string, Map<string, Set<string>>> // fromId -> predicate -> toIds
  reverseRelationships: Map<string, Map<string, Set<string>>> // toId -> predicate -> fromIds
}

function createInMemoryDocStore(): InMemoryDocStore {
  return {
    documents: new Map(),
    relationships: new Map(),
    reverseRelationships: new Map(),
  }
}

function pluralize(name: string): string {
  if (name.endsWith('s')) return name
  if (name.endsWith('y')) return name.slice(0, -1) + 'ies'
  return name + 's'
}

/**
 * Create a new SDB store instance.
 * For benchmarking, this uses an in-memory implementation.
 * In production, you would connect to the actual SDB worker via WebSocket.
 */
export function DB<T extends SchemaDefinition>(
  schema: T,
  _config?: { url?: string }
): SDBStore<T> {
  const store = createInMemoryDocStore()

  const getDocument = (type: string, id: string): Thing | null => {
    const typeStore = store.documents.get(type.toLowerCase())
    return typeStore?.get(id) ?? null
  }

  const setDocument = (type: string, id: string, data: Thing): void => {
    const lowerType = type.toLowerCase()
    let typeStore = store.documents.get(lowerType)
    if (!typeStore) {
      typeStore = new Map()
      store.documents.set(lowerType, typeStore)
    }
    typeStore.set(id, { ...data, $id: id, $type: type })
  }

  const deleteDocument = (type: string, id: string): boolean => {
    const typeStore = store.documents.get(type.toLowerCase())
    if (!typeStore) return false
    return typeStore.delete(id)
  }

  const listDocuments = (type: string, options?: ListOptions): ListResponse<Thing> => {
    const typeStore = store.documents.get(type.toLowerCase())
    if (!typeStore) return { data: [], total: 0, hasMore: false }

    let items = Array.from(typeStore.values())
    const total = items.length

    // Apply ordering
    if (options?.orderBy) {
      const order = options.order === 'desc' ? -1 : 1
      items.sort((a, b) => {
        const aVal = a[options.orderBy!]
        const bVal = b[options.orderBy!]
        if (aVal === bVal) return 0
        if (aVal === undefined || aVal === null) return 1
        if (bVal === undefined || bVal === null) return -1
        return (aVal as string | number) < (bVal as string | number) ? -1 * order : order
      })
    }

    // Apply pagination
    const offset = options?.offset ?? 0
    const limit = options?.limit ?? 100
    items = items.slice(offset, offset + limit)

    return {
      data: items,
      total,
      hasMore: offset + items.length < total,
    }
  }

  const findDocuments = (
    type: string,
    filter: FilterOptions,
    options?: ListOptions
  ): ListResponse<Thing> => {
    const typeStore = store.documents.get(type.toLowerCase())
    if (!typeStore) return { data: [], total: 0, hasMore: false }

    let items = Array.from(typeStore.values()).filter((doc) => {
      for (const [key, value] of Object.entries(filter)) {
        if (doc[key] !== value) return false
      }
      return true
    })

    const total = items.length

    // Apply ordering
    if (options?.orderBy) {
      const order = options.order === 'desc' ? -1 : 1
      items.sort((a, b) => {
        const aVal = a[options.orderBy!]
        const bVal = b[options.orderBy!]
        if (aVal === bVal) return 0
        if (aVal === undefined || aVal === null) return 1
        if (bVal === undefined || bVal === null) return -1
        return (aVal as string | number) < (bVal as string | number) ? -1 * order : order
      })
    }

    // Apply pagination
    const offset = options?.offset ?? 0
    const limit = options?.limit ?? 100
    items = items.slice(offset, offset + limit)

    return {
      data: items,
      total,
      hasMore: offset + items.length < total,
    }
  }

  const linkDocuments = (fromId: string, predicate: string, toId: string): void => {
    // Forward relationship
    let predicateMap = store.relationships.get(fromId)
    if (!predicateMap) {
      predicateMap = new Map()
      store.relationships.set(fromId, predicateMap)
    }
    let toIds = predicateMap.get(predicate)
    if (!toIds) {
      toIds = new Set()
      predicateMap.set(predicate, toIds)
    }
    toIds.add(toId)

    // Reverse relationship
    let reversePredicateMap = store.reverseRelationships.get(toId)
    if (!reversePredicateMap) {
      reversePredicateMap = new Map()
      store.reverseRelationships.set(toId, reversePredicateMap)
    }
    let fromIds = reversePredicateMap.get(predicate)
    if (!fromIds) {
      fromIds = new Set()
      reversePredicateMap.set(predicate, fromIds)
    }
    fromIds.add(fromId)
  }

  const unlinkDocuments = (fromId: string, predicate: string, toId: string): void => {
    // Forward relationship
    const predicateMap = store.relationships.get(fromId)
    const toIds = predicateMap?.get(predicate)
    toIds?.delete(toId)

    // Reverse relationship
    const reversePredicateMap = store.reverseRelationships.get(toId)
    const fromIds = reversePredicateMap?.get(predicate)
    fromIds?.delete(fromId)
  }

  const traverseRelationships = (fromId: string, predicate: string, options?: ListOptions): Thing[] => {
    const predicateMap = store.relationships.get(fromId)
    const toIds = predicateMap?.get(predicate)
    if (!toIds) return []

    let items: Thing[] = []
    for (const toId of toIds) {
      // Find the document by scanning all type stores
      for (const typeStore of store.documents.values()) {
        const doc = typeStore.get(toId)
        if (doc) {
          items.push(doc)
          break
        }
      }
    }

    // Apply pagination
    const offset = options?.offset ?? 0
    const limit = options?.limit ?? 100
    items = items.slice(offset, offset + limit)

    return items
  }

  const createCollectionProxy = <E extends Thing>(
    typeName: string
  ): CollectionProxy<E> => {
    const plural = pluralize(typeName).toLowerCase()

    const proxy = function (idOrFilter: string | FilterOptions): Promise<E | null | ListResponse<E>> {
      if (typeof idOrFilter === 'string') {
        return Promise.resolve(getDocument(plural, idOrFilter) as E | null)
      }
      return Promise.resolve(findDocuments(plural, idOrFilter) as ListResponse<E>)
    }

    Object.assign(proxy, {
      list: (options?: ListOptions): Promise<ListResponse<E>> => {
        return Promise.resolve(listDocuments(plural, options) as ListResponse<E>)
      },

      get: (id: string): Promise<E | null> => {
        return Promise.resolve(getDocument(plural, id) as E | null)
      },

      create: (data: Partial<E>): Promise<E> => {
        const id = data.$id ?? `${typeName.toLowerCase()}-${Date.now()}-${Math.random().toString(36).slice(2)}`
        const doc = { ...data, $id: id, $type: typeName } as E
        setDocument(plural, id, doc as Thing)
        return Promise.resolve(doc)
      },

      count: (filter?: FilterOptions): Promise<CountResponse> => {
        if (filter) {
          const result = findDocuments(plural, filter)
          return Promise.resolve({ count: result.total })
        }
        const typeStore = store.documents.get(plural)
        return Promise.resolve({ count: typeStore?.size ?? 0 })
      },

      find: (filter: FilterOptions, options?: ListOptions): Promise<ListResponse<E>> => {
        return Promise.resolve(findDocuments(plural, filter, options) as ListResponse<E>)
      },

      map: <K extends keyof E>(key: K): Promise<Array<E[K]>> => {
        const result = listDocuments(plural)
        return Promise.resolve(result.data.map((item) => (item as E)[key]))
      },

      filter: (fn: (item: E) => boolean): Promise<E[]> => {
        const result = listDocuments(plural)
        return Promise.resolve((result.data as E[]).filter(fn))
      },

      slice: (start: number, end?: number): Promise<E[]> => {
        const result = listDocuments(plural)
        return Promise.resolve((result.data as E[]).slice(start, end))
      },

      get first(): Promise<E | undefined> {
        const result = listDocuments(plural, { limit: 1 })
        return Promise.resolve(result.data[0] as E | undefined)
      },

      get last(): Promise<E | undefined> {
        const result = listDocuments(plural)
        return Promise.resolve(result.data[result.data.length - 1] as E | undefined)
      },
    })

    return proxy as CollectionProxy<E>
  }

  const createBatchBuilder = (): BatchBuilder<T> => {
    const operations: Array<{
      type: 'create' | 'update' | 'delete' | 'link' | 'unlink'
      typeName?: string
      id?: string
      data?: unknown
      fromId?: string
      predicate?: string
      toId?: string
    }> = []

    const builder: BatchBuilder<T> = {
      create(typeName, data) {
        const plural = pluralize(String(typeName)).toLowerCase()
        operations.push({ type: 'create', typeName: plural, data })
        return builder
      },

      update(typeName, id, data) {
        const plural = pluralize(String(typeName)).toLowerCase()
        operations.push({ type: 'update', typeName: plural, id, data })
        return builder
      },

      delete(typeName, id) {
        const plural = pluralize(String(typeName)).toLowerCase()
        operations.push({ type: 'delete', typeName: plural, id })
        return builder
      },

      link(fromId, predicate, toId) {
        operations.push({ type: 'link', fromId, predicate, toId })
        return builder
      },

      unlink(fromId, predicate, toId) {
        operations.push({ type: 'unlink', fromId, predicate, toId })
        return builder
      },

      async commit(): Promise<unknown[]> {
        const results: unknown[] = []

        for (const op of operations) {
          switch (op.type) {
            case 'create': {
              const data = op.data as Thing | undefined
              const id =
                data?.$id ??
                `item-${Date.now()}-${Math.random().toString(36).slice(2)}`
              const doc = { ...(data ?? {}), $id: id, $type: op.typeName! } as Thing
              setDocument(op.typeName!, id, doc)
              results.push(doc)
              break
            }
            case 'update': {
              const existing = getDocument(op.typeName!, op.id!)
              if (existing) {
                const updated = { ...existing, ...(op.data as Record<string, unknown> ?? {}) }
                setDocument(op.typeName!, op.id!, updated)
                results.push(updated)
              }
              break
            }
            case 'delete': {
              deleteDocument(op.typeName!, op.id!)
              results.push({ deleted: true })
              break
            }
            case 'link': {
              linkDocuments(op.fromId!, op.predicate!, op.toId!)
              results.push({ linked: true })
              break
            }
            case 'unlink': {
              unlinkDocuments(op.fromId!, op.predicate!, op.toId!)
              results.push({ unlinked: true })
              break
            }
          }
        }

        return results
      },
    }

    return builder
  }

  // Build collection proxies from schema
  const collections = new Map<string, CollectionProxy<Thing>>()
  for (const typeName of Object.keys(schema)) {
    const plural = pluralize(typeName).toLowerCase()
    collections.set(plural, createCollectionProxy(typeName))
  }

  return {
    collection<K extends keyof T>(name: K): CollectionProxy<InferEntity<T[K]>> {
      const plural = pluralize(String(name)).toLowerCase()
      return (collections.get(plural) ?? createCollectionProxy(String(name))) as CollectionProxy<
        InferEntity<T[K]>
      >
    },

    get(type: string, id: string): Promise<Thing | null> {
      return Promise.resolve(getDocument(type, id))
    },

    create(type: string, data: Partial<Thing>): Promise<Thing> {
      const id =
        data.$id ?? `${type.toLowerCase()}-${Date.now()}-${Math.random().toString(36).slice(2)}`
      const doc = { ...data, $id: id, $type: type } as Thing
      setDocument(type, id, doc)
      return Promise.resolve(doc)
    },

    update(type: string, id: string, data: Partial<Thing>): Promise<Thing> {
      const existing = getDocument(type, id)
      if (!existing) {
        throw new Error(`Document not found: ${type}/${id}`)
      }
      const updated = { ...existing, ...data }
      setDocument(type, id, updated)
      return Promise.resolve(updated)
    },

    delete(type: string, id: string): Promise<boolean> {
      return Promise.resolve(deleteDocument(type, id))
    },

    list(type: string, options?: ListOptions): Promise<ListResponse<Thing>> {
      return Promise.resolve(listDocuments(type, options))
    },

    find(type: string, filter: FilterOptions, options?: ListOptions): Promise<ListResponse<Thing>> {
      return Promise.resolve(findDocuments(type, filter, options))
    },

    count(type: string, filter?: FilterOptions): Promise<CountResponse> {
      if (filter) {
        const result = findDocuments(type, filter)
        return Promise.resolve({ count: result.total })
      }
      const typeStore = store.documents.get(type.toLowerCase())
      return Promise.resolve({ count: typeStore?.size ?? 0 })
    },

    link(fromId: string, predicate: string, toId: string): Promise<void> {
      linkDocuments(fromId, predicate, toId)
      return Promise.resolve()
    },

    unlink(fromId: string, predicate: string, toId: string): Promise<void> {
      unlinkDocuments(fromId, predicate, toId)
      return Promise.resolve()
    },

    traverse(fromId: string, predicate: string, options?: ListOptions): Promise<Thing[]> {
      return Promise.resolve(traverseRelationships(fromId, predicate, options))
    },

    batch(): BatchBuilder<T> {
      return createBatchBuilder()
    },

    bulkCreate(type: string, items: Array<{ $id: string; data: Partial<Thing> }>): Promise<{ buffered: number }> {
      for (const item of items) {
        setDocument(type, item.$id, { ...item.data, $id: item.$id, $type: type } as Thing)
      }
      return Promise.resolve({ buffered: items.length })
    },

    schema,

    close(): void {
      store.documents.clear()
      store.relationships.clear()
      store.reverseRelationships.clear()
    },
  }
}

/**
 * Create a default SDB store with a common schema for benchmarking.
 */
export async function createSDBStore(): Promise<SDBStore> {
  return DB({
    Thing: {
      name: 'string',
      status: 'string',
      owner: '-> User',
      created_at: 'date',
    },
    User: {
      name: 'string',
      email: 'string',
      status: 'string',
      things: '<- Thing[]',
      created_at: 'date',
    },
    Relationship: {
      subject: '-> Thing',
      predicate: 'string',
      object: '-> Thing',
      created_at: 'date',
    },
  })
}

/**
 * Seed the store with test data based on dataset size.
 */
export async function seedTestData(store: SDBStore, size: DatasetSize = 'medium'): Promise<void> {
  const counts = {
    small: { users: 100, things: 500, relationships: 250 },
    medium: { users: 500, things: 1000, relationships: 500 },
    large: { users: 2000, things: 5000, relationships: 2500 },
  }

  const { users, things, relationships } = counts[size]
  const statuses = ['active', 'inactive', 'pending', 'archived']

  // Insert users
  for (let i = 0; i < users; i++) {
    await store.create('users', {
      $id: `user-${i}`,
      name: `User ${i}`,
      email: `user${i}@example.com`,
      status: statuses[i % statuses.length],
      created_at: new Date(Date.now() - i * 60000).toISOString(),
    })
  }

  // Insert things
  for (let i = 0; i < things; i++) {
    const thingId = `thing-${String(i).padStart(4, '0')}`
    await store.create('things', {
      $id: thingId,
      name: `Thing ${i}`,
      status: statuses[i % statuses.length],
      owner: `user-${i % users}`,
      created_at: new Date(Date.now() - i * 60000).toISOString(),
    })

    // Link to owner
    await store.link(thingId, 'owner', `user-${i % users}`)
  }

  // Insert relationships
  for (let i = 0; i < relationships; i++) {
    const subjectIdx = i % things
    const objectIdx = (i + 1) % things
    const subjectId = `thing-${String(subjectIdx).padStart(4, '0')}`
    const objectId = `thing-${String(objectIdx).padStart(4, '0')}`

    await store.create('relationships', {
      $id: `rel-${String(i).padStart(4, '0')}`,
      subject: subjectId,
      predicate: 'relates_to',
      object: objectId,
      created_at: new Date().toISOString(),
    })

    // Also create the graph relationship
    await store.link(subjectId, 'relates_to', objectId)
  }
}

/**
 * Create a WebSocket-connected SDB client.
 * This connects to the actual SDB worker for production use.
 */
export async function createSDBClient(url: string): Promise<SDBStore> {
  // Lazy import the actual client
  const { DB: createDB } = await import('@dotdo/sdb')

  return createDB(
    {
      Thing: {
        name: 'string',
        status: 'string',
        owner: '-> User',
        created_at: 'date',
      },
      User: {
        name: 'string',
        email: 'string',
        status: 'string',
        things: '<- Thing[]',
        created_at: 'date',
      },
      Relationship: {
        subject: '-> Thing',
        predicate: 'string',
        object: '-> Thing',
        created_at: 'date',
      },
    },
    { url }
  ) as unknown as SDBStore
}
