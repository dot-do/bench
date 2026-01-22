/**
 * Type declaration stubs for external modules
 *
 * These are placeholder types to satisfy the TypeScript compiler
 * when the actual packages are not installed.
 */

// ============================================================================
// @dotdo/db4 - Pure TypeScript document store
// ============================================================================
declare module '@dotdo/db4' {
  export class DB4 {
    get<T = unknown>(collection: string, id: string): Promise<T | null>
    list<T = unknown>(collection: string, options?: unknown): Promise<T[]>
    count(collection: string, options?: unknown): Promise<number>
    set(collection: string, id: string, data: unknown): Promise<void>
    delete(collection: string, id: string): Promise<boolean>
    query(sql: string | unknown): Promise<unknown[]>
    close(): Promise<void>
  }
}

// ============================================================================
// @dotdo/evodb - Event-sourced document store
// ============================================================================
declare module '@dotdo/evodb' {
  export interface QueryBuilder<T> {
    where(field: string, op: '=' | '!=' | '>' | '<' | '>=' | '<=', value: unknown): QueryBuilder<T>
    join(table: string, leftField: string, rightField: string): QueryBuilder<T>
    limit(n: number): QueryBuilder<T>
    offset(n: number): QueryBuilder<T>
    orderBy(field: string, direction?: 'asc' | 'desc'): QueryBuilder<T>
    all(): Promise<T[]>
    first(): Promise<T | null>
    count(): Promise<number>
  }

  export class EvoDB {
    get<T = unknown>(collection: string, id: string): Promise<T | null>
    set(collection: string, id: string, data: unknown): Promise<void>
    delete(collection: string, id: string): Promise<boolean>
    query<T = unknown>(collection: string): QueryBuilder<T>
    close(): Promise<void>
  }
}

// ============================================================================
// @dotdo/graphdb/client - GraphDB WebSocket client
// ============================================================================
declare module '@dotdo/graphdb/client' {
  export interface Entity {
    $id: string
    $type: string
    [key: string]: unknown
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

  export interface TraversalOptions {
    maxDepth?: number
    limit?: number
    predicateFilter?: string[]
  }

  export interface GraphClient {
    insert(entity: Entity): Promise<void>
    batchInsert(entities: Entity[]): Promise<BatchResult<void>>
    query(queryString: string): Promise<QueryResult | Entity | null>
    update(id: string, props: Record<string, unknown>): Promise<void>
    delete(id: string): Promise<void>
    traverse(startId: string, predicate: string, options?: TraversalOptions): Promise<Entity[]>
    reverseTraverse(targetId: string, predicate: string, options?: TraversalOptions): Promise<Entity[]>
    pathTraverse(startId: string, path: string[], options?: TraversalOptions): Promise<Entity[]>
    batchGet(ids: string[]): Promise<BatchResult<Entity | null>>
    close(): void
  }

  export function createGraphClient(wsUrl: string): GraphClient
}

// ============================================================================
// @dotdo/sdb - Schema-driven document store
// ============================================================================
declare module '@dotdo/sdb' {
  export interface SDBStore {
    get(type: string, id: string): Promise<unknown | null>
    list(type: string, options?: unknown): Promise<unknown[]>
    set(type: string, id: string, data: unknown): Promise<void>
    delete(type: string, id: string): Promise<boolean>
    link(fromId: string, predicate: string, toId: string): Promise<void>
    unlink(fromId: string, predicate: string, toId: string): Promise<void>
    query(queryString: string): Promise<unknown[]>
    close(): Promise<void>
  }

  export function DB(schema: Record<string, Record<string, string>>, options?: { url?: string } | string): SDBStore
}

// ============================================================================
// @db4/mongo - MongoDB-compatible API for db4
// ============================================================================
declare module '@db4/mongo' {
  export interface Document {
    [key: string]: unknown
  }

  export type Filter<T> = {
    [P in keyof T]?: T[P] | { $eq?: T[P]; $ne?: T[P]; $gt?: T[P]; $gte?: T[P]; $lt?: T[P]; $lte?: T[P]; $in?: T[P][]; $nin?: T[P][] }
  } & { [key: string]: unknown }

  export interface FindOptions {
    projection?: Record<string, 0 | 1>
    sort?: Record<string, 1 | -1>
    limit?: number
    skip?: number
  }

  export interface InsertOneResult {
    acknowledged: boolean
    insertedId: unknown
  }

  export interface InsertManyResult {
    acknowledged: boolean
    insertedCount: number
    insertedIds: Record<number, unknown>
  }

  export interface UpdateResult {
    acknowledged: boolean
    matchedCount: number
    modifiedCount: number
    upsertedCount: number
    upsertedId?: unknown
  }

  export interface DeleteResult {
    acknowledged: boolean
    deletedCount: number
  }

  export interface AggregateOptions {
    allowDiskUse?: boolean
    maxTimeMS?: number
  }

  export interface FindCursor<T> {
    sort(spec: Record<string, 1 | -1>): FindCursor<T>
    limit(n: number): FindCursor<T>
    skip(n: number): FindCursor<T>
    project(spec: Record<string, 0 | 1>): FindCursor<T>
    toArray(): Promise<T[]>
  }

  export interface AggregateCursor<T> {
    toArray(): Promise<T[]>
  }

  export interface Collection<T extends Document = Document> {
    findOne(filter: Filter<T>): Promise<T | null>
    find(filter: Filter<T>, options?: FindOptions): FindCursor<T>
    insertOne(doc: T): Promise<InsertOneResult>
    insertMany(docs: T[]): Promise<InsertManyResult>
    updateOne(filter: Filter<T>, update: unknown): Promise<UpdateResult>
    updateMany(filter: Filter<T>, update: unknown): Promise<UpdateResult>
    deleteOne(filter: Filter<T>): Promise<DeleteResult>
    deleteMany(filter: Filter<T>): Promise<DeleteResult>
    aggregate(pipeline: unknown[], options?: AggregateOptions): AggregateCursor<T>
    countDocuments(filter?: Filter<T>): Promise<number>
    estimatedDocumentCount(): Promise<number>
    createIndex(keys: Record<string, 1 | -1>): Promise<string>
    distinct(field: string, filter?: Filter<T>): Promise<unknown[]>
    replaceOne(filter: Filter<T>, doc: T, options?: { upsert?: boolean }): Promise<UpdateResult>
    findOneAndUpdate(filter: Filter<T>, update: unknown, options?: { returnDocument?: 'before' | 'after' }): Promise<T | null>
  }

  export interface Db {
    collection<T extends Document = Document>(name: string): Collection<T>
    dropCollection(name: string): Promise<boolean>
    listCollections(): { toArray(): Promise<Array<{ name: string }>> }
  }

  export class MongoClient {
    constructor(url: string)
    connect(): Promise<void>
    db(name?: string): Db
    close(): Promise<void>
  }
}

// ============================================================================
// @dotdo/mongodb - MongoDB-compatible API for PostgreSQL/DocumentDB
// ============================================================================
declare module '@dotdo/mongodb' {
  export interface Document {
    [key: string]: unknown
  }

  export type Filter<T> = {
    [P in keyof T]?: T[P] | { $eq?: T[P]; $ne?: T[P]; $gt?: T[P]; $gte?: T[P]; $lt?: T[P]; $lte?: T[P]; $in?: T[P][]; $nin?: T[P][] }
  } & { [key: string]: unknown }

  export interface FindOptions {
    projection?: Record<string, 0 | 1>
    sort?: Record<string, 1 | -1>
    limit?: number
    skip?: number
  }

  export interface InsertOneResult {
    acknowledged: boolean
    insertedId: unknown
  }

  export interface InsertManyResult {
    acknowledged: boolean
    insertedCount: number
    insertedIds: Record<number, unknown>
  }

  export interface UpdateResult {
    acknowledged: boolean
    matchedCount: number
    modifiedCount: number
    upsertedCount: number
    upsertedId?: unknown
  }

  export interface DeleteResult {
    acknowledged: boolean
    deletedCount: number
  }

  export interface AggregateOptions {
    allowDiskUse?: boolean
    maxTimeMS?: number
  }

  export interface FindCursor<T> {
    sort(spec: Record<string, 1 | -1>): FindCursor<T>
    limit(n: number): FindCursor<T>
    skip(n: number): FindCursor<T>
    project(spec: Record<string, 0 | 1>): FindCursor<T>
    toArray(): Promise<T[]>
  }

  export interface AggregateCursor<T> {
    toArray(): Promise<T[]>
  }

  export interface Collection<T extends Document = Document> {
    findOne(filter: Filter<T>): Promise<T | null>
    find(filter: Filter<T>, options?: FindOptions): FindCursor<T>
    insertOne(doc: T): Promise<InsertOneResult>
    insertMany(docs: T[]): Promise<InsertManyResult>
    updateOne(filter: Filter<T>, update: unknown): Promise<UpdateResult>
    updateMany(filter: Filter<T>, update: unknown): Promise<UpdateResult>
    deleteOne(filter: Filter<T>): Promise<DeleteResult>
    deleteMany(filter: Filter<T>): Promise<DeleteResult>
    aggregate(pipeline: unknown[], options?: AggregateOptions): AggregateCursor<T>
    countDocuments(filter?: Filter<T>): Promise<number>
    estimatedDocumentCount(): Promise<number>
    createIndex(keys: Record<string, 1 | -1>): Promise<string>
    distinct(field: string, filter?: Filter<T>): Promise<unknown[]>
    replaceOne(filter: Filter<T>, doc: T, options?: { upsert?: boolean }): Promise<UpdateResult>
    findOneAndUpdate(filter: Filter<T>, update: unknown, options?: { returnDocument?: 'before' | 'after' }): Promise<T | null>
  }

  export interface Db {
    collection<T extends Document = Document>(name: string): Collection<T>
    dropCollection(name: string): Promise<boolean>
    listCollections(): { toArray(): Promise<Array<{ name: string }>> }
  }

  export class MongoClient {
    constructor(url: string)
    connect(): Promise<void>
    db(name?: string): Db
    close(): Promise<void>
  }
}

// ============================================================================
// @dotdo/sqlite - SQLite via libsql WASM
// ============================================================================
declare module '@dotdo/sqlite' {
  export interface QueryResult<T = unknown> {
    rows: T[]
    rowsAffected: number
    lastInsertRowid: bigint | null
  }

  export interface Client {
    execute(sql: string, params?: unknown[]): Promise<QueryResult>
    batch(statements: Array<{ sql: string; args?: unknown[] }>): Promise<QueryResult[]>
    transaction<T>(fn: (tx: Client) => Promise<T>): Promise<T>
    close(): void
  }

  export function createClient(options: { url: string }): Client
}

// ============================================================================
// @dotdo/duckdb - DuckDB WASM
// ============================================================================
declare module '@dotdo/duckdb' {
  export const DUCKDB_BUNDLES: {
    mvp: { mainModule: string; mainWorker: string }
    eh: { mainModule: string; mainWorker: string }
  }

  export function getJsDelivrBundles(): unknown

  export function selectBundle(bundles: {
    mvp: { mainModule: string; mainWorker: string }
    eh: { mainModule: string; mainWorker: string }
  }): Promise<{ mainWorker: string; mainModule: string; pthreadWorker?: string }>

  export class ConsoleLogger {
    constructor()
    log(level: string, message: string): void
  }

  export class AsyncDuckDB {
    constructor(logger: ConsoleLogger, worker: Worker)
    instantiate(module: string, pthreadWorker?: string): Promise<void>
    registerFileBuffer(name: string, buffer: Uint8Array): Promise<void>
    terminate(): Promise<void>
    connect(): Promise<{
      query<T>(sql: string): Promise<{ toArray(): Array<{ toJSON(): T }> }>
      prepare(sql: string): Promise<{
        query(...args: unknown[]): Promise<{ toArray(): unknown[] }>
        close(): Promise<void>
      }>
      close(): Promise<void>
    }>
  }
}

// ============================================================================
// chdb - ClickHouse embedded
// ============================================================================
declare module 'chdb' {
  export class Session {
    query(sql: string, format: string): string
  }
}
