// Type declarations for optional/external modules

// WASM and Data file imports for Cloudflare Workers
// These are bundled at build time via wrangler rules
declare module '*.wasm' {
  const wasmModule: WebAssembly.Module
  export default wasmModule
}

declare module '*.data' {
  const data: ArrayBuffer
  export default data
}

// Cloudflare Workers modules
declare module 'cloudflare:workers' {
  export class DurableObject {
    constructor(ctx: DurableObjectState, env: unknown)
    fetch(request: Request): Promise<Response>
    alarm?(): Promise<void>
    webSocketMessage?(ws: WebSocket, message: string | ArrayBuffer): void
    webSocketClose?(ws: WebSocket, code: number, reason: string, wasClean: boolean): void
    webSocketError?(ws: WebSocket, error: unknown): void
  }
}

interface DurableObjectState {
  id: DurableObjectId
  storage: DurableObjectStorage
  waitUntil(promise: Promise<unknown>): void
  blockConcurrencyWhile<T>(fn: () => Promise<T>): Promise<T>
}

interface DurableObjectId {
  toString(): string
  equals(other: DurableObjectId): boolean
}

interface DurableObjectStorage {
  get<T = unknown>(key: string): Promise<T | undefined>
  get<T = unknown>(keys: string[]): Promise<Map<string, T>>
  list<T = unknown>(options?: { start?: string; startAfter?: string; end?: string; prefix?: string; reverse?: boolean; limit?: number }): Promise<Map<string, T>>
  put<T>(key: string, value: T): Promise<void>
  put<T>(entries: Record<string, T>): Promise<void>
  delete(key: string): Promise<boolean>
  delete(keys: string[]): Promise<number>
  deleteAll(): Promise<void>
  transaction<T>(closure: (txn: DurableObjectTransaction) => Promise<T>): Promise<T>
  getAlarm(): Promise<number | null>
  setAlarm(scheduledTime: number | Date): Promise<void>
  deleteAlarm(): Promise<void>
  sync(): Promise<void>
}

interface DurableObjectTransaction extends DurableObjectStorage {
  rollback(): void
}

interface DurableObjectNamespace {
  idFromName(name: string): DurableObjectId
  idFromString(id: string): DurableObjectId
  newUniqueId(options?: { jurisdiction?: 'eu' }): DurableObjectId
  get(id: DurableObjectId): DurableObjectStub
}

interface DurableObjectStub {
  id: DurableObjectId
  name?: string
  fetch(request: Request | string, init?: RequestInit): Promise<Response>
}

// DuckDB modules
declare module '@dotdo/duckdb' {
  export function getJsDelivrBundles(): unknown
  export function selectBundle(bundles: unknown): Promise<{ mainWorker: string; mainModule: string; pthreadWorker?: string }>
  export class ConsoleLogger {}
  export class AsyncDuckDB {
    constructor(logger: ConsoleLogger, worker: Worker)
    instantiate(mainModule: string, pthreadWorker?: string): Promise<void>
    connect(): Promise<DuckDBConnection>
    registerFileBuffer(name: string, buffer: Uint8Array): Promise<void>
    terminate(): Promise<void>
  }
  interface DuckDBConnection {
    query(sql: string): Promise<{ toArray(): Array<{ toJSON(): unknown }> }>
    close(): Promise<void>
  }
}

declare module '@duckdb/duckdb-wasm' {
  export function getJsDelivrBundles(): unknown
  export function selectBundle(bundles: unknown): Promise<{ mainWorker: string; mainModule: string; pthreadWorker?: string }>
  export class ConsoleLogger {}
  export class AsyncDuckDB {
    constructor(logger: ConsoleLogger, worker: Worker)
    instantiate(mainModule: string, pthreadWorker?: string): Promise<void>
    connect(): Promise<DuckDBConnection>
    registerFileBuffer(name: string, buffer: Uint8Array): Promise<void>
    terminate(): Promise<void>
  }
  interface DuckDBConnection {
    query(sql: string): Promise<{ toArray(): Array<{ toJSON(): unknown }> }>
    close(): Promise<void>
  }
}

// Postgres/PGLite modules
declare module '@dotdo/postgres' {
  export class PGlite {
    waitReady: Promise<void>
    query<T = unknown>(sql: string, params?: unknown[]): Promise<{ rows: T[]; rowCount: number }>
    close(): Promise<void>
  }
}

declare module '@electric-sql/pglite' {
  export interface PGliteOptions {
    dataDir?: string
    wasmModule?: WebAssembly.Module
    fsBundle?: Blob
    database?: string
    debug?: number
  }

  export class PGlite {
    static create(options?: PGliteOptions): Promise<PGlite>
    constructor(dataDir?: string)
    waitReady: Promise<void>
    query<T = unknown>(sql: string, params?: unknown[]): Promise<{ rows: T[]; fields?: { name: string; dataTypeID: number }[]; affectedRows?: number }>
    transaction<T>(fn: (tx: PGlite) => Promise<T>): Promise<T>
    close(): Promise<void>
  }
}

// libSQL client for SQLite
declare module '@libsql/client' {
  export interface Config {
    url: string
    authToken?: string
  }
  export interface ResultSet {
    rows: unknown[]
    columns: string[]
    rowsAffected: number
    lastInsertRowid: bigint | null
  }
  export interface Client {
    execute(sql: string, args?: unknown[]): Promise<ResultSet>
    batch(statements: Array<{ sql: string; args?: unknown[] }>): Promise<ResultSet[]>
    transaction<T>(fn: (tx: Transaction) => Promise<T>): Promise<T>
    close(): void
  }
  export interface Transaction extends Client {}
  export function createClient(config: Config): Client
}

// EvoDB core package
declare module '@evodb/core' {
  export interface EvoDBConfig {
    mode: 'development' | 'production'
    storage?: unknown
    schemaEvolution?: 'automatic' | 'locked'
    inferTypes?: boolean
    validateOnWrite?: boolean
    rejectUnknownFields?: boolean
  }
  export interface QueryResult<T = Record<string, unknown>> {
    rows: T[]
    totalCount: number
    hasMore: boolean
  }
  export interface UpdateResult<T = Record<string, unknown>> {
    matchedCount: number
    modifiedCount: number
    documents?: T[]
  }
  export interface DeleteResult<T = Record<string, unknown>> {
    deletedCount: number
    documents?: T[]
  }
  export class QueryBuilder<T = Record<string, unknown>> {
    where(column: string, operator: string, value: unknown): QueryBuilder<T>
    select(columns: string[]): QueryBuilder<T>
    orderBy(column: string, direction?: 'asc' | 'desc'): QueryBuilder<T>
    limit(count: number): QueryBuilder<T>
    offset(count: number): QueryBuilder<T>
    execute(): Promise<T[]>
    executeWithMeta(): Promise<QueryResult<T>>
  }
  export class EvoDB {
    constructor(config: EvoDBConfig)
    insert<T extends Record<string, unknown>>(table: string, data: T | T[]): Promise<T[]>
    update<T extends Record<string, unknown>>(table: string, filter: Record<string, unknown>, changes: Partial<T>): Promise<UpdateResult<T>>
    delete<T extends Record<string, unknown>>(table: string, filter: Record<string, unknown>): Promise<DeleteResult<T>>
    query<T = Record<string, unknown>>(table: string): QueryBuilder<T>
    getMode(): 'development' | 'production'
  }
}

// SQLite modules
declare module '@dotdo/sqlite' {
  export interface Database {
    run(sql: string, params?: unknown[]): void
    exec(sql: string): Array<{ columns: string[]; values: unknown[][] }>
    prepare(sql: string): Statement
    close(): void
  }
  export interface Statement {
    run(params?: unknown[]): void
    free(): void
  }
  export default function initSqlJs(): Promise<{ Database: new () => Database }>
}

declare module 'sql.js' {
  export interface Database {
    run(sql: string, params?: unknown[]): void
    exec(sql: string): Array<{ columns: string[]; values: unknown[][] }>
    prepare(sql: string): Statement
    close(): void
  }
  export interface Statement {
    run(params?: unknown[]): void
    free(): void
  }
  export default function initSqlJs(): Promise<{ Database: new () => Database }>
}

// MongoDB modules
declare module '@db4/mongo' {
  export class MongoClient {
    constructor(uri: string)
    connect(): Promise<void>
    db(name?: string): Db
    close(): Promise<void>
  }
  export interface Db {
    collection<T = unknown>(name: string): Collection<T>
  }
  export interface Collection<T = unknown> {
    findOne(filter: object): Promise<T | null>
    find(filter: object): Cursor<T>
    insertOne(doc: T): Promise<{ insertedId: unknown }>
    insertMany(docs: T[]): Promise<{ insertedIds: unknown[] }>
    updateOne(filter: object, update: object): Promise<{ modifiedCount: number }>
    deleteOne(filter: object): Promise<{ deletedCount: number }>
    aggregate<R = T>(pipeline: object[]): Cursor<R>
    countDocuments(filter?: object): Promise<number>
  }
  export interface Cursor<T> {
    limit(n: number): Cursor<T>
    sort(sort: object): Cursor<T>
    toArray(): Promise<T[]>
  }
  export function connect(uri?: string): Promise<MongoClient>
}

declare module '@dotdo/mongodb' {
  export class MongoClient {
    constructor(uri: string)
    connect(): Promise<void>
    db(name?: string): Db
    close(): Promise<void>
  }
  export interface Db {
    collection<T = unknown>(name: string): Collection<T>
  }
  export interface Collection<T = unknown> {
    findOne(filter: object): Promise<T | null>
    find(filter: object): Cursor<T>
    insertOne(doc: T): Promise<{ insertedId: unknown }>
    insertMany(docs: T[]): Promise<{ insertedIds: unknown[] }>
    updateOne(filter: object, update: object): Promise<{ modifiedCount: number }>
    deleteOne(filter: object): Promise<{ deletedCount: number }>
    aggregate<R = T>(pipeline: object[]): Cursor<R>
    countDocuments(filter?: object): Promise<number>
  }
  export interface Cursor<T> {
    limit(n: number): Cursor<T>
    sort(sort: object): Cursor<T>
    toArray(): Promise<T[]>
  }
  export function connect(uri?: string): Promise<MongoClient>
}
