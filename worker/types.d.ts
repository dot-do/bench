// Type declarations for optional/external modules

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
  export class PGlite {
    waitReady: Promise<void>
    query<T = unknown>(sql: string, params?: unknown[]): Promise<{ rows: T[]; rowCount: number }>
    close(): Promise<void>
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
