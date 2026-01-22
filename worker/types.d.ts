// Type declarations for optional/external modules

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
