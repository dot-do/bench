/**
 * SQLite Container Database Adapter
 *
 * Adapter for connecting to SQLite running in a Cloudflare Container.
 * Lightweight embedded database ideal for simple workloads.
 *
 * @see https://developers.cloudflare.com/containers/
 * @see https://www.sqlite.org/
 */

import type { Container } from '@cloudflare/containers'
import {
  BaseContainerDatabase,
  type ContainerDatabaseConfig,
  type ContainerDatabase,
} from './base.js'

// SQLite specific configuration
export interface SQLiteContainerConfig extends ContainerDatabaseConfig {
  // Database file path (default: :memory:)
  database?: string

  // Enable WAL mode for better concurrency
  walMode?: boolean

  // Busy timeout in milliseconds
  busyTimeout?: number

  // Enable foreign keys
  foreignKeys?: boolean

  // Journal mode (delete, truncate, persist, memory, wal, off)
  journalMode?: 'delete' | 'truncate' | 'persist' | 'memory' | 'wal' | 'off'
}

// SQLite query result
export interface SQLiteQueryResult<T = unknown> {
  rows: T[]
  rowsAffected: number
  lastInsertRowid: number | null
  changes: number
}

/**
 * SQLite Container Database Adapter
 *
 * Connects to SQLite via an HTTP REST API running in the container.
 * The container runs a lightweight HTTP-to-SQLite bridge.
 */
export class SQLiteContainer extends BaseContainerDatabase implements ContainerDatabase {
  private sqliteConfig: Required<SQLiteContainerConfig>

  static readonly DEFAULT_PORT = 8080 // HTTP API port
  static readonly DATABASE_TYPE = 'SQLite'

  constructor(config: SQLiteContainerConfig) {
    super(config)

    this.sqliteConfig = {
      ...this.config,
      database: config.database ?? ':memory:',
      walMode: config.walMode ?? true,
      busyTimeout: config.busyTimeout ?? 5000,
      foreignKeys: config.foreignKeys ?? true,
      journalMode: config.journalMode ?? 'wal',
    }
  }

  getDatabaseType(): string {
    return SQLiteContainer.DATABASE_TYPE
  }

  getDefaultPort(): number {
    return SQLiteContainer.DEFAULT_PORT
  }

  /**
   * Connect to the SQLite container.
   * Waits for the database to be ready.
   */
  protected async doConnect(): Promise<void> {
    await this.waitForReady()
    await this.initializeDatabase()
  }

  /**
   * Close the SQLite connection.
   */
  protected async doClose(): Promise<void> {
    // HTTP connections are stateless, nothing to close
  }

  /**
   * Execute a query and return results.
   */
  protected async doQuery<T>(sql: string, params?: unknown[]): Promise<T[]> {
    const response = await this.containerFetch('/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: sql,
        params: params ?? [],
        database: this.sqliteConfig.database,
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`SQLite query failed: ${error}`)
    }

    const result = (await response.json()) as SQLiteQueryResult<T>
    return result.rows
  }

  /**
   * Execute a statement and return affected row count.
   */
  protected async doExecute(sql: string, params?: unknown[]): Promise<{ rowsAffected: number }> {
    const response = await this.containerFetch('/execute', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: sql,
        params: params ?? [],
        database: this.sqliteConfig.database,
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`SQLite execute failed: ${error}`)
    }

    const result = (await response.json()) as { rowsAffected: number; lastInsertRowid: number | null }
    return { rowsAffected: result.rowsAffected }
  }

  /**
   * Ping SQLite to verify connectivity.
   */
  protected async doPing(): Promise<void> {
    const response = await this.containerFetch('/health', {
      method: 'GET',
    })

    if (!response.ok) {
      throw new Error('SQLite health check failed')
    }
  }

  /**
   * Wait for SQLite to be ready to accept connections.
   */
  private async waitForReady(): Promise<void> {
    const maxAttempts = 30
    const delayMs = 1000

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      try {
        const response = await this.containerFetch('/ready', {
          method: 'GET',
        })

        if (response.ok) {
          return
        }
      } catch {
        // Container may not be ready yet
      }

      await this.sleep(delayMs)
    }

    throw new Error('SQLite container failed to become ready')
  }

  /**
   * Initialize database with configured settings.
   */
  private async initializeDatabase(): Promise<void> {
    // Set journal mode
    await this.containerFetch('/execute', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: `PRAGMA journal_mode = ${this.sqliteConfig.journalMode}`,
        params: [],
        database: this.sqliteConfig.database,
      }),
    })

    // Set busy timeout
    await this.containerFetch('/execute', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: `PRAGMA busy_timeout = ${this.sqliteConfig.busyTimeout}`,
        params: [],
        database: this.sqliteConfig.database,
      }),
    })

    // Set foreign keys
    if (this.sqliteConfig.foreignKeys) {
      await this.containerFetch('/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          query: 'PRAGMA foreign_keys = ON',
          params: [],
          database: this.sqliteConfig.database,
        }),
      })
    }
  }

  // SQLite-specific operations

  /**
   * Execute a query and return full result with metadata.
   */
  async queryWithMetadata<T = unknown>(sql: string, params?: unknown[]): Promise<SQLiteQueryResult<T>> {
    this.ensureConnected()

    const response = await this.containerFetch('/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: sql,
        params: params ?? [],
        database: this.sqliteConfig.database,
        includeMetadata: true,
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`SQLite query failed: ${error}`)
    }

    return (await response.json()) as SQLiteQueryResult<T>
  }

  /**
   * Execute multiple statements in a transaction.
   */
  async transaction<T>(fn: (tx: SQLiteTransaction) => Promise<T>): Promise<T> {
    this.ensureConnected()

    // Begin transaction
    await this.containerFetch('/execute', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: 'BEGIN TRANSACTION',
        params: [],
        database: this.sqliteConfig.database,
      }),
    })

    const tx: SQLiteTransaction = {
      query: async <R>(sql: string, params?: unknown[]): Promise<R[]> => {
        const response = await this.containerFetch('/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            query: sql,
            params: params ?? [],
            database: this.sqliteConfig.database,
          }),
        })

        if (!response.ok) {
          const error = await response.text()
          throw new Error(`SQLite query failed: ${error}`)
        }

        const result = (await response.json()) as SQLiteQueryResult<R>
        return result.rows
      },
      execute: async (sql: string, params?: unknown[]): Promise<{ rowsAffected: number; lastInsertRowid: number | null }> => {
        const response = await this.containerFetch('/execute', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            query: sql,
            params: params ?? [],
            database: this.sqliteConfig.database,
          }),
        })

        if (!response.ok) {
          const error = await response.text()
          throw new Error(`SQLite execute failed: ${error}`)
        }

        return (await response.json()) as { rowsAffected: number; lastInsertRowid: number | null }
      },
    }

    try {
      const result = await fn(tx)

      // Commit transaction
      await this.containerFetch('/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          query: 'COMMIT',
          params: [],
          database: this.sqliteConfig.database,
        }),
      })

      return result
    } catch (error) {
      // Rollback transaction
      await this.containerFetch('/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          query: 'ROLLBACK',
          params: [],
          database: this.sqliteConfig.database,
        }),
      })

      throw error
    }
  }

  /**
   * Execute multiple statements in a batch.
   */
  async batch(statements: Array<{ sql: string; params?: unknown[] }>): Promise<SQLiteQueryResult[]> {
    this.ensureConnected()

    const response = await this.containerFetch('/batch', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        statements,
        database: this.sqliteConfig.database,
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`SQLite batch failed: ${error}`)
    }

    return (await response.json()) as SQLiteQueryResult[]
  }

  /**
   * Get table information.
   */
  async describeTable(tableName: string): Promise<Array<{
    cid: number
    name: string
    type: string
    notnull: number
    dflt_value: unknown
    pk: number
  }>> {
    this.ensureConnected()
    return this.query(`PRAGMA table_info(${tableName})`)
  }

  /**
   * List all tables.
   */
  async listTables(): Promise<string[]> {
    this.ensureConnected()
    const result = await this.query<{ name: string }>(
      "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    return result.map((row) => row.name)
  }

  /**
   * Get database statistics.
   */
  async getDatabaseInfo(): Promise<{
    pageSize: number
    pageCount: number
    freelist: number
    schemaVersion: number
    userVersion: number
  }> {
    this.ensureConnected()

    const [pageSize] = await this.query<{ page_size: number }>('PRAGMA page_size')
    const [pageCount] = await this.query<{ page_count: number }>('PRAGMA page_count')
    const [freelist] = await this.query<{ freelist_count: number }>('PRAGMA freelist_count')
    const [schemaVersion] = await this.query<{ schema_version: number }>('PRAGMA schema_version')
    const [userVersion] = await this.query<{ user_version: number }>('PRAGMA user_version')

    return {
      pageSize: pageSize.page_size,
      pageCount: pageCount.page_count,
      freelist: freelist.freelist_count,
      schemaVersion: schemaVersion.schema_version,
      userVersion: userVersion.user_version,
    }
  }

  /**
   * Vacuum the database to reclaim space.
   */
  async vacuum(): Promise<void> {
    this.ensureConnected()
    await this.execute('VACUUM')
  }

  /**
   * Analyze the database for query optimization.
   */
  async analyze(tableName?: string): Promise<void> {
    this.ensureConnected()
    await this.execute(tableName ? `ANALYZE ${tableName}` : 'ANALYZE')
  }

  /**
   * Create a backup of the database.
   */
  async backup(destPath: string): Promise<void> {
    this.ensureConnected()

    const response = await this.containerFetch('/backup', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        database: this.sqliteConfig.database,
        destination: destPath,
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`SQLite backup failed: ${error}`)
    }
  }

  /**
   * Check database integrity.
   */
  async integrityCheck(): Promise<{ ok: boolean; errors: string[] }> {
    this.ensureConnected()
    const result = await this.query<{ integrity_check: string }>('PRAGMA integrity_check')

    if (result.length === 1 && result[0].integrity_check === 'ok') {
      return { ok: true, errors: [] }
    }

    return {
      ok: false,
      errors: result.map((row) => row.integrity_check),
    }
  }

  /**
   * Get the last insert rowid.
   */
  async lastInsertRowid(): Promise<number> {
    this.ensureConnected()
    const result = await this.query<{ last_insert_rowid: number }>('SELECT last_insert_rowid() as last_insert_rowid')
    return result[0].last_insert_rowid
  }

  /**
   * Get the number of changes from the last statement.
   */
  async changes(): Promise<number> {
    this.ensureConnected()
    const result = await this.query<{ changes: number }>('SELECT changes() as changes')
    return result[0].changes
  }
}

/**
 * SQLite transaction interface.
 */
export interface SQLiteTransaction {
  query<T = unknown>(sql: string, params?: unknown[]): Promise<T[]>
  execute(sql: string, params?: unknown[]): Promise<{ rowsAffected: number; lastInsertRowid: number | null }>
}

/**
 * Create a SQLite container adapter.
 */
export function createSQLiteContainer(
  container: Container,
  options?: Partial<Omit<SQLiteContainerConfig, 'container'>>
): SQLiteContainer {
  return new SQLiteContainer({
    container,
    ...options,
  })
}

/**
 * SQLite container class for wrangler.toml binding.
 */
export const SQLiteContainerClass = {
  name: 'SQLiteContainer',
  defaultPort: SQLiteContainer.DEFAULT_PORT,
  sleepAfter: '10m',

  /**
   * Generate wrangler.toml container binding.
   */
  toWranglerBinding(bindingName: string, options?: { maxInstances?: number; sleepAfter?: string }): string {
    return `
[[containers]]
binding = "${bindingName}"
class_name = "SQLiteContainer"
image = "./containers/dockerfiles/Dockerfile.sqlite"
max_instances = ${options?.maxInstances ?? 10}
default_port = ${SQLiteContainer.DEFAULT_PORT}
sleep_after = "${options?.sleepAfter ?? '10m'}"
`
  },
}

/**
 * Wrangler configuration for SQLite container.
 */
export const SQLITE_WRANGLER_CONFIG = {
  binding: 'SQLITE_CONTAINER',
  className: 'SQLiteContainer',
  image: './containers/dockerfiles/Dockerfile.sqlite',
  defaultPort: SQLiteContainer.DEFAULT_PORT,
  maxInstances: 10,
  sleepAfter: '10m',
}
