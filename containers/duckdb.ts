/**
 * DuckDB Container Database Adapter
 *
 * Adapter for connecting to DuckDB running in a Cloudflare Container.
 * DuckDB is optimized for analytical queries and columnar storage.
 *
 * @see https://developers.cloudflare.com/containers/
 * @see https://duckdb.org/
 */

import type { Container } from '@cloudflare/containers'
import {
  BaseContainerDatabase,
  type ContainerDatabaseConfig,
  type ContainerDatabase,
} from './base.js'

// DuckDB specific configuration
export interface DuckDBContainerConfig extends ContainerDatabaseConfig {
  // Database file path (default: :memory:)
  database?: string

  // Read-only mode
  readOnly?: boolean

  // Number of threads to use
  threads?: number

  // Memory limit (e.g., '4GB')
  memoryLimit?: string
}

// DuckDB query result
export interface DuckDBQueryResult<T = unknown> {
  rows: T[]
  rowCount: number
  columns: Array<{
    name: string
    type: string
  }>
  time: number // Query execution time in seconds
}

// DuckDB export format
export type DuckDBExportFormat = 'parquet' | 'csv' | 'json'

/**
 * DuckDB Container Database Adapter
 *
 * Connects to DuckDB via an HTTP REST API running in the container.
 * The container runs a lightweight HTTP-to-DuckDB bridge.
 */
export class DuckDBContainer extends BaseContainerDatabase implements ContainerDatabase {
  private duckConfig: Required<DuckDBContainerConfig>

  static readonly DEFAULT_PORT = 9999 // DuckDB HTTP API port
  static readonly DATABASE_TYPE = 'DuckDB'

  constructor(config: DuckDBContainerConfig) {
    super(config)

    this.duckConfig = {
      ...this.config,
      database: config.database ?? ':memory:',
      readOnly: config.readOnly ?? false,
      threads: config.threads ?? 4,
      memoryLimit: config.memoryLimit ?? '4GB',
    }
  }

  getDatabaseType(): string {
    return DuckDBContainer.DATABASE_TYPE
  }

  getDefaultPort(): number {
    return DuckDBContainer.DEFAULT_PORT
  }

  /**
   * Connect to the DuckDB container.
   * Waits for the database to be ready.
   */
  protected async doConnect(): Promise<void> {
    await this.waitForReady()
  }

  /**
   * Close the DuckDB connection.
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
        query: this.processParams(sql, params),
        format: 'json',
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`DuckDB query failed: ${error}`)
    }

    const result = (await response.json()) as DuckDBQueryResult<T>
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
        query: this.processParams(sql, params),
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`DuckDB execute failed: ${error}`)
    }

    const result = (await response.json()) as { rowsAffected: number }
    return result
  }

  /**
   * Ping DuckDB to verify connectivity.
   */
  protected async doPing(): Promise<void> {
    const response = await this.containerFetch('/health', {
      method: 'GET',
    })

    if (!response.ok) {
      throw new Error('DuckDB health check failed')
    }
  }

  /**
   * Wait for DuckDB to be ready to accept connections.
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

    throw new Error('DuckDB container failed to become ready')
  }

  /**
   * Process parameterized query (replace $1, $2 with values).
   */
  private processParams(sql: string, params?: unknown[]): string {
    if (!params || params.length === 0) {
      return sql
    }

    let processed = sql
    params.forEach((param, index) => {
      const placeholder = `$${index + 1}`
      const value = this.formatValue(param)
      processed = processed.split(placeholder).join(value)
    })

    return processed
  }

  /**
   * Format a value for DuckDB query.
   */
  private formatValue(value: unknown): string {
    if (value === null || value === undefined) {
      return 'NULL'
    }

    if (typeof value === 'string') {
      return `'${value.replace(/'/g, "''")}'`
    }

    if (typeof value === 'number') {
      return String(value)
    }

    if (typeof value === 'boolean') {
      return value ? 'TRUE' : 'FALSE'
    }

    if (value instanceof Date) {
      return `TIMESTAMP '${value.toISOString()}'`
    }

    if (Array.isArray(value)) {
      return `[${value.map((v) => this.formatValue(v)).join(', ')}]`
    }

    return `'${JSON.stringify(value).replace(/'/g, "''")}'`
  }

  // DuckDB-specific operations

  /**
   * Execute a query and return full result with metadata.
   */
  async queryWithMetadata<T = unknown>(sql: string, params?: unknown[]): Promise<DuckDBQueryResult<T>> {
    this.ensureConnected()

    const response = await this.containerFetch('/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: this.processParams(sql, params),
        format: 'json',
        includeMetadata: true,
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`DuckDB query failed: ${error}`)
    }

    return (await response.json()) as DuckDBQueryResult<T>
  }

  /**
   * Load data from a Parquet file (URL or path).
   */
  async readParquet<T = unknown>(path: string): Promise<T[]> {
    this.ensureConnected()
    return this.query<T>(`SELECT * FROM read_parquet('${path}')`)
  }

  /**
   * Load data from a CSV file (URL or path).
   */
  async readCSV<T = unknown>(
    path: string,
    options?: {
      header?: boolean
      delimiter?: string
      columns?: Record<string, string>
    }
  ): Promise<T[]> {
    this.ensureConnected()

    const opts: string[] = []
    if (options?.header !== undefined) {
      opts.push(`header = ${options.header}`)
    }
    if (options?.delimiter) {
      opts.push(`delim = '${options.delimiter}'`)
    }
    if (options?.columns) {
      const colDefs = Object.entries(options.columns)
        .map(([name, type]) => `'${name}': '${type}'`)
        .join(', ')
      opts.push(`columns = {${colDefs}}`)
    }

    const optString = opts.length > 0 ? `, ${opts.join(', ')}` : ''
    return this.query<T>(`SELECT * FROM read_csv('${path}'${optString})`)
  }

  /**
   * Load data from a JSON file (URL or path).
   */
  async readJSON<T = unknown>(path: string): Promise<T[]> {
    this.ensureConnected()
    return this.query<T>(`SELECT * FROM read_json('${path}')`)
  }

  /**
   * Export query results to a format.
   */
  async exportTo(
    sql: string,
    path: string,
    format: DuckDBExportFormat = 'parquet'
  ): Promise<void> {
    this.ensureConnected()

    const copyCmd = format === 'parquet'
      ? `COPY (${sql}) TO '${path}' (FORMAT PARQUET)`
      : format === 'csv'
        ? `COPY (${sql}) TO '${path}' (FORMAT CSV, HEADER TRUE)`
        : `COPY (${sql}) TO '${path}' (FORMAT JSON)`

    await this.execute(copyCmd)
  }

  /**
   * Create a table from a Parquet file.
   */
  async createTableFromParquet(tableName: string, path: string): Promise<void> {
    this.ensureConnected()
    await this.execute(`CREATE TABLE ${tableName} AS SELECT * FROM read_parquet('${path}')`)
  }

  /**
   * Create a table from a CSV file.
   */
  async createTableFromCSV(tableName: string, path: string): Promise<void> {
    this.ensureConnected()
    await this.execute(`CREATE TABLE ${tableName} AS SELECT * FROM read_csv('${path}')`)
  }

  /**
   * Get table information.
   */
  async describeTable(tableName: string): Promise<Array<{ column_name: string; column_type: string; null: string }>> {
    this.ensureConnected()
    return this.query<{ column_name: string; column_type: string; null: string }>(
      `DESCRIBE ${tableName}`
    )
  }

  /**
   * List all tables.
   */
  async listTables(): Promise<string[]> {
    this.ensureConnected()
    const result = await this.query<{ name: string }>(`SHOW TABLES`)
    return result.map((row) => row.name)
  }

  /**
   * Execute an EXPLAIN ANALYZE query.
   */
  async explainAnalyze(sql: string, params?: unknown[]): Promise<string> {
    this.ensureConnected()
    const result = await this.query<{ explain: string }>(
      `EXPLAIN ANALYZE ${this.processParams(sql, params)}`
    )
    return result.map((row) => row.explain).join('\n')
  }

  /**
   * Load an extension.
   */
  async loadExtension(name: string): Promise<void> {
    this.ensureConnected()
    await this.execute(`INSTALL ${name}`)
    await this.execute(`LOAD ${name}`)
  }

  /**
   * Set a DuckDB configuration option.
   */
  async setConfig(key: string, value: string | number | boolean): Promise<void> {
    this.ensureConnected()
    const formattedValue = typeof value === 'string' ? `'${value}'` : String(value)
    await this.execute(`SET ${key} = ${formattedValue}`)
  }

  /**
   * Run analytical aggregation.
   */
  async summarize(tableName: string): Promise<unknown[]> {
    this.ensureConnected()
    return this.query(`SUMMARIZE ${tableName}`)
  }

  /**
   * Get database statistics.
   */
  async getDatabaseSize(): Promise<{ database_size: string; block_size: number; total_blocks: number }> {
    this.ensureConnected()
    const result = await this.query<{ database_size: string; block_size: number; total_blocks: number }>(
      `SELECT * FROM pragma_database_size()`
    )
    return result[0]
  }
}

/**
 * Create a DuckDB container adapter.
 */
export function createDuckDBContainer(
  container: Container,
  options?: Partial<Omit<DuckDBContainerConfig, 'container'>>
): DuckDBContainer {
  return new DuckDBContainer({
    container,
    ...options,
  })
}

/**
 * DuckDB container class for wrangler.toml binding.
 */
export const DuckDBContainerClass = {
  name: 'DuckDBContainer',
  defaultPort: DuckDBContainer.DEFAULT_PORT,
  sleepAfter: '10m',

  /**
   * Generate wrangler.toml container binding.
   */
  toWranglerBinding(bindingName: string, options?: { maxInstances?: number; sleepAfter?: string }): string {
    return `
[[containers]]
binding = "${bindingName}"
class_name = "DuckDBContainer"
image = "./containers/dockerfiles/Dockerfile.duckdb"
max_instances = ${options?.maxInstances ?? 10}
default_port = ${DuckDBContainer.DEFAULT_PORT}
sleep_after = "${options?.sleepAfter ?? '10m'}"
`
  },
}

/**
 * Wrangler configuration for DuckDB container.
 */
export const DUCKDB_WRANGLER_CONFIG = {
  binding: 'DUCKDB_CONTAINER',
  className: 'DuckDBContainer',
  image: './containers/dockerfiles/Dockerfile.duckdb',
  defaultPort: DuckDBContainer.DEFAULT_PORT,
  maxInstances: 10,
  sleepAfter: '10m',
}
