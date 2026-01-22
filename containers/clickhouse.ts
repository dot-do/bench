/**
 * ClickHouse Container Database Adapter
 *
 * Adapter for connecting to ClickHouse running in a Cloudflare Container.
 * ClickHouse provides native HTTP interface, making it ideal for container usage.
 *
 * @see https://developers.cloudflare.com/containers/
 * @see https://clickhouse.com/docs/en/interfaces/http
 */

import type { Container } from '@cloudflare/containers'
import {
  BaseContainerDatabase,
  type ContainerDatabaseConfig,
  type ContainerDatabase,
} from './base.js'

// ClickHouse specific configuration
export interface ClickHouseContainerConfig extends ContainerDatabaseConfig {
  // Database name (default: default)
  database?: string

  // Username (default: default)
  username?: string

  // Password (optional)
  password?: string

  // Output format (default: JSONEachRow)
  format?: 'JSONEachRow' | 'JSON' | 'TabSeparated' | 'CSV'
}

// ClickHouse query result metadata
export interface ClickHouseQueryMeta {
  name: string
  type: string
}

// ClickHouse statistics from query
export interface ClickHouseStatistics {
  elapsed: number
  rows_read: number
  bytes_read: number
}

// ClickHouse JSON format response
export interface ClickHouseJSONResponse<T = unknown> {
  meta: ClickHouseQueryMeta[]
  data: T[]
  rows: number
  statistics: ClickHouseStatistics
}

/**
 * ClickHouse Container Database Adapter
 *
 * Connects to ClickHouse via its native HTTP interface.
 * ClickHouse is optimized for analytical queries and columnar storage.
 */
export class ClickHouseContainer extends BaseContainerDatabase implements ContainerDatabase {
  private chConfig: Required<ClickHouseContainerConfig>

  static readonly DEFAULT_PORT = 8123 // ClickHouse HTTP port
  static readonly DATABASE_TYPE = 'ClickHouse'

  constructor(config: ClickHouseContainerConfig) {
    super(config)

    this.chConfig = {
      ...this.config,
      database: config.database ?? 'default',
      username: config.username ?? 'default',
      password: config.password ?? '',
      format: config.format ?? 'JSONEachRow',
    }
  }

  getDatabaseType(): string {
    return ClickHouseContainer.DATABASE_TYPE
  }

  getDefaultPort(): number {
    return ClickHouseContainer.DEFAULT_PORT
  }

  /**
   * Connect to the ClickHouse container.
   * Waits for the database to be ready.
   */
  protected async doConnect(): Promise<void> {
    await this.waitForReady()
  }

  /**
   * Close the ClickHouse connection.
   */
  protected async doClose(): Promise<void> {
    // HTTP connections are stateless, nothing to close
  }

  /**
   * Execute a query and return results.
   */
  protected async doQuery<T>(sql: string, params?: unknown[]): Promise<T[]> {
    const processedSql = this.processParams(sql, params)
    const url = this.buildUrl(processedSql, 'JSONEachRow')

    const response = await this.containerFetch(url, {
      method: 'POST',
      headers: this.buildHeaders(),
      body: processedSql,
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`ClickHouse query failed: ${error}`)
    }

    // JSONEachRow returns newline-delimited JSON
    const text = await response.text()
    if (!text.trim()) {
      return []
    }

    return text
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line) as T)
  }

  /**
   * Execute a statement and return affected row count.
   */
  protected async doExecute(sql: string, params?: unknown[]): Promise<{ rowsAffected: number }> {
    const processedSql = this.processParams(sql, params)
    const url = this.buildUrl(processedSql)

    const response = await this.containerFetch(url, {
      method: 'POST',
      headers: this.buildHeaders(),
      body: processedSql,
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`ClickHouse execute failed: ${error}`)
    }

    // ClickHouse returns summary header for row count
    const summary = response.headers.get('X-ClickHouse-Summary')
    if (summary) {
      const parsed = JSON.parse(summary) as { written_rows?: string }
      return { rowsAffected: parseInt(parsed.written_rows ?? '0', 10) }
    }

    return { rowsAffected: 0 }
  }

  /**
   * Ping ClickHouse to verify connectivity.
   */
  protected async doPing(): Promise<void> {
    const response = await this.containerFetch('/ping', {
      method: 'GET',
    })

    if (!response.ok) {
      throw new Error('ClickHouse health check failed')
    }
  }

  /**
   * Wait for ClickHouse to be ready to accept connections.
   */
  private async waitForReady(): Promise<void> {
    const maxAttempts = 30
    const delayMs = 1000

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      try {
        const response = await this.containerFetch('/ping', {
          method: 'GET',
        })

        if (response.ok) {
          const text = await response.text()
          if (text.includes('Ok')) {
            return
          }
        }
      } catch {
        // Container may not be ready yet
      }

      await this.sleep(delayMs)
    }

    throw new Error('ClickHouse container failed to become ready')
  }

  /**
   * Build query URL with parameters.
   */
  private buildUrl(query: string, format?: string): string {
    const params = new URLSearchParams({
      database: this.chConfig.database,
      default_format: format ?? this.chConfig.format,
    })

    return `/?${params.toString()}`
  }

  /**
   * Build request headers with authentication.
   */
  private buildHeaders(): Record<string, string> {
    const headers: Record<string, string> = {
      'Content-Type': 'text/plain',
    }

    if (this.chConfig.username) {
      headers['X-ClickHouse-User'] = this.chConfig.username
    }

    if (this.chConfig.password) {
      headers['X-ClickHouse-Key'] = this.chConfig.password
    }

    return headers
  }

  /**
   * Process parameterized query (ClickHouse uses {name:Type} syntax).
   */
  private processParams(sql: string, params?: unknown[]): string {
    if (!params || params.length === 0) {
      return sql
    }

    // Replace positional parameters ($1, $2, etc.) with values
    let processed = sql
    params.forEach((param, index) => {
      const placeholder = `$${index + 1}`
      const value = this.formatValue(param)
      processed = processed.replace(placeholder, value)
    })

    return processed
  }

  /**
   * Format a value for ClickHouse query.
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
      return value ? '1' : '0'
    }

    if (value instanceof Date) {
      return `'${value.toISOString()}'`
    }

    if (Array.isArray(value)) {
      return `[${value.map((v) => this.formatValue(v)).join(', ')}]`
    }

    return `'${JSON.stringify(value).replace(/'/g, "''")}'`
  }

  /**
   * Execute a query with JSON format and return full response.
   */
  async queryJSON<T = unknown>(sql: string, params?: unknown[]): Promise<ClickHouseJSONResponse<T>> {
    this.ensureConnected()

    const processedSql = this.processParams(sql, params)
    const url = this.buildUrl(`${processedSql} FORMAT JSON`)

    const response = await this.containerFetch(url, {
      method: 'POST',
      headers: this.buildHeaders(),
      body: processedSql + ' FORMAT JSON',
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`ClickHouse query failed: ${error}`)
    }

    return (await response.json()) as ClickHouseJSONResponse<T>
  }

  /**
   * Insert data in batch using JSONEachRow format.
   */
  async insertBatch<T extends Record<string, unknown>>(table: string, rows: T[]): Promise<{ rowsAffected: number }> {
    this.ensureConnected()

    if (rows.length === 0) {
      return { rowsAffected: 0 }
    }

    const jsonLines = rows.map((row) => JSON.stringify(row)).join('\n')
    const url = `/?query=INSERT INTO ${table} FORMAT JSONEachRow&database=${this.chConfig.database}`

    const response = await this.containerFetch(url, {
      method: 'POST',
      headers: {
        ...this.buildHeaders(),
        'Content-Type': 'application/json',
      },
      body: jsonLines,
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`ClickHouse insert failed: ${error}`)
    }

    return { rowsAffected: rows.length }
  }

  /**
   * Create a database.
   */
  async createDatabase(name: string): Promise<void> {
    await this.execute(`CREATE DATABASE IF NOT EXISTS ${name}`)
  }

  /**
   * List all databases.
   */
  async listDatabases(): Promise<string[]> {
    const rows = await this.query<{ name: string }>('SHOW DATABASES')
    return rows.map((row) => row.name)
  }

  /**
   * Get table schema.
   */
  async describeTable(table: string): Promise<Array<{ name: string; type: string }>> {
    return this.query<{ name: string; type: string }>(`DESCRIBE TABLE ${table}`)
  }
}

/**
 * Create a ClickHouse container adapter.
 */
export function createClickHouseContainer(
  container: Container,
  options?: Partial<Omit<ClickHouseContainerConfig, 'container'>>
): ClickHouseContainer {
  return new ClickHouseContainer({
    container,
    ...options,
  })
}

/**
 * ClickHouse container class for wrangler.toml binding.
 */
export const ClickHouseContainerClass = {
  name: 'ClickHouseContainer',
  defaultPort: ClickHouseContainer.DEFAULT_PORT,
  sleepAfter: '10m',

  /**
   * Generate wrangler.toml container binding.
   */
  toWranglerBinding(bindingName: string, options?: { maxInstances?: number; sleepAfter?: string }): string {
    return `
[[containers]]
binding = "${bindingName}"
class_name = "ClickHouseContainer"
image = "./containers/dockerfiles/Dockerfile.clickhouse"
max_instances = ${options?.maxInstances ?? 10}
default_port = ${ClickHouseContainer.DEFAULT_PORT}
sleep_after = "${options?.sleepAfter ?? '10m'}"
`
  },
}

/**
 * Wrangler configuration for ClickHouse container.
 */
export const CLICKHOUSE_WRANGLER_CONFIG = {
  binding: 'CLICKHOUSE_CONTAINER',
  className: 'ClickHouseContainer',
  image: './containers/dockerfiles/Dockerfile.clickhouse',
  defaultPort: ClickHouseContainer.DEFAULT_PORT,
  maxInstances: 10,
  sleepAfter: '10m',
}
