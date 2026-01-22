/**
 * PostgreSQL Container Database Adapter
 *
 * Adapter for connecting to PostgreSQL running in a Cloudflare Container.
 * Uses HTTP-based wire protocol for communication.
 *
 * @see https://developers.cloudflare.com/containers/
 */

import type { Container } from '@cloudflare/containers'
import {
  BaseContainerDatabase,
  type ContainerDatabaseConfig,
  type ContainerDatabase,
  type ContainerSize,
} from './base.js'

// PostgreSQL specific configuration
export interface PostgresContainerConfig extends ContainerDatabaseConfig {
  // Database name (default: postgres)
  database?: string

  // Username (default: postgres)
  username?: string

  // Password (optional, depends on pg_hba.conf)
  password?: string

  // Connection string override
  connectionString?: string
}

// PostgreSQL query result
export interface PostgresQueryResult<T = unknown> {
  rows: T[]
  rowCount: number
  fields: Array<{
    name: string
    tableID: number
    columnID: number
    dataTypeID: number
    dataTypeSize: number
    dataTypeModifier: number
    format: string
  }>
  command: string
}

/**
 * PostgreSQL Container Database Adapter
 *
 * Connects to PostgreSQL via an HTTP REST API running in the container.
 * The container runs a lightweight HTTP-to-PostgreSQL bridge.
 */
export class PostgresContainer extends BaseContainerDatabase implements ContainerDatabase {
  private pgConfig: Required<PostgresContainerConfig>

  static readonly DEFAULT_PORT = 5432
  static readonly HTTP_PORT = 8080 // HTTP bridge port
  static readonly DATABASE_TYPE = 'PostgreSQL'

  constructor(config: PostgresContainerConfig) {
    super(config)

    this.pgConfig = {
      ...this.config,
      database: config.database ?? 'postgres',
      username: config.username ?? 'postgres',
      password: config.password ?? '',
      connectionString:
        config.connectionString ?? `postgresql://${config.username ?? 'postgres'}@localhost:5432/${config.database ?? 'postgres'}`,
    }
  }

  getDatabaseType(): string {
    return PostgresContainer.DATABASE_TYPE
  }

  getDefaultPort(): number {
    return PostgresContainer.HTTP_PORT
  }

  /**
   * Connect to the PostgreSQL container.
   * Waits for the database to be ready.
   */
  protected async doConnect(): Promise<void> {
    // Wait for PostgreSQL to be ready by checking health endpoint
    await this.waitForReady()
  }

  /**
   * Close the PostgreSQL connection.
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
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        query: sql,
        params: params ?? [],
        database: this.pgConfig.database,
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`PostgreSQL query failed: ${error}`)
    }

    const result = (await response.json()) as PostgresQueryResult<T>
    return result.rows
  }

  /**
   * Execute a statement and return affected row count.
   */
  protected async doExecute(sql: string, params?: unknown[]): Promise<{ rowsAffected: number }> {
    const response = await this.containerFetch('/execute', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        query: sql,
        params: params ?? [],
        database: this.pgConfig.database,
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`PostgreSQL execute failed: ${error}`)
    }

    const result = (await response.json()) as { rowsAffected: number }
    return result
  }

  /**
   * Ping PostgreSQL to verify connectivity.
   */
  protected async doPing(): Promise<void> {
    const response = await this.containerFetch('/health', {
      method: 'GET',
    })

    if (!response.ok) {
      throw new Error('PostgreSQL health check failed')
    }
  }

  /**
   * Wait for PostgreSQL to be ready to accept connections.
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

    throw new Error('PostgreSQL container failed to become ready')
  }

  /**
   * Execute a transaction with multiple statements.
   */
  async transaction<T>(fn: (execute: (sql: string, params?: unknown[]) => Promise<void>) => Promise<T>): Promise<T> {
    this.ensureConnected()

    // Start transaction
    await this.containerFetch('/transaction/begin', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ database: this.pgConfig.database }),
    })

    try {
      const result = await fn(async (sql, params) => {
        await this.doExecute(sql, params)
      })

      // Commit transaction
      await this.containerFetch('/transaction/commit', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ database: this.pgConfig.database }),
      })

      return result
    } catch (error) {
      // Rollback transaction
      await this.containerFetch('/transaction/rollback', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ database: this.pgConfig.database }),
      })

      throw error
    }
  }

  /**
   * Create a new database.
   */
  async createDatabase(name: string): Promise<void> {
    await this.containerFetch('/admin/create-database', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name }),
    })
  }

  /**
   * List all databases.
   */
  async listDatabases(): Promise<string[]> {
    const response = await this.containerFetch('/admin/databases', {
      method: 'GET',
    })

    if (!response.ok) {
      throw new Error('Failed to list databases')
    }

    const result = (await response.json()) as { databases: string[] }
    return result.databases
  }
}

/**
 * Create a PostgreSQL container adapter.
 */
export function createPostgresContainer(
  container: Container,
  options?: Partial<Omit<PostgresContainerConfig, 'container'>>
): PostgresContainer {
  return new PostgresContainer({
    container,
    ...options,
  })
}

/**
 * PostgreSQL container class for wrangler.toml binding.
 * Extend this class in your worker to customize container behavior.
 */
export const PostgresContainerClass = {
  name: 'PostgresContainer',
  defaultPort: PostgresContainer.HTTP_PORT,
  sleepAfter: '10m',

  /**
   * Generate wrangler.toml container binding.
   */
  toWranglerBinding(bindingName: string, options?: { maxInstances?: number; sleepAfter?: string }): string {
    return `
[[containers]]
binding = "${bindingName}"
class_name = "PostgresContainer"
image = "./containers/dockerfiles/Dockerfile.postgres"
max_instances = ${options?.maxInstances ?? 10}
default_port = ${PostgresContainer.HTTP_PORT}
sleep_after = "${options?.sleepAfter ?? '10m'}"
`
  },
}

/**
 * Wrangler configuration for PostgreSQL container.
 */
export const POSTGRES_WRANGLER_CONFIG = {
  binding: 'POSTGRES_CONTAINER',
  className: 'PostgresContainer',
  image: './containers/dockerfiles/Dockerfile.postgres',
  defaultPort: PostgresContainer.HTTP_PORT,
  maxInstances: 10,
  sleepAfter: '10m',
}
