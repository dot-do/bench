/**
 * Base Container Database Adapter
 *
 * Abstract base class for all Cloudflare Container database adapters.
 * Provides common functionality for container lifecycle management,
 * connection handling, and health checks.
 *
 * @see https://developers.cloudflare.com/containers/
 */

import type { Container } from '@cloudflare/containers'

// Container size tiers available in Cloudflare Containers
export type ContainerSize =
  | 'lite' // 256MB RAM, 0.25 vCPU
  | 'basic' // 512MB RAM, 0.5 vCPU
  | 'standard-1' // 1GB RAM, 1 vCPU
  | 'standard-2' // 2GB RAM, 2 vCPU
  | 'standard-4' // 4GB RAM, 4 vCPU
  | 'performance-8' // 8GB RAM, 8 vCPU
  | 'performance-16' // 16GB RAM, 16 vCPU

// Container database interface that all adapters must implement
export interface ContainerDatabase {
  // Connection lifecycle
  connect(): Promise<void>
  isConnected(): boolean
  close(): Promise<void>

  // Query execution
  query<T = unknown>(sql: string, params?: unknown[]): Promise<T[]>
  execute(sql: string, params?: unknown[]): Promise<{ rowsAffected: number }>

  // Container metadata
  getContainerSize(): ContainerSize
  getStartupTime(): number
  getDatabaseType(): string
  getDefaultPort(): number

  // Health check
  ping(): Promise<boolean>
}

// Configuration for container database adapters
export interface ContainerDatabaseConfig {
  // Container binding from wrangler.toml
  container: Container

  // Session identifier for container affinity
  sessionId?: string

  // Container size tier (affects pricing and resources)
  size?: ContainerSize

  // Connection timeout in milliseconds
  connectionTimeout?: number

  // Maximum number of connection retries
  maxRetries?: number

  // Retry delay in milliseconds
  retryDelay?: number

  // Sleep after inactivity (e.g., '10m', '1h')
  sleepAfter?: string
}

// Connection state
interface ConnectionState {
  connected: boolean
  startupTime: number
  lastPing: number
  retryCount: number
}

/**
 * Abstract base class for container database adapters.
 * Handles common functionality like connection management, retries, and health checks.
 */
export abstract class BaseContainerDatabase implements ContainerDatabase {
  protected config: Required<ContainerDatabaseConfig>
  protected state: ConnectionState

  // Default configuration values
  protected static readonly DEFAULT_SIZE: ContainerSize = 'standard-1'
  protected static readonly DEFAULT_TIMEOUT = 30000 // 30 seconds
  protected static readonly DEFAULT_MAX_RETRIES = 3
  protected static readonly DEFAULT_RETRY_DELAY = 1000 // 1 second
  protected static readonly DEFAULT_SLEEP_AFTER = '10m'

  constructor(config: ContainerDatabaseConfig) {
    this.config = {
      container: config.container,
      sessionId: config.sessionId ?? crypto.randomUUID(),
      size: config.size ?? BaseContainerDatabase.DEFAULT_SIZE,
      connectionTimeout: config.connectionTimeout ?? BaseContainerDatabase.DEFAULT_TIMEOUT,
      maxRetries: config.maxRetries ?? BaseContainerDatabase.DEFAULT_MAX_RETRIES,
      retryDelay: config.retryDelay ?? BaseContainerDatabase.DEFAULT_RETRY_DELAY,
      sleepAfter: config.sleepAfter ?? BaseContainerDatabase.DEFAULT_SLEEP_AFTER,
    }

    this.state = {
      connected: false,
      startupTime: 0,
      lastPing: 0,
      retryCount: 0,
    }
  }

  /**
   * Connect to the container database.
   * Tracks startup time for benchmarking.
   */
  async connect(): Promise<void> {
    if (this.state.connected) {
      return
    }

    const startTime = performance.now()

    try {
      await this.withRetry(async () => {
        await this.doConnect()
      })

      this.state.startupTime = performance.now() - startTime
      this.state.connected = true
      this.state.lastPing = Date.now()
    } catch (error) {
      throw new Error(`Failed to connect to ${this.getDatabaseType()} container: ${error}`)
    }
  }

  /**
   * Check if currently connected to the database.
   */
  isConnected(): boolean {
    return this.state.connected
  }

  /**
   * Close the connection to the container database.
   */
  async close(): Promise<void> {
    if (!this.state.connected) {
      return
    }

    try {
      await this.doClose()
    } finally {
      this.state.connected = false
    }
  }

  /**
   * Execute a query and return results.
   */
  async query<T = unknown>(sql: string, params?: unknown[]): Promise<T[]> {
    this.ensureConnected()
    return this.doQuery<T>(sql, params)
  }

  /**
   * Execute a statement and return affected row count.
   */
  async execute(sql: string, params?: unknown[]): Promise<{ rowsAffected: number }> {
    this.ensureConnected()
    return this.doExecute(sql, params)
  }

  /**
   * Get the configured container size.
   */
  getContainerSize(): ContainerSize {
    return this.config.size
  }

  /**
   * Get the measured startup time in milliseconds.
   */
  getStartupTime(): number {
    return this.state.startupTime
  }

  /**
   * Ping the database to check connectivity.
   */
  async ping(): Promise<boolean> {
    try {
      await this.doPing()
      this.state.lastPing = Date.now()
      return true
    } catch {
      return false
    }
  }

  // Abstract methods to be implemented by concrete adapters
  abstract getDatabaseType(): string
  abstract getDefaultPort(): number

  protected abstract doConnect(): Promise<void>
  protected abstract doClose(): Promise<void>
  protected abstract doQuery<T>(sql: string, params?: unknown[]): Promise<T[]>
  protected abstract doExecute(sql: string, params?: unknown[]): Promise<{ rowsAffected: number }>
  protected abstract doPing(): Promise<void>

  /**
   * Ensure the database is connected before executing operations.
   */
  protected ensureConnected(): void {
    if (!this.state.connected) {
      throw new Error(`Not connected to ${this.getDatabaseType()} container`)
    }
  }

  /**
   * Execute an operation with retry logic.
   */
  protected async withRetry<T>(operation: () => Promise<T>): Promise<T> {
    let lastError: Error | undefined

    for (let attempt = 0; attempt <= this.config.maxRetries; attempt++) {
      try {
        return await operation()
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error))
        this.state.retryCount++

        if (attempt < this.config.maxRetries) {
          await this.sleep(this.config.retryDelay * Math.pow(2, attempt))
        }
      }
    }

    throw lastError ?? new Error('Operation failed after retries')
  }

  /**
   * Make a fetch request to the container.
   */
  protected async containerFetch(path: string, options?: RequestInit): Promise<Response> {
    const url = `http://localhost:${this.getDefaultPort()}${path}`
    const request = new Request(url, options)

    // Use the container binding to route the request
    return this.config.container.fetch(request)
  }

  /**
   * Sleep for specified milliseconds.
   */
  protected sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms))
  }
}

/**
 * Helper function to create a wrangler.toml container binding configuration.
 * Returns the TOML string for the container configuration.
 */
export function createContainerBinding(
  name: string,
  options: {
    className: string
    image: string
    maxInstances?: number
    defaultPort: number
    sleepAfter?: string
  }
): string {
  return `
[[containers]]
binding = "${name}"
class_name = "${options.className}"
image = "${options.image}"
max_instances = ${options.maxInstances ?? 10}
default_port = ${options.defaultPort}
sleep_after = "${options.sleepAfter ?? '10m'}"
`
}

/**
 * Utility to measure container cold start time.
 */
export async function measureColdStart(
  createAdapter: () => ContainerDatabase
): Promise<{ adapter: ContainerDatabase; coldStartMs: number }> {
  const start = performance.now()
  const adapter = createAdapter()
  await adapter.connect()
  const coldStartMs = performance.now() - start

  return { adapter, coldStartMs }
}

/**
 * Utility to run a benchmark query multiple times and return statistics.
 */
export async function benchmarkQuery(
  adapter: ContainerDatabase,
  sql: string,
  params?: unknown[],
  iterations = 100
): Promise<{
  min: number
  max: number
  avg: number
  p50: number
  p95: number
  p99: number
}> {
  const times: number[] = []

  for (let i = 0; i < iterations; i++) {
    const start = performance.now()
    await adapter.query(sql, params)
    times.push(performance.now() - start)
  }

  times.sort((a, b) => a - b)

  return {
    min: times[0],
    max: times[times.length - 1],
    avg: times.reduce((a, b) => a + b, 0) / times.length,
    p50: times[Math.floor(times.length * 0.5)],
    p95: times[Math.floor(times.length * 0.95)],
    p99: times[Math.floor(times.length * 0.99)],
  }
}
