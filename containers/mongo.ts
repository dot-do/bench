/**
 * MongoDB Container Database Adapter
 *
 * Adapter for connecting to MongoDB running in a Cloudflare Container.
 * Uses HTTP-based API for communication since Workers cannot use native MongoDB protocol.
 *
 * @see https://developers.cloudflare.com/containers/
 */

import type { Container } from '@cloudflare/containers'
import {
  BaseContainerDatabase,
  type ContainerDatabaseConfig,
  type ContainerDatabase,
} from './base.js'

// MongoDB specific configuration
export interface MongoContainerConfig extends ContainerDatabaseConfig {
  // Database name (default: test)
  database?: string

  // Username (optional)
  username?: string

  // Password (optional)
  password?: string

  // Authentication database (default: admin)
  authDatabase?: string
}

// MongoDB document type
export type MongoDocument = Record<string, unknown> & { _id?: string | { $oid: string } }

// MongoDB query filter
export type MongoFilter = Record<string, unknown>

// MongoDB update operations
export type MongoUpdate = {
  $set?: Record<string, unknown>
  $unset?: Record<string, unknown>
  $inc?: Record<string, number>
  $push?: Record<string, unknown>
  $pull?: Record<string, unknown>
  $addToSet?: Record<string, unknown>
}

// MongoDB aggregation pipeline stage
export type MongoPipelineStage = Record<string, unknown>

// MongoDB operation result
export interface MongoOperationResult {
  acknowledged: boolean
  insertedId?: string
  insertedCount?: number
  matchedCount?: number
  modifiedCount?: number
  deletedCount?: number
  upsertedId?: string
  upsertedCount?: number
}

/**
 * MongoDB Container Database Adapter
 *
 * Connects to MongoDB via an HTTP REST API running in the container.
 * The container runs a lightweight HTTP-to-MongoDB bridge.
 */
export class MongoContainer extends BaseContainerDatabase implements ContainerDatabase {
  private mongoConfig: Required<MongoContainerConfig>

  static readonly DEFAULT_PORT = 27017 // Native MongoDB port
  static readonly HTTP_PORT = 8080 // HTTP bridge port
  static readonly DATABASE_TYPE = 'MongoDB'

  constructor(config: MongoContainerConfig) {
    super(config)

    this.mongoConfig = {
      ...this.config,
      database: config.database ?? 'test',
      username: config.username ?? '',
      password: config.password ?? '',
      authDatabase: config.authDatabase ?? 'admin',
    }
  }

  getDatabaseType(): string {
    return MongoContainer.DATABASE_TYPE
  }

  getDefaultPort(): number {
    return MongoContainer.HTTP_PORT
  }

  /**
   * Connect to the MongoDB container.
   * Waits for the database to be ready.
   */
  protected async doConnect(): Promise<void> {
    await this.waitForReady()
  }

  /**
   * Close the MongoDB connection.
   */
  protected async doClose(): Promise<void> {
    // HTTP connections are stateless, nothing to close
  }

  /**
   * Execute a SQL-like query (translated to MongoDB operations).
   * This is provided for interface compatibility; prefer using MongoDB-native methods.
   */
  protected async doQuery<T>(sql: string, params?: unknown[]): Promise<T[]> {
    // Simple SQL to MongoDB translation for basic SELECT statements
    // Format: SELECT * FROM collection WHERE field = value
    const selectMatch = sql.match(/SELECT\s+\*\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?/i)

    if (selectMatch) {
      const [, collection, whereClause] = selectMatch
      const filter = whereClause ? this.parseWhereClause(whereClause, params) : {}
      return this.find<T>(collection, filter)
    }

    throw new Error('MongoDB adapter requires MongoDB-native methods. Use find(), insertOne(), etc.')
  }

  /**
   * Execute a SQL-like statement (translated to MongoDB operations).
   */
  protected async doExecute(sql: string, params?: unknown[]): Promise<{ rowsAffected: number }> {
    // Simple SQL to MongoDB translation for basic INSERT/UPDATE/DELETE
    const insertMatch = sql.match(/INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)/i)
    const updateMatch = sql.match(/UPDATE\s+(\w+)\s+SET\s+(.+)\s+WHERE\s+(.+)/i)
    const deleteMatch = sql.match(/DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?/i)

    if (insertMatch) {
      const [, collection, fieldStr, valueStr] = insertMatch
      const fields = fieldStr.split(',').map((f) => f.trim())
      const values = this.parseValues(valueStr, params)
      const doc: MongoDocument = {}
      fields.forEach((field, i) => {
        doc[field] = values[i]
      })
      const result = await this.insertOne(collection, doc)
      return { rowsAffected: result.insertedCount ?? 0 }
    }

    if (updateMatch) {
      const [, collection, setClause, whereClause] = updateMatch
      const update = this.parseSetClause(setClause, params)
      const filter = this.parseWhereClause(whereClause, params)
      const result = await this.updateMany(collection, filter, { $set: update })
      return { rowsAffected: result.modifiedCount ?? 0 }
    }

    if (deleteMatch) {
      const [, collection, whereClause] = deleteMatch
      const filter = whereClause ? this.parseWhereClause(whereClause, params) : {}
      const result = await this.deleteMany(collection, filter)
      return { rowsAffected: result.deletedCount ?? 0 }
    }

    throw new Error('MongoDB adapter requires MongoDB-native methods')
  }

  /**
   * Ping MongoDB to verify connectivity.
   */
  protected async doPing(): Promise<void> {
    const response = await this.containerFetch('/health', {
      method: 'GET',
    })

    if (!response.ok) {
      throw new Error('MongoDB health check failed')
    }
  }

  /**
   * Wait for MongoDB to be ready to accept connections.
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

    throw new Error('MongoDB container failed to become ready')
  }

  // MongoDB-native operations

  /**
   * Find documents in a collection.
   */
  async find<T = MongoDocument>(
    collection: string,
    filter: MongoFilter = {},
    options?: {
      projection?: Record<string, 0 | 1>
      sort?: Record<string, 1 | -1>
      limit?: number
      skip?: number
    }
  ): Promise<T[]> {
    this.ensureConnected()

    const response = await this.containerFetch('/find', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        database: this.mongoConfig.database,
        collection,
        filter,
        ...options,
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`MongoDB find failed: ${error}`)
    }

    const result = (await response.json()) as { documents: T[] }
    return result.documents
  }

  /**
   * Find a single document.
   */
  async findOne<T = MongoDocument>(
    collection: string,
    filter: MongoFilter = {},
    options?: { projection?: Record<string, 0 | 1> }
  ): Promise<T | null> {
    this.ensureConnected()

    const response = await this.containerFetch('/findOne', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        database: this.mongoConfig.database,
        collection,
        filter,
        ...options,
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`MongoDB findOne failed: ${error}`)
    }

    const result = (await response.json()) as { document: T | null }
    return result.document
  }

  /**
   * Insert a single document.
   */
  async insertOne(collection: string, document: MongoDocument): Promise<MongoOperationResult> {
    this.ensureConnected()

    const response = await this.containerFetch('/insertOne', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        database: this.mongoConfig.database,
        collection,
        document,
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`MongoDB insertOne failed: ${error}`)
    }

    return (await response.json()) as MongoOperationResult
  }

  /**
   * Insert multiple documents.
   */
  async insertMany(collection: string, documents: MongoDocument[]): Promise<MongoOperationResult> {
    this.ensureConnected()

    const response = await this.containerFetch('/insertMany', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        database: this.mongoConfig.database,
        collection,
        documents,
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`MongoDB insertMany failed: ${error}`)
    }

    return (await response.json()) as MongoOperationResult
  }

  /**
   * Update a single document.
   */
  async updateOne(
    collection: string,
    filter: MongoFilter,
    update: MongoUpdate,
    options?: { upsert?: boolean }
  ): Promise<MongoOperationResult> {
    this.ensureConnected()

    const response = await this.containerFetch('/updateOne', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        database: this.mongoConfig.database,
        collection,
        filter,
        update,
        ...options,
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`MongoDB updateOne failed: ${error}`)
    }

    return (await response.json()) as MongoOperationResult
  }

  /**
   * Update multiple documents.
   */
  async updateMany(
    collection: string,
    filter: MongoFilter,
    update: MongoUpdate,
    options?: { upsert?: boolean }
  ): Promise<MongoOperationResult> {
    this.ensureConnected()

    const response = await this.containerFetch('/updateMany', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        database: this.mongoConfig.database,
        collection,
        filter,
        update,
        ...options,
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`MongoDB updateMany failed: ${error}`)
    }

    return (await response.json()) as MongoOperationResult
  }

  /**
   * Delete a single document.
   */
  async deleteOne(collection: string, filter: MongoFilter): Promise<MongoOperationResult> {
    this.ensureConnected()

    const response = await this.containerFetch('/deleteOne', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        database: this.mongoConfig.database,
        collection,
        filter,
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`MongoDB deleteOne failed: ${error}`)
    }

    return (await response.json()) as MongoOperationResult
  }

  /**
   * Delete multiple documents.
   */
  async deleteMany(collection: string, filter: MongoFilter): Promise<MongoOperationResult> {
    this.ensureConnected()

    const response = await this.containerFetch('/deleteMany', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        database: this.mongoConfig.database,
        collection,
        filter,
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`MongoDB deleteMany failed: ${error}`)
    }

    return (await response.json()) as MongoOperationResult
  }

  /**
   * Run an aggregation pipeline.
   */
  async aggregate<T = MongoDocument>(collection: string, pipeline: MongoPipelineStage[]): Promise<T[]> {
    this.ensureConnected()

    const response = await this.containerFetch('/aggregate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        database: this.mongoConfig.database,
        collection,
        pipeline,
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`MongoDB aggregate failed: ${error}`)
    }

    const result = (await response.json()) as { documents: T[] }
    return result.documents
  }

  /**
   * Count documents matching a filter.
   */
  async countDocuments(collection: string, filter: MongoFilter = {}): Promise<number> {
    this.ensureConnected()

    const response = await this.containerFetch('/count', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        database: this.mongoConfig.database,
        collection,
        filter,
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`MongoDB count failed: ${error}`)
    }

    const result = (await response.json()) as { count: number }
    return result.count
  }

  /**
   * Create an index on a collection.
   */
  async createIndex(
    collection: string,
    keys: Record<string, 1 | -1>,
    options?: { unique?: boolean; name?: string }
  ): Promise<string> {
    this.ensureConnected()

    const response = await this.containerFetch('/createIndex', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        database: this.mongoConfig.database,
        collection,
        keys,
        ...options,
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`MongoDB createIndex failed: ${error}`)
    }

    const result = (await response.json()) as { indexName: string }
    return result.indexName
  }

  /**
   * List all collections in the current database.
   */
  async listCollections(): Promise<string[]> {
    this.ensureConnected()

    const response = await this.containerFetch('/listCollections', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        database: this.mongoConfig.database,
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`MongoDB listCollections failed: ${error}`)
    }

    const result = (await response.json()) as { collections: string[] }
    return result.collections
  }

  // Helper methods for SQL parsing

  private parseWhereClause(clause: string, params?: unknown[]): MongoFilter {
    // Simple WHERE clause parsing (field = value, field = $1, etc.)
    const filter: MongoFilter = {}
    const conditions = clause.split(/\s+AND\s+/i)

    for (const condition of conditions) {
      const match = condition.match(/(\w+)\s*=\s*(.+)/)
      if (match) {
        const [, field, value] = match
        const trimmedValue = value.trim()

        if (trimmedValue.startsWith('$') && params) {
          const paramIndex = parseInt(trimmedValue.slice(1), 10) - 1
          filter[field] = params[paramIndex]
        } else if (trimmedValue.startsWith("'") && trimmedValue.endsWith("'")) {
          filter[field] = trimmedValue.slice(1, -1)
        } else {
          filter[field] = isNaN(Number(trimmedValue)) ? trimmedValue : Number(trimmedValue)
        }
      }
    }

    return filter
  }

  private parseSetClause(clause: string, params?: unknown[]): Record<string, unknown> {
    const update: Record<string, unknown> = {}
    const assignments = clause.split(',')

    for (const assignment of assignments) {
      const match = assignment.match(/(\w+)\s*=\s*(.+)/)
      if (match) {
        const [, field, value] = match
        const trimmedValue = value.trim()

        if (trimmedValue.startsWith('$') && params) {
          const paramIndex = parseInt(trimmedValue.slice(1), 10) - 1
          update[field] = params[paramIndex]
        } else if (trimmedValue.startsWith("'") && trimmedValue.endsWith("'")) {
          update[field] = trimmedValue.slice(1, -1)
        } else {
          update[field] = isNaN(Number(trimmedValue)) ? trimmedValue : Number(trimmedValue)
        }
      }
    }

    return update
  }

  private parseValues(valueStr: string, params?: unknown[]): unknown[] {
    const values: unknown[] = []
    const parts = valueStr.split(',')

    for (const part of parts) {
      const trimmed = part.trim()

      if (trimmed.startsWith('$') && params) {
        const paramIndex = parseInt(trimmed.slice(1), 10) - 1
        values.push(params[paramIndex])
      } else if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
        values.push(trimmed.slice(1, -1))
      } else {
        values.push(isNaN(Number(trimmed)) ? trimmed : Number(trimmed))
      }
    }

    return values
  }
}

/**
 * Create a MongoDB container adapter.
 */
export function createMongoContainer(
  container: Container,
  options?: Partial<Omit<MongoContainerConfig, 'container'>>
): MongoContainer {
  return new MongoContainer({
    container,
    ...options,
  })
}

/**
 * MongoDB container class for wrangler.toml binding.
 */
export const MongoContainerClass = {
  name: 'MongoContainer',
  defaultPort: MongoContainer.HTTP_PORT,
  sleepAfter: '10m',

  /**
   * Generate wrangler.toml container binding.
   */
  toWranglerBinding(bindingName: string, options?: { maxInstances?: number; sleepAfter?: string }): string {
    return `
[[containers]]
binding = "${bindingName}"
class_name = "MongoContainer"
image = "./containers/dockerfiles/Dockerfile.mongo"
max_instances = ${options?.maxInstances ?? 10}
default_port = ${MongoContainer.HTTP_PORT}
sleep_after = "${options?.sleepAfter ?? '10m'}"
`
  },
}

/**
 * Wrangler configuration for MongoDB container.
 */
export const MONGO_WRANGLER_CONFIG = {
  binding: 'MONGO_CONTAINER',
  className: 'MongoContainer',
  image: './containers/dockerfiles/Dockerfile.mongo',
  defaultPort: MongoContainer.HTTP_PORT,
  maxInstances: 10,
  sleepAfter: '10m',
}
