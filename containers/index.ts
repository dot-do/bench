/**
 * Container Database Adapters
 *
 * Exports all container database adapters for Cloudflare Containers.
 * Each adapter connects to a database running in a container via HTTP.
 *
 * @see https://developers.cloudflare.com/containers/
 */

// Base classes and interfaces
export {
  BaseContainerDatabase,
  type ContainerDatabase,
  type ContainerDatabaseConfig,
  type ContainerSize,
  createContainerBinding,
  measureColdStart,
  benchmarkQuery,
} from './base.js'

// PostgreSQL
export {
  PostgresContainer,
  createPostgresContainer,
  PostgresContainerClass,
  POSTGRES_WRANGLER_CONFIG,
  type PostgresContainerConfig,
  type PostgresQueryResult,
} from './postgres.js'

// ClickHouse
export {
  ClickHouseContainer,
  createClickHouseContainer,
  ClickHouseContainerClass,
  CLICKHOUSE_WRANGLER_CONFIG,
  type ClickHouseContainerConfig,
  type ClickHouseQueryMeta,
  type ClickHouseStatistics,
  type ClickHouseJSONResponse,
} from './clickhouse.js'

// MongoDB
export {
  MongoContainer,
  createMongoContainer,
  MongoContainerClass,
  MONGO_WRANGLER_CONFIG,
  type MongoContainerConfig,
  type MongoDocument,
  type MongoFilter,
  type MongoUpdate,
  type MongoPipelineStage,
  type MongoOperationResult,
} from './mongo.js'

// DuckDB
export {
  DuckDBContainer,
  createDuckDBContainer,
  DuckDBContainerClass,
  DUCKDB_WRANGLER_CONFIG,
  type DuckDBContainerConfig,
  type DuckDBQueryResult,
  type DuckDBExportFormat,
} from './duckdb.js'

// SQLite
export {
  SQLiteContainer,
  createSQLiteContainer,
  SQLiteContainerClass,
  SQLITE_WRANGLER_CONFIG,
  type SQLiteContainerConfig,
  type SQLiteQueryResult,
  type SQLiteTransaction,
} from './sqlite.js'

// Import for use in constants below
import { PostgresContainer, PostgresContainerClass, POSTGRES_WRANGLER_CONFIG } from './postgres.js'
import { ClickHouseContainer, ClickHouseContainerClass, CLICKHOUSE_WRANGLER_CONFIG } from './clickhouse.js'
import { MongoContainer, MongoContainerClass, MONGO_WRANGLER_CONFIG } from './mongo.js'
import { DuckDBContainer, DuckDBContainerClass, DUCKDB_WRANGLER_CONFIG } from './duckdb.js'
import { SQLiteContainer, SQLiteContainerClass, SQLITE_WRANGLER_CONFIG } from './sqlite.js'

/**
 * All container database adapters.
 */
export const ContainerAdapters = {
  postgres: PostgresContainer,
  clickhouse: ClickHouseContainer,
  mongo: MongoContainer,
  duckdb: DuckDBContainer,
  sqlite: SQLiteContainer,
} as const

/**
 * All wrangler configurations.
 */
export const WranglerConfigs = {
  postgres: POSTGRES_WRANGLER_CONFIG,
  clickhouse: CLICKHOUSE_WRANGLER_CONFIG,
  mongo: MONGO_WRANGLER_CONFIG,
  duckdb: DUCKDB_WRANGLER_CONFIG,
  sqlite: SQLITE_WRANGLER_CONFIG,
} as const

/**
 * All container class helpers for wrangler binding generation.
 */
export const ContainerClasses = {
  postgres: PostgresContainerClass,
  clickhouse: ClickHouseContainerClass,
  mongo: MongoContainerClass,
  duckdb: DuckDBContainerClass,
  sqlite: SQLiteContainerClass,
} as const

/**
 * Database type identifier.
 */
export type DatabaseType = keyof typeof ContainerAdapters

/**
 * Generate complete wrangler.toml container bindings for all databases.
 */
export function generateAllWranglerBindings(options?: {
  prefix?: string
  maxInstances?: number
  sleepAfter?: string
}): string {
  const prefix = options?.prefix ?? ''
  const configs = [
    PostgresContainerClass.toWranglerBinding(`${prefix}POSTGRES`, options),
    ClickHouseContainerClass.toWranglerBinding(`${prefix}CLICKHOUSE`, options),
    MongoContainerClass.toWranglerBinding(`${prefix}MONGO`, options),
    DuckDBContainerClass.toWranglerBinding(`${prefix}DUCKDB`, options),
    SQLiteContainerClass.toWranglerBinding(`${prefix}SQLITE`, options),
  ]

  return configs.join('\n')
}

/**
 * Default ports for each database type.
 */
export const DEFAULT_PORTS = {
  postgres: PostgresContainer.HTTP_PORT,
  clickhouse: ClickHouseContainer.DEFAULT_PORT,
  mongo: MongoContainer.HTTP_PORT,
  duckdb: DuckDBContainer.DEFAULT_PORT,
  sqlite: SQLiteContainer.DEFAULT_PORT,
} as const

/**
 * Database type display names.
 */
export const DATABASE_NAMES = {
  postgres: 'PostgreSQL',
  clickhouse: 'ClickHouse',
  mongo: 'MongoDB',
  duckdb: 'DuckDB',
  sqlite: 'SQLite',
} as const

// Re-export Container type from SDK for convenience
export type { Container } from '@cloudflare/containers'
