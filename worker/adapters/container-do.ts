/**
 * Container Durable Objects for Cloudflare Containers
 *
 * Real Cloudflare Container classes that extend the Container base class.
 * These classes run actual containerized databases (PostgreSQL, ClickHouse,
 * MongoDB, DuckDB, SQLite) within Cloudflare's container infrastructure.
 *
 * Each container:
 * - Extends the Container class from @cloudflare/containers
 * - Specifies a Dockerfile image for the database
 * - Configures environment variables and default ports
 * - Implements auto-sleep for cost optimization
 *
 * @see https://developers.cloudflare.com/containers/
 */

import { Container } from '@cloudflare/containers'

// =============================================================================
// PostgreSQL Container
// =============================================================================

/**
 * PostgreSQL container running in Cloudflare Containers.
 * Uses the HTTP bridge on port 8080 for Worker communication.
 */
export class PostgresBenchContainer extends Container {
  /** Default port for HTTP bridge communication */
  defaultPort = 8080

  /** Auto-sleep after 10 minutes of inactivity for cost savings */
  sleepAfter = '10m'

  /** PostgreSQL environment variables */
  envVars = {
    POSTGRES_USER: 'postgres',
    POSTGRES_PASSWORD: 'postgres',
    POSTGRES_DB: 'bench',
  }
}

// =============================================================================
// ClickHouse Container
// =============================================================================

/**
 * ClickHouse container running in Cloudflare Containers.
 * Uses ClickHouse's native HTTP interface on port 8123.
 */
export class ClickHouseBenchContainer extends Container {
  /** Default port for ClickHouse HTTP interface */
  defaultPort = 8123

  /** Auto-sleep after 10 minutes of inactivity */
  sleepAfter = '10m'

  /** ClickHouse environment variables */
  envVars = {
    CLICKHOUSE_USER: 'default',
    CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: '1',
  }
}

// =============================================================================
// MongoDB Container
// =============================================================================

/**
 * MongoDB container running in Cloudflare Containers.
 * Uses the HTTP bridge on port 8080 for Worker communication.
 */
export class MongoBenchContainer extends Container {
  /** Default port for HTTP bridge communication */
  defaultPort = 8080

  /** Auto-sleep after 10 minutes of inactivity */
  sleepAfter = '10m'

  /** MongoDB environment variables */
  envVars = {
    MONGODB_URI: 'mongodb://localhost:27017',
  }
}

// =============================================================================
// DuckDB Container
// =============================================================================

/**
 * DuckDB container running in Cloudflare Containers.
 * Uses the HTTP bridge on port 9999 for Worker communication.
 */
export class DuckDBBenchContainer extends Container {
  /** Default port for HTTP bridge communication */
  defaultPort = 9999

  /** Auto-sleep after 10 minutes of inactivity */
  sleepAfter = '10m'

  /** DuckDB environment variables */
  envVars = {
    DUCKDB_DATABASE: ':memory:',
  }
}

// =============================================================================
// SQLite Container
// =============================================================================

/**
 * SQLite container running in Cloudflare Containers.
 * Uses the HTTP bridge on port 8080 for Worker communication.
 */
export class SQLiteBenchContainer extends Container {
  /** Default port for HTTP bridge communication */
  defaultPort = 8080

  /** Auto-sleep after 10 minutes of inactivity */
  sleepAfter = '10m'

  /** SQLite environment variables */
  envVars = {
    SQLITE_DATABASE: ':memory:',
  }
}
