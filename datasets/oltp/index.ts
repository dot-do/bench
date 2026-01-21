/**
 * OLTP Dataset Registry and Types
 *
 * This module provides type definitions and a registry for OLTP benchmark datasets.
 * Each dataset defines schemas, relationships, seed data generators, and benchmark queries.
 */

// Dataset size tiers
export type DatasetSize = '1mb' | '10mb' | '100mb' | '1gb' | '10gb' | '20gb' | '30gb' | '50gb'

// Column data types that work across SQL and document databases
export type ColumnType =
  | 'uuid'
  | 'string'
  | 'text'
  | 'integer'
  | 'bigint'
  | 'decimal'
  | 'boolean'
  | 'timestamp'
  | 'date'
  | 'json'
  | 'array'

// Column definition
export interface ColumnConfig {
  name: string
  type: ColumnType
  nullable?: boolean
  primaryKey?: boolean
  unique?: boolean
  indexed?: boolean
  default?: unknown
  // For string types
  maxLength?: number
  // For decimal types
  precision?: number
  scale?: number
  // For foreign keys
  references?: {
    table: string
    column: string
  }
  // For data generation
  generator?: GeneratorConfig
}

// Index definition
export interface IndexConfig {
  name: string
  columns: string[]
  unique?: boolean
  type?: 'btree' | 'hash' | 'gin' | 'gist'
  where?: string // Partial index condition
}

// Table definition
export interface TableConfig {
  name: string
  columns: ColumnConfig[]
  indexes?: IndexConfig[]
  // For document databases
  embedded?: string[] // Columns to embed as nested documents
  // Partitioning strategy for large tables
  partitionBy?: {
    type: 'range' | 'list' | 'hash'
    column: string
    partitions?: number
  }
}

// Relationship types
export type RelationshipType = 'one-to-one' | 'one-to-many' | 'many-to-many'

// Relationship definition
export interface RelationshipConfig {
  name: string
  type: RelationshipType
  from: {
    table: string
    column: string
  }
  to: {
    table: string
    column: string
  }
  // For many-to-many
  through?: {
    table: string
    fromColumn: string
    toColumn: string
  }
  // For document databases - embed the related docs
  embed?: boolean
  // Cascading behavior
  onDelete?: 'cascade' | 'set-null' | 'restrict'
  onUpdate?: 'cascade' | 'set-null' | 'restrict'
}

// Data generator configuration
export type GeneratorType =
  | 'uuid'
  | 'sequence'
  | 'random-int'
  | 'random-decimal'
  | 'random-string'
  | 'random-text'
  | 'random-boolean'
  | 'timestamp-range'
  | 'date-range'
  | 'enum'
  | 'weighted-enum'
  | 'faker'
  | 'reference'
  | 'computed'

export interface GeneratorConfig {
  type: GeneratorType
  // For random-int
  min?: number
  max?: number
  // For random-decimal
  precision?: number
  // For random-string
  length?: number
  charset?: string
  // For timestamp/date range
  start?: string | Date
  end?: string | Date
  // For enum/weighted-enum
  values?: unknown[]
  weights?: number[]
  // For faker
  fakerMethod?: string
  fakerLocale?: string
  // For reference - pick from existing records
  referenceTable?: string
  referenceColumn?: string
  distribution?: 'uniform' | 'zipf' | 'normal'
  // For computed - depends on other columns
  compute?: (row: Record<string, unknown>) => unknown
}

// Benchmark query definition
export interface BenchmarkQuery {
  name: string
  description: string
  category: 'point-lookup' | 'range-scan' | 'join' | 'aggregate' | 'write' | 'mixed'
  // SQL query template with placeholders
  sql: string
  // For document databases
  documentQuery?: {
    collection: string
    operation: 'find' | 'aggregate' | 'insert' | 'update' | 'delete'
    filter?: Record<string, unknown>
    projection?: Record<string, unknown>
    pipeline?: Record<string, unknown>[]
  }
  // Parameter generators for query execution
  parameters?: GeneratorConfig[]
  // Expected characteristics
  expectedComplexity?: 'O(1)' | 'O(log n)' | 'O(n)' | 'O(n log n)' | 'O(n^2)'
  // Percentage of workload (for mixed workloads)
  weight?: number
}

// Workload profile
export interface WorkloadProfile {
  name: string
  description: string
  readWriteRatio: number // e.g., 0.9 = 90% reads, 10% writes
  queries: BenchmarkQuery[]
  // Target operations per second
  targetOps?: number
  // Number of concurrent connections
  concurrency?: number
  // Duration in seconds
  duration?: number
}

// Size tier configuration
export interface SizeTierConfig {
  size: DatasetSize
  seedCount: Record<string, number>
  // Estimated storage size in bytes
  estimatedBytes: number
  // Recommended resources
  recommendedMemoryMB?: number
  recommendedCores?: number
}

// Main dataset configuration
export interface DatasetConfig {
  name: string
  description: string
  version: string
  tables: TableConfig[]
  relationships: RelationshipConfig[]
  sizeTiers: SizeTierConfig[]
  workloads: WorkloadProfile[]
  // Dataset-specific metadata
  metadata?: Record<string, unknown>
}

// Dataset registry
const datasets: Map<string, DatasetConfig> = new Map()

/**
 * Register a dataset configuration
 */
export function registerDataset(config: DatasetConfig): void {
  datasets.set(config.name, config)
}

/**
 * Get a dataset by name
 */
export function getDataset(name: string): DatasetConfig | undefined {
  return datasets.get(name)
}

/**
 * Get all registered datasets
 */
export function getAllDatasets(): DatasetConfig[] {
  return Array.from(datasets.values())
}

/**
 * Get dataset names
 */
export function getDatasetNames(): string[] {
  return Array.from(datasets.keys())
}

/**
 * Get seed counts for a specific size tier
 */
export function getSeedCounts(
  dataset: DatasetConfig,
  size: DatasetSize
): Record<string, number> | undefined {
  const tier = dataset.sizeTiers.find(t => t.size === size)
  return tier?.seedCount
}

/**
 * Calculate estimated row counts based on size scaling
 */
export function scaleRowCounts(
  baseCounts: Record<string, number>,
  fromSize: DatasetSize,
  toSize: DatasetSize
): Record<string, number> {
  const sizeMultipliers: Record<DatasetSize, number> = {
    '1mb': 1,
    '10mb': 10,
    '100mb': 100,
    '1gb': 1000,
    '10gb': 10000,
    '20gb': 20000,
    '30gb': 30000,
    '50gb': 50000,
  }

  const scale = sizeMultipliers[toSize] / sizeMultipliers[fromSize]
  const result: Record<string, number> = {}

  for (const [table, count] of Object.entries(baseCounts)) {
    result[table] = Math.round(count * scale)
  }

  return result
}

/**
 * Generate SQL DDL for a table
 */
export function generateTableDDL(table: TableConfig, dialect: 'postgres' | 'mysql' | 'sqlite'): string {
  const columnDefs = table.columns.map(col => {
    const typeMap: Record<ColumnType, Record<string, string>> = {
      uuid: { postgres: 'UUID', mysql: 'CHAR(36)', sqlite: 'TEXT' },
      string: { postgres: 'VARCHAR', mysql: 'VARCHAR', sqlite: 'TEXT' },
      text: { postgres: 'TEXT', mysql: 'TEXT', sqlite: 'TEXT' },
      integer: { postgres: 'INTEGER', mysql: 'INT', sqlite: 'INTEGER' },
      bigint: { postgres: 'BIGINT', mysql: 'BIGINT', sqlite: 'INTEGER' },
      decimal: { postgres: 'DECIMAL', mysql: 'DECIMAL', sqlite: 'REAL' },
      boolean: { postgres: 'BOOLEAN', mysql: 'TINYINT(1)', sqlite: 'INTEGER' },
      timestamp: { postgres: 'TIMESTAMP', mysql: 'DATETIME', sqlite: 'TEXT' },
      date: { postgres: 'DATE', mysql: 'DATE', sqlite: 'TEXT' },
      json: { postgres: 'JSONB', mysql: 'JSON', sqlite: 'TEXT' },
      array: { postgres: 'JSONB', mysql: 'JSON', sqlite: 'TEXT' },
    }

    let typeDef = typeMap[col.type][dialect]

    if (col.type === 'string' && col.maxLength) {
      typeDef = `${typeDef}(${col.maxLength})`
    }

    if (col.type === 'decimal' && col.precision && col.scale) {
      typeDef = `${typeDef}(${col.precision}, ${col.scale})`
    }

    const parts = [col.name, typeDef]

    if (col.primaryKey) parts.push('PRIMARY KEY')
    if (!col.nullable && !col.primaryKey) parts.push('NOT NULL')
    if (col.unique && !col.primaryKey) parts.push('UNIQUE')
    if (col.default !== undefined) {
      parts.push(`DEFAULT ${typeof col.default === 'string' ? `'${col.default}'` : col.default}`)
    }

    return parts.join(' ')
  })

  let ddl = `CREATE TABLE ${table.name} (\n  ${columnDefs.join(',\n  ')}\n)`

  return ddl
}

/**
 * Generate SQL DDL for indexes
 */
export function generateIndexDDL(table: TableConfig, dialect: 'postgres' | 'mysql' | 'sqlite'): string[] {
  if (!table.indexes) return []

  return table.indexes.map(idx => {
    const unique = idx.unique ? 'UNIQUE ' : ''
    const columns = idx.columns.join(', ')
    let ddl = `CREATE ${unique}INDEX ${idx.name} ON ${table.name} (${columns})`

    if (idx.where && dialect === 'postgres') {
      ddl += ` WHERE ${idx.where}`
    }

    return ddl
  })
}

// Re-export datasets
export * from './ecommerce'
export * from './saas-multi-tenant'
export * from './social-network'
export * from './iot-timeseries'
