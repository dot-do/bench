/**
 * Analytics Dataset Registry
 *
 * Real-world datasets for benchmarking analytical databases:
 * - ClickBench: Web analytics (99M rows)
 * - Wiktionary: Full-text search (10GB+)
 * - Wikidata: Knowledge graph (100GB+)
 * - Common Crawl: Web graph analytics
 *
 * Usage:
 * ```ts
 * import { datasets, getDataset, listDatasets } from './datasets/analytics'
 *
 * // Get a specific dataset
 * const clickbench = getDataset('clickbench')
 *
 * // List all available datasets
 * const all = listDatasets()
 *
 * // Get recommended datasets for a database type
 * const duckdbDatasets = listDatasets({ suitedFor: 'duckdb' })
 * ```
 */

import { clickbench, type ClickBenchConfig } from './clickbench'
import { wiktionary, type WiktionaryConfig } from './wiktionary'
import { wikidata, type WikidataConfig } from './wikidata'
import { commonCrawlHostGraph, type CommonCrawlHostGraphConfig } from './common-crawl-hostgraph'

/**
 * Dataset size categories
 */
export type DatasetSize = 'small' | 'medium' | 'large' | 'xlarge'

/**
 * Supported database types for suitability recommendations
 */
export type DatabaseType = 'db4' | 'evodb' | 'duckdb' | 'clickhouse' | 'sqlite' | 'postgres'

/**
 * Query complexity levels
 */
export type QueryComplexity = 'simple' | 'moderate' | 'complex' | 'expert'

/**
 * Base dataset configuration
 */
export interface DatasetConfig {
  /** Unique identifier */
  id: string
  /** Human-readable name */
  name: string
  /** Detailed description */
  description: string
  /** Dataset category */
  category: 'web-analytics' | 'full-text' | 'knowledge-graph' | 'graph-analytics'
  /** Size category */
  size: DatasetSize
  /** Approximate row count */
  rowCount: string
  /** Compressed size */
  compressedSize: string
  /** Uncompressed size */
  uncompressedSize: string
  /** Source URL */
  sourceUrl: string
  /** License information */
  license: string
  /** Databases best suited for this dataset */
  suitedFor: DatabaseType[]
  /** Download configurations for different sizes */
  downloadConfigs: {
    /** Small subset for local testing */
    local: DownloadConfig
    /** Medium subset for development */
    development?: DownloadConfig
    /** Full dataset for production benchmarks */
    production: DownloadConfig
  }
  /** Schema definition */
  schema: SchemaDefinition
  /** Benchmark queries */
  queries: BenchmarkQuery[]
  /** Performance expectations per database */
  performanceExpectations: Record<DatabaseType, PerformanceExpectation>
  /** R2 storage configuration */
  r2Config: R2StorageConfig
}

/**
 * Download configuration for a dataset subset
 */
export interface DownloadConfig {
  /** Download URLs (may be multiple parts) */
  urls: string[]
  /** Expected file size after download */
  size: string
  /** Approximate row count */
  rowCount: string
  /** Checksum for verification */
  checksum?: string
  /** Download instructions */
  instructions: string[]
  /** Setup commands */
  setupCommands: string[]
}

/**
 * Schema definition for a dataset
 */
export interface SchemaDefinition {
  /** Table name */
  tableName: string
  /** Column definitions */
  columns: ColumnDefinition[]
  /** Primary key columns */
  primaryKey?: string[]
  /** Indexes for optimal performance */
  indexes: IndexDefinition[]
  /** Partitioning strategy if applicable */
  partitioning?: PartitioningStrategy
  /** SQL CREATE TABLE statement */
  createTableSQL: Record<DatabaseType, string>
}

/**
 * Column definition
 */
export interface ColumnDefinition {
  name: string
  type: string
  nullable: boolean
  description: string
  /** Type mapping per database */
  typeMapping?: Record<DatabaseType, string>
}

/**
 * Index definition
 */
export interface IndexDefinition {
  name: string
  columns: string[]
  type: 'btree' | 'hash' | 'fulltext' | 'gin' | 'gist' | 'bloom'
  description: string
}

/**
 * Partitioning strategy
 */
export interface PartitioningStrategy {
  type: 'range' | 'hash' | 'list'
  column: string
  granularity?: string
  description: string
}

/**
 * Benchmark query definition
 */
export interface BenchmarkQuery {
  /** Query identifier */
  id: string
  /** Human-readable name */
  name: string
  /** Description of what this query tests */
  description: string
  /** Complexity level */
  complexity: QueryComplexity
  /** SQL query (may have database-specific variants) */
  sql: string | Partial<Record<DatabaseType, string>>
  /** What aspects this query benchmarks */
  benchmarks: string[]
  /** Expected result characteristics */
  expectedResults?: {
    rowCount?: number | string
    columns?: string[]
  }
}

/**
 * Performance expectations for a database
 */
export interface PerformanceExpectation {
  /** Load time expectation */
  loadTime: string
  /** Simple query latency */
  simpleQueryLatency: string
  /** Complex query latency */
  complexQueryLatency: string
  /** Storage efficiency */
  storageEfficiency: string
  /** Concurrency handling */
  concurrency: string
  /** Notes on performance characteristics */
  notes: string[]
}

/**
 * R2 storage configuration for lakehouse pattern
 */
export interface R2StorageConfig {
  /** R2 bucket name pattern */
  bucketName: string
  /** Path prefix for this dataset */
  pathPrefix: string
  /** Recommended file format */
  format: 'parquet' | 'csv' | 'json' | 'arrow'
  /** Compression */
  compression: 'zstd' | 'gzip' | 'snappy' | 'none'
  /** Partitioning for R2 storage */
  partitioning?: {
    columns: string[]
    format: string
  }
  /** Upload instructions */
  uploadInstructions: string[]
  /** Query via DuckDB instructions */
  duckdbInstructions: string[]
}

/**
 * All available datasets
 */
export const datasets = {
  clickbench,
  wiktionary,
  wikidata,
  commonCrawlHostGraph,
} as const

/**
 * Dataset IDs
 */
export type DatasetId = keyof typeof datasets

/**
 * Get a specific dataset by ID
 */
export function getDataset(id: DatasetId): DatasetConfig {
  return datasets[id]
}

/**
 * List datasets with optional filtering
 */
export function listDatasets(options?: {
  suitedFor?: DatabaseType
  category?: DatasetConfig['category']
  size?: DatasetSize
  complexity?: QueryComplexity
}): DatasetConfig[] {
  let result: DatasetConfig[] = []
  for (const key in datasets) {
    result.push(datasets[key as DatasetId])
  }

  if (options?.suitedFor) {
    const targetDb = options.suitedFor
    result = result.filter((d) => d.suitedFor.indexOf(targetDb) !== -1)
  }

  if (options?.category) {
    result = result.filter((d) => d.category === options.category)
  }

  if (options?.size) {
    result = result.filter((d) => d.size === options.size)
  }

  if (options?.complexity) {
    result = result.filter((d) => d.queries.some((q) => q.complexity === options.complexity))
  }

  return result
}

/**
 * Get all queries for a dataset, filtered by complexity
 */
export function getQueries(datasetId: DatasetId, complexity?: QueryComplexity): BenchmarkQuery[] {
  const dataset = getDataset(datasetId)
  if (!complexity) return dataset.queries
  return dataset.queries.filter((q) => q.complexity === complexity)
}

/**
 * Get setup commands for a dataset and target database
 */
export function getSetupCommands(
  datasetId: DatasetId,
  database: DatabaseType,
  subset: 'local' | 'development' | 'production' = 'local'
): string[] {
  const dataset = getDataset(datasetId)
  const config = dataset.downloadConfigs[subset] || dataset.downloadConfigs.local
  const createTable = dataset.schema.createTableSQL[database]

  return [...config.instructions, ...config.setupCommands, `-- Create table:\n${createTable}`]
}

/**
 * Generate R2 upload script for a dataset
 */
export function generateR2UploadScript(datasetId: DatasetId): string {
  const dataset = getDataset(datasetId)
  const { r2Config } = dataset

  return `#!/bin/bash
# R2 Upload Script for ${dataset.name}
# Bucket: ${r2Config.bucketName}
# Format: ${r2Config.format} (${r2Config.compression})

${r2Config.uploadInstructions.join('\n')}

# Query via DuckDB:
${r2Config.duckdbInstructions.map((i) => `# ${i}`).join('\n')}
`
}

// Re-export individual datasets
export { clickbench, wiktionary, wikidata, commonCrawlHostGraph }

// Re-export types
export type { ClickBenchConfig } from './clickbench'
export type { WiktionaryConfig } from './wiktionary'
export type { WikidataConfig } from './wikidata'
export type { CommonCrawlHostGraphConfig } from './common-crawl-hostgraph'
