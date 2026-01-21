/**
 * Reference Dataset Registry
 *
 * Real-world reference datasets for database benchmarking:
 * - IMDb: Movie/TV database (~10M titles, ~12M names)
 * - FDA FAERS: Adverse event reports (~25M reports)
 * - Open Food Facts: Food product database (~3.5M products)
 *
 * These datasets provide:
 * - Real-world data distributions and patterns
 * - Complex relationships (many-to-many, hierarchical)
 * - Full-text search workloads
 * - Large-scale aggregation benchmarks
 * - Time-series and temporal analysis
 *
 * Usage:
 * ```ts
 * import { datasets, getDataset, listDatasets } from './datasets/reference'
 *
 * // Get a specific dataset
 * const imdbData = getDataset('imdb')
 *
 * // List all available datasets
 * const all = listDatasets()
 *
 * // Get datasets suited for a specific database
 * const postgresDatasets = listDatasets({ suitedFor: 'postgres' })
 * ```
 */

import { imdb, type IMDbConfig } from './imdb'
import { fdaFaers, type FAERSConfig } from './fda-faers'
import { openFoodFacts, type OpenFoodFactsConfig } from './open-food-facts'

// Re-export types from analytics for consistency
export type {
  DatasetConfig,
  BenchmarkQuery,
  DatabaseType,
  DatasetSize,
  QueryComplexity,
  DownloadConfig,
  SchemaDefinition,
  ColumnDefinition,
  IndexDefinition,
  PartitioningStrategy,
  PerformanceExpectation,
  R2StorageConfig,
} from '../analytics'

/**
 * All available reference datasets
 */
export const datasets = {
  imdb,
  'fda-faers': fdaFaers,
  'open-food-facts': openFoodFacts,
} as const

/**
 * Dataset IDs
 */
export type ReferenceDatasetId = keyof typeof datasets

/**
 * Union type of all reference dataset configs
 */
export type ReferenceDatasetConfig = IMDbConfig | FAERSConfig | OpenFoodFactsConfig

/**
 * Get a specific dataset by ID
 */
export function getDataset(id: ReferenceDatasetId): ReferenceDatasetConfig {
  return datasets[id]
}

/**
 * List datasets with optional filtering
 */
export function listDatasets(options?: {
  suitedFor?: 'db4' | 'evodb' | 'duckdb' | 'clickhouse' | 'sqlite' | 'postgres'
  category?: 'full-text' | 'knowledge-graph' | 'web-analytics' | 'graph-analytics'
  size?: 'small' | 'medium' | 'large' | 'xlarge'
  complexity?: 'simple' | 'moderate' | 'complex' | 'expert'
}): ReferenceDatasetConfig[] {
  let result: ReferenceDatasetConfig[] = []
  for (const key in datasets) {
    result.push(datasets[key as ReferenceDatasetId])
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
export function getQueries(
  datasetId: ReferenceDatasetId,
  complexity?: 'simple' | 'moderate' | 'complex' | 'expert'
) {
  const dataset = getDataset(datasetId)
  if (!complexity) return dataset.queries
  return dataset.queries.filter((q) => q.complexity === complexity)
}

/**
 * Get setup commands for a dataset and target database
 */
export function getSetupCommands(
  datasetId: ReferenceDatasetId,
  database: 'db4' | 'evodb' | 'duckdb' | 'clickhouse' | 'sqlite' | 'postgres',
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
export function generateR2UploadScript(datasetId: ReferenceDatasetId): string {
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

/**
 * Get dataset statistics summary
 */
export function getDatasetStats(): {
  id: ReferenceDatasetId
  name: string
  rowCount: string
  size: string
  tables: number
  queries: number
}[] {
  const result: {
    id: ReferenceDatasetId
    name: string
    rowCount: string
    size: string
    tables: number
    queries: number
  }[] = []

  for (const key in datasets) {
    const id = key as ReferenceDatasetId
    const dataset = datasets[id]
    result.push({
      id,
      name: dataset.name,
      rowCount: dataset.rowCount,
      size: dataset.compressedSize,
      tables: dataset.schema.columns.length > 0 ? 1 : 0, // Simplified; actual tables vary
      queries: dataset.queries.length,
    })
  }

  return result
}

// Re-export individual datasets
export { imdb, fdaFaers, openFoodFacts }

// Re-export types
export type { IMDbConfig } from './imdb'
export type { FAERSConfig } from './fda-faers'
export type { OpenFoodFactsConfig } from './open-food-facts'
