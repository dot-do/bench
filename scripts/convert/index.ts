/**
 * Dataset Conversion Scripts Index
 *
 * Provides unified interface for converting datasets between formats:
 * - SQLite (.db) - for sqlite/postgres WASM
 * - JSONL (.jsonl) - for document stores (@db4/mongo, @dotdo/mongodb)
 * - Parquet (.parquet) - for DuckDB analytics
 * - N-Triples (.nt) - for GraphDB/SDB
 *
 * Usage:
 * ```ts
 * import { convert, convertAll, detectFormat } from './scripts/convert'
 *
 * // Convert single file
 * await convert('data/input.csv', 'output/data.db', { format: 'sqlite' })
 *
 * // Convert to multiple formats
 * await convertAll('data/', 'output/', ['sqlite', 'jsonl', 'parquet'])
 *
 * // Detect input format
 * const format = detectFormat('data/input.parquet') // 'parquet'
 * ```
 *
 * @module scripts/convert
 *
 * Note: These scripts are designed to run in Node.js CLI environment.
 * Use with: npx tsx scripts/convert/index.ts
 */

// Node.js imports - these scripts run in Node.js CLI, not Workers
// @ts-ignore - Node.js types not available in Workers project
import * as fs from 'fs'
// @ts-ignore
import * as path from 'path'

// Import individual converters
import { convert as toSQLite, type ToSQLiteOptions } from './to-sqlite'
import { convert as toJSONL, type ToJSONLOptions } from './to-jsonl'
import { convert as toParquet, type ToParquetOptions } from './to-parquet'
import { convert as toTriples, type ToTriplesOptions } from './to-triples'

// Re-export individual converters
export { toSQLite, toJSONL, toParquet, toTriples }

// Re-export option types
export type {
  ToSQLiteOptions,
  ToJSONLOptions,
  ToParquetOptions,
  ToTriplesOptions,
}

/**
 * Output format type
 */
export type OutputFormat = 'sqlite' | 'jsonl' | 'parquet' | 'triples'

/**
 * Input format type (auto-detected)
 */
export type InputFormat = 'csv' | 'tsv' | 'json' | 'jsonl' | 'parquet' | 'sqlite' | 'nt' | 'unknown'

/**
 * Union type for all conversion options
 */
export type ConversionOptions =
  | ToSQLiteOptions
  | ToJSONLOptions
  | ToParquetOptions
  | ToTriplesOptions

/**
 * Base options common to all converters
 */
export interface BaseConversionOptions {
  format: OutputFormat
  tables?: string[]
}

/**
 * Convert a dataset to the specified format
 *
 * @param inputPath - Path to input file or directory
 * @param outputPath - Path to output file or directory
 * @param options - Conversion options including target format
 * @returns Promise that resolves when conversion is complete
 *
 * @example
 * ```ts
 * // Convert CSV to SQLite
 * await convert('data/users.csv', 'output/users.db', { format: 'sqlite' })
 *
 * // Convert JSON to JSONL with MongoDB compatibility
 * await convert('data/products.json', 'output/products.jsonl', {
 *   format: 'jsonl',
 *   mongoCompatible: true
 * })
 *
 * // Convert CSV to Parquet with partitioning
 * await convert('data/events.csv', 'output/events/', {
 *   format: 'parquet',
 *   partitionBy: ['year', 'month']
 * })
 *
 * // Convert to N-Triples with relationships
 * await convert('data/entities.jsonl', 'output/entities.nt', {
 *   format: 'triples',
 *   baseUri: 'http://example.org/',
 *   relationships: [
 *     { from: 'orders', to: 'customers', foreignKey: 'customer_id', predicate: 'http://example.org/customer' }
 *   ]
 * })
 * ```
 */
export async function convert(
  inputPath: string,
  outputPath: string,
  options: ConversionOptions
): Promise<void> {
  const format = options.format

  switch (format) {
    case 'sqlite':
      await toSQLite(inputPath, outputPath, options as ToSQLiteOptions)
      break
    case 'jsonl':
      await toJSONL(inputPath, outputPath, options as ToJSONLOptions)
      break
    case 'parquet':
      await toParquet(inputPath, outputPath, options as ToParquetOptions)
      break
    case 'triples':
      await toTriples(inputPath, outputPath, options as ToTriplesOptions)
      break
    default:
      throw new Error(`Unsupported output format: ${format}`)
  }
}

/**
 * Convert a dataset to multiple output formats
 *
 * @param inputDir - Path to input directory
 * @param outputDir - Path to output directory
 * @param formats - Array of output formats to generate
 * @param options - Additional options for each format
 * @returns Promise with paths to generated files
 *
 * @example
 * ```ts
 * // Convert all files in input/ to multiple formats
 * const results = await convertAll('input/', 'output/', ['sqlite', 'jsonl', 'parquet'])
 * console.log(results)
 * // {
 * //   sqlite: ['output/sqlite/users.db', 'output/sqlite/orders.db'],
 * //   jsonl: ['output/jsonl/users.jsonl', 'output/jsonl/orders.jsonl'],
 * //   parquet: ['output/parquet/users.parquet', 'output/parquet/orders.parquet']
 * // }
 * ```
 */
export async function convertAll(
  inputDir: string,
  outputDir: string,
  formats: OutputFormat[],
  options?: {
    sqlite?: Partial<ToSQLiteOptions>
    jsonl?: Partial<ToJSONLOptions>
    parquet?: Partial<ToParquetOptions>
    triples?: Partial<ToTriplesOptions>
  }
): Promise<Record<OutputFormat, string[]>> {
  const results: Record<OutputFormat, string[]> = {
    sqlite: [],
    jsonl: [],
    parquet: [],
    triples: [],
  }

  // Get input files
  const inputFiles = getInputFiles(inputDir)

  if (inputFiles.length === 0) {
    console.warn(`No supported input files found in ${inputDir}`)
    return results
  }

  // Process each format
  for (const format of formats) {
    const formatDir = path.join(outputDir, format)

    if (!fs.existsSync(formatDir)) {
      fs.mkdirSync(formatDir, { recursive: true })
    }

    console.log(`\n--- Converting to ${format.toUpperCase()} ---`)

    for (const inputFile of inputFiles) {
      const baseName = path.basename(inputFile, path.extname(inputFile))
      const outputExt = getOutputExtension(format)
      const outputPath = path.join(formatDir, `${baseName}${outputExt}`)

      try {
        const formatOptions = getFormatOptions(format, options)
        await convert(inputFile, outputPath, formatOptions)
        results[format].push(outputPath)
      } catch (err) {
        console.error(`Error converting ${inputFile} to ${format}:`, err)
      }
    }
  }

  return results
}

/**
 * Detect input file format from extension and content
 *
 * @param inputPath - Path to input file
 * @returns Detected input format
 *
 * @example
 * ```ts
 * detectFormat('data/users.csv')     // 'csv'
 * detectFormat('data/events.parquet') // 'parquet'
 * detectFormat('data/graph.nt')       // 'nt'
 * ```
 */
export function detectFormat(inputPath: string): InputFormat {
  // Handle directories
  if (fs.existsSync(inputPath) && fs.statSync(inputPath).isDirectory()) {
    const files = fs.readdirSync(inputPath)
    if (files.length > 0) {
      return detectFormat(path.join(inputPath, files[0]))
    }
    return 'unknown'
  }

  const ext = path.extname(inputPath).toLowerCase()

  switch (ext) {
    case '.csv':
      return 'csv'
    case '.tsv':
      return 'tsv'
    case '.json':
      return 'json'
    case '.jsonl':
    case '.ndjson':
      return 'jsonl'
    case '.parquet':
      return 'parquet'
    case '.db':
    case '.sqlite':
    case '.sqlite3':
      return 'sqlite'
    case '.nt':
    case '.ntriples':
      return 'nt'
    default:
      // Try to detect from content
      if (fs.existsSync(inputPath)) {
        try {
          const sample = fs.readFileSync(inputPath, { encoding: 'utf8', flag: 'r' }).slice(0, 1000)
          if (sample.includes('\t')) return 'tsv'
          if (sample.startsWith('[') || sample.startsWith('{')) return 'json'
          if (sample.includes('<http://')) return 'nt'
        } catch {
          // Binary file or read error
        }
      }
      return 'unknown'
  }
}

/**
 * Get the recommended output format for a given input format
 *
 * @param inputFormat - Input file format
 * @returns Recommended output formats
 */
export function getRecommendedFormats(inputFormat: InputFormat): OutputFormat[] {
  switch (inputFormat) {
    case 'csv':
    case 'tsv':
      return ['sqlite', 'jsonl', 'parquet']
    case 'json':
    case 'jsonl':
      return ['jsonl', 'sqlite', 'parquet', 'triples']
    case 'parquet':
      return ['sqlite', 'jsonl']
    case 'sqlite':
      return ['jsonl', 'parquet', 'triples']
    case 'nt':
      return ['jsonl']
    default:
      return ['sqlite', 'jsonl', 'parquet', 'triples']
  }
}

/**
 * Get file extension for output format
 */
function getOutputExtension(format: OutputFormat): string {
  switch (format) {
    case 'sqlite':
      return '.db'
    case 'jsonl':
      return '.jsonl'
    case 'parquet':
      return '.parquet'
    case 'triples':
      return '.nt'
    default:
      return ''
  }
}

/**
 * Get input files from directory
 */
function getInputFiles(inputDir: string): string[] {
  const supportedExtensions = ['.csv', '.tsv', '.json', '.jsonl', '.ndjson', '.parquet', '.db', '.sqlite']
  const files: string[] = []

  if (!fs.existsSync(inputDir)) {
    return files
  }

  if (fs.statSync(inputDir).isFile()) {
    return [inputDir]
  }

  for (const file of fs.readdirSync(inputDir)) {
    const ext = path.extname(file).toLowerCase()
    if (supportedExtensions.includes(ext)) {
      files.push(path.join(inputDir, file))
    }
  }

  return files
}

/**
 * Get format-specific options
 */
function getFormatOptions(
  format: OutputFormat,
  options?: {
    sqlite?: Partial<ToSQLiteOptions>
    jsonl?: Partial<ToJSONLOptions>
    parquet?: Partial<ToParquetOptions>
    triples?: Partial<ToTriplesOptions>
  }
): ConversionOptions {
  switch (format) {
    case 'sqlite':
      return { format: 'sqlite', ...options?.sqlite } as ToSQLiteOptions
    case 'jsonl':
      return { format: 'jsonl', ...options?.jsonl } as ToJSONLOptions
    case 'parquet':
      return { format: 'parquet', ...options?.parquet } as ToParquetOptions
    case 'triples':
      return { format: 'triples', ...options?.triples } as ToTriplesOptions
    default:
      throw new Error(`Unsupported format: ${format}`)
  }
}

/**
 * Generate conversion pipeline for a dataset definition
 *
 * @param datasetPath - Path to dataset configuration
 * @param outputDir - Output directory
 * @param formats - Target formats
 */
export async function convertDataset(
  datasetPath: string,
  outputDir: string,
  formats: OutputFormat[]
): Promise<Record<OutputFormat, string[]>> {
  // This function would integrate with the dataset definitions in datasets/
  // For now, it uses the basic convertAll function

  const inputDir = path.dirname(datasetPath)
  return convertAll(inputDir, outputDir, formats)
}

/**
 * Utility function to create conversion options from dataset schema
 */
export function createOptionsFromSchema(
  schema: {
    tableName: string
    columns: Array<{
      name: string
      type: string
      primaryKey?: boolean
      references?: { table: string; column: string }
    }>
  },
  format: OutputFormat
): ConversionOptions {
  switch (format) {
    case 'sqlite':
      return {
        format: 'sqlite',
        schema: {
          tableName: schema.tableName,
          columns: schema.columns.map((col) => ({
            name: col.name,
            type: mapTypeToSQLite(col.type),
            primaryKey: col.primaryKey,
          })),
        },
      } as ToSQLiteOptions

    case 'jsonl':
      return {
        format: 'jsonl',
        mongoCompatible: true,
        fieldTransforms: schema.columns.find((c) => c.primaryKey)
          ? { [schema.columns.find((c) => c.primaryKey)!.name]: '_id' }
          : undefined,
      } as ToJSONLOptions

    case 'parquet':
      return {
        format: 'parquet',
        schema: {
          tableName: schema.tableName,
          columns: schema.columns.map((col) => ({
            name: col.name,
            type: mapTypeToParquet(col.type),
          })),
        },
      } as ToParquetOptions

    case 'triples':
      return {
        format: 'triples',
        primaryKey: schema.columns.find((c) => c.primaryKey)?.name || 'id',
        relationships: schema.columns
          .filter((c) => c.references)
          .map((col) => ({
            from: schema.tableName,
            to: col.references!.table,
            foreignKey: col.name,
            predicate: `http://example.org/vocab/${col.name.replace(/_id$/, '')}`,
          })),
      } as ToTriplesOptions

    default:
      throw new Error(`Unsupported format: ${format}`)
  }
}

/**
 * Map generic type to SQLite type
 */
function mapTypeToSQLite(type: string): string {
  const lowerType = type.toLowerCase()
  if (lowerType.includes('int')) return 'INTEGER'
  if (lowerType.includes('float') || lowerType.includes('double') || lowerType.includes('decimal') || lowerType.includes('real')) return 'REAL'
  if (lowerType.includes('bool')) return 'INTEGER'
  if (lowerType.includes('blob') || lowerType.includes('binary')) return 'BLOB'
  return 'TEXT'
}

/**
 * Map generic type to Parquet type
 */
function mapTypeToParquet(
  type: string
): 'BOOLEAN' | 'INT32' | 'INT64' | 'FLOAT' | 'DOUBLE' | 'BYTE_ARRAY' | 'VARCHAR' | 'DATE' | 'TIMESTAMP' | 'TIME' | 'DECIMAL' | 'UUID' | 'JSON' | 'STRUCT' | 'LIST' | 'MAP' {
  const lowerType = type.toLowerCase()
  if (lowerType === 'boolean' || lowerType === 'bool') return 'BOOLEAN'
  if (lowerType === 'integer' || lowerType === 'int' || lowerType === 'int32' || lowerType === 'smallint') return 'INT32'
  if (lowerType === 'bigint' || lowerType === 'int64' || lowerType === 'long') return 'INT64'
  if (lowerType === 'float' || lowerType === 'real') return 'FLOAT'
  if (lowerType === 'double' || lowerType === 'number') return 'DOUBLE'
  if (lowerType === 'decimal' || lowerType === 'numeric') return 'DECIMAL'
  if (lowerType === 'date') return 'DATE'
  if (lowerType === 'timestamp' || lowerType === 'datetime') return 'TIMESTAMP'
  if (lowerType === 'time') return 'TIME'
  if (lowerType === 'uuid') return 'UUID'
  if (lowerType === 'json' || lowerType === 'jsonb') return 'JSON'
  if (lowerType === 'blob' || lowerType === 'binary' || lowerType === 'bytea') return 'BYTE_ARRAY'
  if (lowerType === 'array' || lowerType.includes('[]')) return 'LIST'
  return 'VARCHAR'
}

// Export default convert function
export default convert
