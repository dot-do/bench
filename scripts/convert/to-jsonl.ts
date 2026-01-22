/**
 * Convert datasets to JSONL format
 *
 * Converts various input formats (CSV, TSV, JSON, SQLite, Parquet) to JSONL.
 * Suitable for document stores like @db4/mongo and @dotdo/mongodb.
 *
 * Features:
 * - One .jsonl file per table/collection
 * - Preserves relationships via embedded documents or references
 * - Supports nested document structures
 * - Streaming conversion for large files
 *
 * @module scripts/convert/to-jsonl
 *
 * Note: This script is designed to run in Node.js CLI environment.
 * Use with: npx tsx scripts/convert/to-jsonl.ts
 */

// Node.js imports - these scripts run in Node.js CLI, not Workers
// @ts-ignore - Node.js types not available in Workers project
import * as fs from 'fs'
// @ts-ignore
import * as path from 'path'
// @ts-ignore
import { createReadStream, createWriteStream } from 'fs'
// @ts-ignore
import { createInterface } from 'readline'
// @ts-ignore
import { Transform } from 'stream'

// Remove unused import
// import { pipeline } from 'stream/promises'

/**
 * Supported input file formats
 */
type InputFormat = 'csv' | 'tsv' | 'json' | 'jsonl' | 'parquet' | 'sqlite'

/**
 * Conversion options for JSONL
 */
export interface ToJSONLOptions {
  format: 'jsonl'
  tables?: string[]
  /** Pretty print JSON (default: false for space efficiency) */
  prettyPrint?: boolean
  /** Embed related documents (default: false) */
  embedRelations?: boolean
  /** Relations to embed as nested documents */
  relations?: RelationConfig[]
  /** Transform field names (e.g., id -> _id for MongoDB) */
  fieldTransforms?: Record<string, string>
  /** Add MongoDB-compatible _id field (default: true) */
  mongoCompatible?: boolean
  /** Batch size for streaming (default: 1000) */
  batchSize?: number
  /** Schema definition for type coercion */
  schema?: SchemaDefinition
}

/**
 * Relation configuration for embedding
 */
export interface RelationConfig {
  /** Parent collection name */
  from: string
  /** Child collection name */
  to: string
  /** Foreign key field in child */
  foreignKey: string
  /** Field name for embedded array */
  embedAs: string
  /** Fields to include in embedded documents */
  include?: string[]
  /** Fields to exclude from embedded documents */
  exclude?: string[]
}

/**
 * Schema definition for type coercion
 */
export interface SchemaDefinition {
  tableName: string
  columns: Array<{
    name: string
    type: string
    primaryKey?: boolean
  }>
}

/**
 * Convert a dataset to JSONL format
 *
 * @param inputPath - Path to input file or directory
 * @param outputPath - Path to output file or directory
 * @param options - Conversion options
 */
export async function convert(
  inputPath: string,
  outputPath: string,
  options?: ToJSONLOptions
): Promise<void> {
  const opts: ToJSONLOptions = {
    format: 'jsonl',
    prettyPrint: false,
    embedRelations: false,
    mongoCompatible: true,
    batchSize: 1000,
    ...options,
  }

  // Ensure output directory exists
  const outputDir = fs.statSync(inputPath).isDirectory()
    ? outputPath
    : path.dirname(outputPath)
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true })
  }

  // Determine input format
  const inputFormat = detectInputFormat(inputPath)

  // Process based on input
  if (fs.statSync(inputPath).isDirectory()) {
    await convertDirectory(inputPath, outputPath, opts)
  } else {
    await convertFile(inputPath, outputPath, inputFormat, opts)
  }

  console.log(`Converted to JSONL: ${outputPath}`)
}

/**
 * Detect input file format from extension
 */
function detectInputFormat(inputPath: string): InputFormat {
  if (fs.statSync(inputPath).isDirectory()) {
    // Check first file in directory
    const files = fs.readdirSync(inputPath)
    if (files.length > 0) {
      return detectInputFormat(path.join(inputPath, files[0]))
    }
    return 'json'
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
    default:
      return 'json'
  }
}

/**
 * Convert a directory of files
 */
async function convertDirectory(
  inputDir: string,
  outputDir: string,
  opts: ToJSONLOptions
): Promise<void> {
  const files = fs.readdirSync(inputDir)

  for (const file of files) {
    const inputPath = path.join(inputDir, file)
    const baseName = path.basename(file, path.extname(file))
    const outputPath = path.join(outputDir, `${baseName}.jsonl`)

    // Skip if tables filter is specified and doesn't match
    if (opts.tables && !opts.tables.includes(baseName)) {
      continue
    }

    const format = detectInputFormat(inputPath)
    await convertFile(inputPath, outputPath, format, opts)
  }
}

/**
 * Convert a single file
 */
async function convertFile(
  inputPath: string,
  outputPath: string,
  format: InputFormat,
  opts: ToJSONLOptions
): Promise<void> {
  switch (format) {
    case 'csv':
    case 'tsv':
      await convertDelimitedToJSONL(inputPath, outputPath, format === 'tsv' ? '\t' : ',', opts)
      break
    case 'json':
      await convertJSONToJSONL(inputPath, outputPath, opts)
      break
    case 'jsonl':
      await transformJSONL(inputPath, outputPath, opts)
      break
    case 'parquet':
      await convertParquetToJSONL(inputPath, outputPath, opts)
      break
    case 'sqlite':
      await convertSQLiteToJSONL(inputPath, outputPath, opts)
      break
    default:
      throw new Error(`Unsupported input format: ${format}`)
  }
}

/**
 * Convert delimited (CSV/TSV) file to JSONL
 */
async function convertDelimitedToJSONL(
  inputPath: string,
  outputPath: string,
  delimiter: string,
  opts: ToJSONLOptions
): Promise<void> {
  let headers: string[] = []
  let lineCount = 0

  const writeStream = createWriteStream(outputPath)

  const rl = createInterface({
    input: createReadStream(inputPath),
    crlfDelay: Infinity,
  })

  for await (const line of rl) {
    if (lineCount === 0) {
      headers = parseDelimitedLine(line, delimiter)
      lineCount++
      continue
    }

    const values = parseDelimitedLine(line, delimiter)
    const record = createRecord(headers, values, opts)

    if (record) {
      const json = opts.prettyPrint
        ? JSON.stringify(record, null, 2)
        : JSON.stringify(record)
      writeStream.write(json + '\n')
    }

    lineCount++
  }

  writeStream.end()
  console.log(`Converted ${lineCount - 1} records to ${outputPath}`)
}

/**
 * Convert JSON array file to JSONL
 */
async function convertJSONToJSONL(
  inputPath: string,
  outputPath: string,
  opts: ToJSONLOptions
): Promise<void> {
  const content = fs.readFileSync(inputPath, 'utf8')
  const data = JSON.parse(content)

  const writeStream = createWriteStream(outputPath)
  const records = Array.isArray(data) ? data : [data]

  for (const record of records) {
    const transformed = transformRecord(record, opts)
    const json = opts.prettyPrint
      ? JSON.stringify(transformed, null, 2)
      : JSON.stringify(transformed)
    writeStream.write(json + '\n')
  }

  writeStream.end()
  console.log(`Converted ${records.length} records to ${outputPath}`)
}

/**
 * Transform existing JSONL file
 */
async function transformJSONL(
  inputPath: string,
  outputPath: string,
  opts: ToJSONLOptions
): Promise<void> {
  // If no transformations needed, just copy
  if (!opts.fieldTransforms && !opts.mongoCompatible) {
    fs.copyFileSync(inputPath, outputPath)
    return
  }

  const transformStream = new Transform({
    objectMode: true,
    transform(chunk: unknown, _encoding: string, callback: (err: Error | null, data?: string) => void) {
      try {
        const record = JSON.parse(String(chunk))
        const transformed = transformRecord(record, opts)
        const json = opts.prettyPrint
          ? JSON.stringify(transformed, null, 2)
          : JSON.stringify(transformed)
        callback(null, json + '\n')
      } catch (err) {
        callback(err as Error)
      }
    },
  })

  const rl = createInterface({
    input: createReadStream(inputPath),
    crlfDelay: Infinity,
  })

  const writeStream = createWriteStream(outputPath)

  for await (const line of rl) {
    if (line.trim()) {
      try {
        const record = JSON.parse(line)
        const transformed = transformRecord(record, opts)
        const json = opts.prettyPrint
          ? JSON.stringify(transformed, null, 2)
          : JSON.stringify(transformed)
        writeStream.write(json + '\n')
      } catch (err) {
        console.error(`Error parsing line: ${line.slice(0, 100)}...`)
      }
    }
  }

  writeStream.end()
}

/**
 * Convert Parquet file to JSONL using DuckDB
 */
async function convertParquetToJSONL(
  inputPath: string,
  outputPath: string,
  opts: ToJSONLOptions
): Promise<void> {
  // Generate DuckDB command for conversion
  const scriptPath = outputPath.replace('.jsonl', '_convert.sql')
  const tempJsonPath = outputPath.replace('.jsonl', '_temp.json')

  const script = `
-- Convert Parquet to JSONL using DuckDB
-- Run: duckdb < ${scriptPath}

COPY (SELECT * FROM read_parquet('${inputPath}'))
TO '${tempJsonPath}' (FORMAT JSON, ARRAY true);

-- Then convert JSON array to JSONL:
-- node -e "const fs=require('fs'); const d=JSON.parse(fs.readFileSync('${tempJsonPath}')); d.forEach(r=>console.log(JSON.stringify(r)))" > ${outputPath}
`

  fs.writeFileSync(scriptPath, script)
  console.log(`Generated Parquet conversion script: ${scriptPath}`)
  console.log(`Run: duckdb < ${scriptPath}`)
}

/**
 * Convert SQLite database to JSONL
 */
async function convertSQLiteToJSONL(
  inputPath: string,
  outputPath: string,
  opts: ToJSONLOptions
): Promise<void> {
  // Generate SQLite command for conversion
  const outputDir = path.dirname(outputPath)
  const scriptPath = path.join(outputDir, 'convert_sqlite.sh')

  // Get table names if not specified
  const tablesClause = opts.tables
    ? opts.tables.map((t) => `'${t}'`).join(', ')
    : `(SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%')`

  const script = `#!/bin/bash
# Convert SQLite to JSONL
# Run: bash ${scriptPath}

DB="${inputPath}"
OUTPUT_DIR="${outputDir}"

# Get table names
TABLES=$(sqlite3 "$DB" "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")

for TABLE in $TABLES; do
  echo "Converting table: $TABLE"

  # Get column names
  COLUMNS=$(sqlite3 -separator ', ' "$DB" "PRAGMA table_info($TABLE);" | cut -d'|' -f2 | tr '\\n' ',' | sed 's/,$//')

  # Export as JSON
  sqlite3 -json "$DB" "SELECT * FROM $TABLE;" | \\
    node -e "const d=JSON.parse(require('fs').readFileSync('/dev/stdin','utf8')); d.forEach(r=>console.log(JSON.stringify(r)))" \\
    > "$OUTPUT_DIR/$TABLE.jsonl"

  echo "Created: $OUTPUT_DIR/$TABLE.jsonl"
done

echo "Conversion complete!"
`

  fs.writeFileSync(scriptPath, script, { mode: 0o755 })
  console.log(`Generated SQLite conversion script: ${scriptPath}`)
  console.log(`Run: bash ${scriptPath}`)
}

/**
 * Parse a delimited line handling quoted values
 */
function parseDelimitedLine(line: string, delimiter: string): string[] {
  const result: string[] = []
  let current = ''
  let inQuotes = false

  for (let i = 0; i < line.length; i++) {
    const char = line[i]

    if (char === '"' && !inQuotes) {
      inQuotes = true
    } else if (char === '"' && inQuotes) {
      if (line[i + 1] === '"') {
        current += '"'
        i++
      } else {
        inQuotes = false
      }
    } else if (char === delimiter && !inQuotes) {
      result.push(current)
      current = ''
    } else {
      current += char
    }
  }

  result.push(current)
  return result
}

/**
 * Create a record object from headers and values
 */
function createRecord(
  headers: string[],
  values: string[],
  opts: ToJSONLOptions
): Record<string, unknown> | null {
  const record: Record<string, unknown> = {}

  for (let i = 0; i < headers.length; i++) {
    const header = headers[i]
    const value = values[i]

    // Skip null values represented as \N
    if (value === '\\N' || value === undefined) {
      record[header] = null
      continue
    }

    // Type coercion based on value
    record[header] = coerceValue(value)
  }

  return transformRecord(record, opts)
}

/**
 * Coerce string value to appropriate type
 */
function coerceValue(value: string): unknown {
  // Empty string -> null
  if (value === '') return null

  // Boolean
  if (value.toLowerCase() === 'true') return true
  if (value.toLowerCase() === 'false') return false

  // Integer
  if (/^-?\d+$/.test(value)) {
    const num = parseInt(value, 10)
    // Check for safe integer range
    if (num >= Number.MIN_SAFE_INTEGER && num <= Number.MAX_SAFE_INTEGER) {
      return num
    }
    // Return as string for BigInt values
    return value
  }

  // Float
  if (/^-?\d+\.?\d*(?:[eE][+-]?\d+)?$/.test(value)) {
    return parseFloat(value)
  }

  // JSON object or array
  if ((value.startsWith('{') && value.endsWith('}')) ||
      (value.startsWith('[') && value.endsWith(']'))) {
    try {
      return JSON.parse(value)
    } catch {
      // Not valid JSON, return as string
    }
  }

  // Default: string
  return value
}

/**
 * Transform record with field mapping and MongoDB compatibility
 */
function transformRecord(
  record: Record<string, unknown>,
  opts: ToJSONLOptions
): Record<string, unknown> {
  const result: Record<string, unknown> = {}

  for (const [key, value] of Object.entries(record)) {
    // Apply field transforms
    const newKey = opts.fieldTransforms?.[key] || key
    result[newKey] = value
  }

  // MongoDB compatibility: ensure _id field
  if (opts.mongoCompatible) {
    if (!result._id) {
      // Use existing id field or generate one
      if (result.id !== undefined) {
        result._id = result.id
      } else if (result.uuid !== undefined) {
        result._id = result.uuid
      }
      // If still no _id, leave it to MongoDB to generate
    }
  }

  return result
}

/**
 * Generate JSONL file from records in memory
 */
export function recordsToJSONL(
  records: Record<string, unknown>[],
  opts?: Partial<ToJSONLOptions>
): string {
  const options: ToJSONLOptions = {
    format: 'jsonl',
    prettyPrint: false,
    mongoCompatible: true,
    ...opts,
  }

  return records
    .map((record) => {
      const transformed = transformRecord(record, options)
      return options.prettyPrint
        ? JSON.stringify(transformed, null, 2)
        : JSON.stringify(transformed)
    })
    .join('\n')
}

/**
 * Parse JSONL string to records array
 */
export function parseJSONL(content: string): Record<string, unknown>[] {
  return content
    .split('\n')
    .filter((line) => line.trim())
    .map((line) => JSON.parse(line))
}

export default convert
