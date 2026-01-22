/**
 * Convert datasets to Parquet format
 *
 * Converts various input formats (CSV, TSV, JSON, JSONL, SQLite) to Parquet.
 * Suitable for DuckDB analytics and columnar data lakes.
 *
 * Features:
 * - Columnar storage with compression
 * - Schema preservation and type inference
 * - Partitioning support for large datasets
 * - Integration with DuckDB for conversion
 *
 * @module scripts/convert/to-parquet
 *
 * Note: This script is designed to run in Node.js CLI environment.
 * Use with: npx tsx scripts/convert/to-parquet.ts
 */

// Node.js imports - these scripts run in Node.js CLI, not Workers
// @ts-ignore - Node.js types not available in Workers project
import * as fs from 'fs'
// @ts-ignore
import * as path from 'path'
// @ts-ignore
import { createReadStream } from 'fs'
// @ts-ignore
import { createInterface } from 'readline'

/**
 * Supported input file formats
 */
type InputFormat = 'csv' | 'tsv' | 'json' | 'jsonl' | 'sqlite'

/**
 * Parquet compression codecs
 */
type CompressionCodec = 'snappy' | 'gzip' | 'zstd' | 'lz4' | 'uncompressed'

/**
 * Conversion options for Parquet
 */
export interface ToParquetOptions {
  format: 'parquet'
  tables?: string[]
  /** Compression codec (default: zstd) */
  compression?: CompressionCodec
  /** Row group size (default: 100000) */
  rowGroupSize?: number
  /** Enable partitioning */
  partitionBy?: string[]
  /** Schema definition for explicit types */
  schema?: SchemaDefinition
  /** Input file delimiter for CSV/TSV (default: auto-detect) */
  delimiter?: string
  /** DuckDB path (default: duckdb in PATH) */
  duckdbPath?: string
  /** Use DuckDB memory mode for small files (default: true for < 100MB) */
  memoryMode?: boolean
}

/**
 * Schema definition for explicit column types
 */
export interface SchemaDefinition {
  tableName: string
  columns: Array<{
    name: string
    type: ParquetType
    nullable?: boolean
  }>
}

/**
 * Parquet logical types
 */
type ParquetType =
  | 'BOOLEAN'
  | 'INT32'
  | 'INT64'
  | 'FLOAT'
  | 'DOUBLE'
  | 'BYTE_ARRAY'
  | 'VARCHAR'
  | 'DATE'
  | 'TIMESTAMP'
  | 'TIME'
  | 'DECIMAL'
  | 'UUID'
  | 'JSON'
  | 'STRUCT'
  | 'LIST'
  | 'MAP'

/**
 * Convert a dataset to Parquet format
 *
 * @param inputPath - Path to input file or directory
 * @param outputPath - Path to output .parquet file or directory
 * @param options - Conversion options
 */
export async function convert(
  inputPath: string,
  outputPath: string,
  options?: ToParquetOptions
): Promise<void> {
  const opts: ToParquetOptions = {
    format: 'parquet',
    compression: 'zstd',
    rowGroupSize: 100000,
    memoryMode: true,
    ...options,
  }

  // Ensure output directory exists
  const outputDir = path.dirname(outputPath)
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true })
  }

  // Determine input format
  const inputFormat = detectInputFormat(inputPath)

  // Generate DuckDB conversion script
  const script = await generateConversionScript(inputPath, outputPath, inputFormat, opts)

  // Write script file
  const scriptPath = outputPath.replace('.parquet', '_convert.sql')
  fs.writeFileSync(scriptPath, script)

  console.log(`Generated Parquet conversion script: ${scriptPath}`)
  console.log(`Execute with: ${opts.duckdbPath || 'duckdb'} < ${scriptPath}`)

  // Also write a shell script for convenience
  const shellScript = generateShellScript(scriptPath, outputPath, opts)
  const shellPath = outputPath.replace('.parquet', '_convert.sh')
  fs.writeFileSync(shellPath, shellScript, { mode: 0o755 })
  console.log(`Execute with: bash ${shellPath}`)
}

/**
 * Detect input file format from extension
 */
function detectInputFormat(inputPath: string): InputFormat {
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
    case '.db':
    case '.sqlite':
    case '.sqlite3':
      return 'sqlite'
    default:
      // Try to detect from content
      if (fs.existsSync(inputPath)) {
        const sample = fs.readFileSync(inputPath, { encoding: 'utf8', flag: 'r' }).slice(0, 1000)
        if (sample.includes('\t')) return 'tsv'
        if (sample.startsWith('[') || sample.startsWith('{')) return 'json'
      }
      return 'csv'
  }
}

/**
 * Generate DuckDB SQL script for conversion
 */
async function generateConversionScript(
  inputPath: string,
  outputPath: string,
  format: InputFormat,
  opts: ToParquetOptions
): Promise<string> {
  const lines: string[] = []
  const tableName = opts.schema?.tableName || path.basename(inputPath, path.extname(inputPath))

  lines.push('-- DuckDB Parquet conversion script')
  lines.push(`-- Generated for: ${inputPath}`)
  lines.push(`-- Output: ${outputPath}`)
  lines.push('')

  // Memory settings for large files
  if (!opts.memoryMode) {
    lines.push("-- Set memory limit for large files")
    lines.push("SET memory_limit='4GB';")
    lines.push("SET threads=4;")
    lines.push('')
  }

  // Generate read function based on input format
  const readFunction = generateReadFunction(inputPath, format, opts)

  // If schema is provided, create explicit types
  if (opts.schema) {
    lines.push('-- Create table with explicit schema')
    lines.push(generateCreateTableSQL(opts.schema))
    lines.push('')
    lines.push(`INSERT INTO ${tableName}`)
    lines.push(readFunction + ';')
  }

  // Generate COPY statement for output
  lines.push('')
  lines.push('-- Convert to Parquet')

  if (opts.partitionBy && opts.partitionBy.length > 0) {
    // Partitioned output
    lines.push(`COPY (`)
    if (opts.schema) {
      lines.push(`  SELECT * FROM ${tableName}`)
    } else {
      lines.push(`  ${readFunction}`)
    }
    lines.push(`) TO '${outputPath}' (`)
    lines.push(`  FORMAT PARQUET,`)
    lines.push(`  COMPRESSION '${opts.compression}',`)
    lines.push(`  ROW_GROUP_SIZE ${opts.rowGroupSize},`)
    lines.push(`  PARTITION_BY (${opts.partitionBy.join(', ')})`)
    lines.push(`);`)
  } else {
    // Single file output
    lines.push(`COPY (`)
    if (opts.schema) {
      lines.push(`  SELECT * FROM ${tableName}`)
    } else {
      lines.push(`  ${readFunction}`)
    }
    lines.push(`) TO '${outputPath}' (`)
    lines.push(`  FORMAT PARQUET,`)
    lines.push(`  COMPRESSION '${opts.compression}',`)
    lines.push(`  ROW_GROUP_SIZE ${opts.rowGroupSize}`)
    lines.push(`);`)
  }

  // Verification query
  lines.push('')
  lines.push('-- Verify output')
  lines.push(`SELECT COUNT(*) as row_count FROM read_parquet('${outputPath}${opts.partitionBy ? '/**/*.parquet' : ''}');`)

  return lines.join('\n')
}

/**
 * Generate DuckDB read function for input format
 */
function generateReadFunction(
  inputPath: string,
  format: InputFormat,
  opts: ToParquetOptions
): string {
  switch (format) {
    case 'csv':
      return `SELECT * FROM read_csv('${inputPath}', auto_detect=true, header=true)`

    case 'tsv':
      return `SELECT * FROM read_csv('${inputPath}', auto_detect=true, header=true, delim='\\t', nullstr='\\\\N')`

    case 'json':
      return `SELECT * FROM read_json('${inputPath}', auto_detect=true, format='array')`

    case 'jsonl':
      return `SELECT * FROM read_json('${inputPath}', auto_detect=true, format='newline_delimited')`

    case 'sqlite':
      // For SQLite, we need to install and load the extension
      return `-- First run: INSTALL sqlite; LOAD sqlite;
ATTACH '${inputPath}' AS source_db (TYPE sqlite);
SELECT * FROM source_db.${opts.schema?.tableName || 'main_table'}`

    default:
      return `SELECT * FROM '${inputPath}'`
  }
}

/**
 * Generate CREATE TABLE SQL from schema definition
 */
function generateCreateTableSQL(schema: SchemaDefinition): string {
  const columnDefs = schema.columns.map((col) => {
    const nullable = col.nullable === false ? ' NOT NULL' : ''
    return `  ${col.name} ${mapTypeToDuckDB(col.type)}${nullable}`
  })

  return `CREATE TABLE ${schema.tableName} (\n${columnDefs.join(',\n')}\n);`
}

/**
 * Map Parquet types to DuckDB types
 */
function mapTypeToDuckDB(type: ParquetType): string {
  switch (type) {
    case 'BOOLEAN':
      return 'BOOLEAN'
    case 'INT32':
      return 'INTEGER'
    case 'INT64':
      return 'BIGINT'
    case 'FLOAT':
      return 'FLOAT'
    case 'DOUBLE':
      return 'DOUBLE'
    case 'BYTE_ARRAY':
      return 'BLOB'
    case 'VARCHAR':
      return 'VARCHAR'
    case 'DATE':
      return 'DATE'
    case 'TIMESTAMP':
      return 'TIMESTAMP'
    case 'TIME':
      return 'TIME'
    case 'DECIMAL':
      return 'DECIMAL(18,2)'
    case 'UUID':
      return 'UUID'
    case 'JSON':
      return 'JSON'
    case 'STRUCT':
      return 'STRUCT()'
    case 'LIST':
      return 'LIST(VARCHAR)'
    case 'MAP':
      return 'MAP(VARCHAR, VARCHAR)'
    default:
      return 'VARCHAR'
  }
}

/**
 * Generate shell script for running conversion
 */
function generateShellScript(
  sqlScriptPath: string,
  outputPath: string,
  opts: ToParquetOptions
): string {
  const duckdb = opts.duckdbPath || 'duckdb'

  return `#!/bin/bash
# Parquet conversion script
# Generated for: ${outputPath}

set -e

# Check if duckdb is installed
if ! command -v ${duckdb} &> /dev/null; then
    echo "Error: duckdb is not installed or not in PATH"
    echo "Install with: brew install duckdb (macOS) or see https://duckdb.org/docs/installation"
    exit 1
fi

echo "Converting to Parquet..."
${duckdb} < "${sqlScriptPath}"

if [ -f "${outputPath}" ] || [ -d "${outputPath}" ]; then
    echo "Success! Output: ${outputPath}"

    # Show file info
    if [ -f "${outputPath}" ]; then
        ls -lh "${outputPath}"
    else
        echo "Partitioned output directory:"
        find "${outputPath}" -name "*.parquet" | head -10
        echo "Total files: $(find "${outputPath}" -name "*.parquet" | wc -l)"
    fi

    # Show schema
    echo ""
    echo "Schema:"
    ${duckdb} -c "DESCRIBE SELECT * FROM read_parquet('${outputPath}${opts.partitionBy ? '/**/*.parquet' : ''}');"
else
    echo "Error: Output file not created"
    exit 1
fi
`
}

/**
 * Infer Parquet schema from sample data
 */
export async function inferSchema(
  inputPath: string,
  format: InputFormat,
  sampleSize: number = 100
): Promise<SchemaDefinition> {
  const tableName = path.basename(inputPath, path.extname(inputPath))
  const columns: SchemaDefinition['columns'] = []

  if (format === 'csv' || format === 'tsv') {
    const { headers, sampleRows } = await readDelimitedSample(
      inputPath,
      format === 'tsv' ? '\t' : ',',
      sampleSize
    )

    for (let i = 0; i < headers.length; i++) {
      const values = sampleRows.map((row) => row[i])
      columns.push({
        name: sanitizeColumnName(headers[i]),
        type: inferParquetType(values),
        nullable: values.some((v) => v === null || v === '' || v === '\\N'),
      })
    }
  } else if (format === 'json' || format === 'jsonl') {
    const sampleData = await readJSONSample(inputPath, format, sampleSize)

    if (sampleData.length > 0) {
      const allKeys = new Set<string>()
      for (const record of sampleData) {
        for (const key of Object.keys(record)) {
          allKeys.add(key)
        }
      }

      for (const key of allKeys) {
        const values = sampleData.map((r) => r[key])
        columns.push({
          name: sanitizeColumnName(key),
          type: inferParquetType(values),
          nullable: values.some((v) => v === null || v === undefined),
        })
      }
    }
  }

  return { tableName, columns }
}

/**
 * Infer Parquet type from sample values
 */
function inferParquetType(values: unknown[]): ParquetType {
  let hasNull = false
  let allBoolean = true
  let allInteger = true
  let allNumber = true
  let allDate = true
  let allTimestamp = true
  let allUUID = true

  for (const val of values) {
    if (val === null || val === undefined || val === '' || val === '\\N') {
      hasNull = true
      continue
    }

    const strVal = String(val)

    // Check boolean
    if (allBoolean && !['true', 'false', '0', '1'].includes(strVal.toLowerCase())) {
      allBoolean = false
    }

    // Check integer
    if (allInteger && !/^-?\d+$/.test(strVal)) {
      allInteger = false
    }

    // Check number
    if (allNumber && !/^-?\d+\.?\d*(?:[eE][+-]?\d+)?$/.test(strVal)) {
      allNumber = false
    }

    // Check date (YYYY-MM-DD)
    if (allDate && !/^\d{4}-\d{2}-\d{2}$/.test(strVal)) {
      allDate = false
    }

    // Check timestamp
    if (allTimestamp && !/^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}/.test(strVal)) {
      allTimestamp = false
    }

    // Check UUID
    if (allUUID && !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(strVal)) {
      allUUID = false
    }

    // Check object/array
    if (typeof val === 'object' && val !== null) {
      if (Array.isArray(val)) return 'LIST'
      return 'JSON'
    }
  }

  if (allBoolean && !allInteger) return 'BOOLEAN'
  if (allUUID) return 'UUID'
  if (allDate) return 'DATE'
  if (allTimestamp) return 'TIMESTAMP'
  if (allInteger) {
    // Check range for INT32 vs INT64
    const nums = values
      .filter((v) => v !== null && v !== undefined && v !== '')
      .map((v) => parseInt(String(v), 10))
    const max = Math.max(...nums)
    const min = Math.min(...nums)
    if (min >= -2147483648 && max <= 2147483647) return 'INT32'
    return 'INT64'
  }
  if (allNumber) return 'DOUBLE'

  return 'VARCHAR'
}

/**
 * Read sample rows from delimited file
 */
async function readDelimitedSample(
  filePath: string,
  delimiter: string,
  sampleSize: number
): Promise<{ headers: string[]; sampleRows: (string | null)[][] }> {
  return new Promise((resolve, reject) => {
    const headers: string[] = []
    const sampleRows: (string | null)[][] = []
    let lineCount = 0

    const rl = createInterface({
      input: createReadStream(filePath),
      crlfDelay: Infinity,
    })

    rl.on('line', (line: string) => {
      const values = parseDelimitedLine(line, delimiter)

      if (lineCount === 0) {
        headers.push(...values)
      } else if (lineCount <= sampleSize) {
        sampleRows.push(values.map((v) => (v === '\\N' || v === '' ? null : v)))
      } else {
        rl.close()
      }

      lineCount++
    })

    rl.on('close', () => {
      resolve({ headers, sampleRows })
    })

    rl.on('error', reject)
  })
}

/**
 * Read sample data from JSON/JSONL file
 */
async function readJSONSample(
  filePath: string,
  format: 'json' | 'jsonl',
  sampleSize: number
): Promise<Record<string, unknown>[]> {
  const content = fs.readFileSync(filePath, 'utf8')

  if (format === 'json') {
    const data = JSON.parse(content)
    if (Array.isArray(data)) {
      return data.slice(0, sampleSize)
    }
    return [data]
  }

  // JSONL format
  const lines = content.split('\n').filter((line: string) => line.trim())
  return lines.slice(0, sampleSize).map((line: string) => JSON.parse(line))
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
 * Sanitize column name for SQL
 */
function sanitizeColumnName(name: string): string {
  return name
    .replace(/[^\w\s]/g, '_')
    .replace(/\s+/g, '_')
    .replace(/^(\d)/, '_$1')
    .toLowerCase()
}

/**
 * Generate DuckDB query to read Parquet file(s)
 */
export function generateReadQuery(
  parquetPath: string,
  options?: {
    select?: string[]
    where?: string
    limit?: number
    orderBy?: string
  }
): string {
  const select = options?.select?.join(', ') || '*'
  const globPattern = parquetPath.includes('*') ? parquetPath : parquetPath

  let query = `SELECT ${select}\nFROM read_parquet('${globPattern}')`

  if (options?.where) {
    query += `\nWHERE ${options.where}`
  }

  if (options?.orderBy) {
    query += `\nORDER BY ${options.orderBy}`
  }

  if (options?.limit) {
    query += `\nLIMIT ${options.limit}`
  }

  return query
}

export default convert
