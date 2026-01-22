/**
 * Convert datasets to SQLite format
 *
 * Converts various input formats (CSV, TSV, JSON, JSONL, Parquet) to SQLite databases.
 * Suitable for sqlite/postgres WASM environments.
 *
 * @module scripts/convert/to-sqlite
 *
 * Note: This script is designed to run in Node.js CLI environment.
 * Use with: npx tsx scripts/convert/to-sqlite.ts
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
type InputFormat = 'csv' | 'tsv' | 'json' | 'jsonl' | 'parquet'

/**
 * Column type inference result
 */
interface InferredColumn {
  name: string
  type: 'INTEGER' | 'REAL' | 'TEXT' | 'BLOB'
  nullable: boolean
}

/**
 * Conversion options for SQLite
 */
export interface ToSQLiteOptions {
  format: 'sqlite'
  tables?: string[]
  /** Infer types from data (default: true) */
  inferTypes?: boolean
  /** Create indexes on primary key columns (default: true) */
  createIndexes?: boolean
  /** Batch size for inserts (default: 1000) */
  batchSize?: number
  /** Use WAL mode for better performance (default: true) */
  walMode?: boolean
  /** Input file delimiter for CSV/TSV (default: auto-detect) */
  delimiter?: string
  /** Schema definition to use instead of inferring */
  schema?: SchemaDefinition
}

/**
 * Schema definition for explicit table structure
 */
export interface SchemaDefinition {
  tableName: string
  columns: Array<{
    name: string
    type: string
    primaryKey?: boolean
    unique?: boolean
    nullable?: boolean
    default?: unknown
  }>
  indexes?: Array<{
    name: string
    columns: string[]
    unique?: boolean
  }>
}

/**
 * Convert a dataset to SQLite format
 *
 * @param inputPath - Path to input file or directory
 * @param outputPath - Path to output .db file
 * @param options - Conversion options
 */
export async function convert(
  inputPath: string,
  outputPath: string,
  options?: ToSQLiteOptions
): Promise<void> {
  const opts: ToSQLiteOptions = {
    format: 'sqlite',
    inferTypes: true,
    createIndexes: true,
    batchSize: 1000,
    walMode: true,
    ...options,
  }

  // Ensure output directory exists
  const outputDir = path.dirname(outputPath)
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true })
  }

  // Determine input format
  const inputFormat = detectInputFormat(inputPath)

  // Generate SQL statements
  const sqlStatements: string[] = []

  // Initialize database
  if (opts.walMode) {
    sqlStatements.push('PRAGMA journal_mode=WAL;')
  }
  sqlStatements.push('PRAGMA synchronous=NORMAL;')
  sqlStatements.push('PRAGMA cache_size=10000;')

  // Process based on input format
  if (inputFormat === 'parquet') {
    // For Parquet, generate DuckDB-based conversion script
    const statements = await convertParquetToSQLite(inputPath, opts)
    sqlStatements.push(...statements)
  } else if (inputFormat === 'jsonl' || inputFormat === 'json') {
    const statements = await convertJSONToSQLite(inputPath, inputFormat, opts)
    sqlStatements.push(...statements)
  } else {
    // CSV/TSV
    const statements = await convertDelimitedToSQLite(inputPath, inputFormat, opts)
    sqlStatements.push(...statements)
  }

  // Write SQL script that can be executed with sqlite3
  const scriptPath = outputPath.replace('.db', '.sql')
  fs.writeFileSync(scriptPath, sqlStatements.join('\n\n'))

  console.log(`Generated SQLite conversion script: ${scriptPath}`)
  console.log(`Execute with: sqlite3 ${outputPath} < ${scriptPath}`)
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
    case '.parquet':
      return 'parquet'
    default:
      // Try to detect from content
      const sample = fs.readFileSync(inputPath, { encoding: 'utf8', flag: 'r' }).slice(0, 1000)
      if (sample.includes('\t')) return 'tsv'
      if (sample.startsWith('[') || sample.startsWith('{')) return 'json'
      return 'csv'
  }
}

/**
 * Infer column types from sample data
 */
function inferColumnTypes(values: unknown[]): 'INTEGER' | 'REAL' | 'TEXT' | 'BLOB' {
  let hasNull = false
  let isInteger = true
  let isReal = true

  for (const val of values) {
    if (val === null || val === undefined || val === '' || val === '\\N') {
      hasNull = true
      continue
    }

    const strVal = String(val)

    // Check if integer
    if (isInteger && !/^-?\d+$/.test(strVal)) {
      isInteger = false
    }

    // Check if real number
    if (isReal && !/^-?\d+\.?\d*(?:[eE][+-]?\d+)?$/.test(strVal)) {
      isReal = false
    }

    if (!isInteger && !isReal) break
  }

  if (isInteger) return 'INTEGER'
  if (isReal) return 'REAL'
  return 'TEXT'
}

/**
 * Convert Parquet file to SQLite using DuckDB
 */
async function convertParquetToSQLite(
  inputPath: string,
  opts: ToSQLiteOptions
): Promise<string[]> {
  const tableName = opts.schema?.tableName || path.basename(inputPath, '.parquet')
  const statements: string[] = []

  statements.push('-- Parquet to SQLite conversion')
  statements.push('-- Requires DuckDB CLI for initial conversion:')
  statements.push(`-- duckdb -c "INSTALL sqlite; LOAD sqlite;"`)
  statements.push(
    `-- duckdb -c "ATTACH '${inputPath.replace('.parquet', '.db')}' AS sqlite_db (TYPE sqlite);"`
  )
  statements.push(
    `-- duckdb -c "CREATE TABLE sqlite_db.${tableName} AS SELECT * FROM read_parquet('${inputPath}');"`
  )
  statements.push('')
  statements.push('-- Or use the following DuckDB script:')
  statements.push(`-- INSTALL sqlite;`)
  statements.push(`-- LOAD sqlite;`)
  statements.push(`-- ATTACH '${inputPath.replace('.parquet', '.db')}' AS sqlite_db (TYPE sqlite);`)
  statements.push(`-- CREATE TABLE sqlite_db.${tableName} AS SELECT * FROM read_parquet('${inputPath}');`)

  // If schema is provided, create explicit table structure
  if (opts.schema) {
    statements.push('')
    statements.push('-- Explicit table structure:')
    statements.push(generateCreateTableSQL(opts.schema))

    if (opts.createIndexes && opts.schema.indexes) {
      for (const idx of opts.schema.indexes) {
        const unique = idx.unique ? 'UNIQUE ' : ''
        statements.push(`CREATE ${unique}INDEX ${idx.name} ON ${opts.schema.tableName}(${idx.columns.join(', ')});`)
      }
    }
  }

  return statements
}

/**
 * Convert JSON/JSONL to SQLite
 */
async function convertJSONToSQLite(
  inputPath: string,
  format: 'json' | 'jsonl',
  opts: ToSQLiteOptions
): Promise<string[]> {
  const tableName = opts.schema?.tableName || path.basename(inputPath, `.${format}`)
  const statements: string[] = []

  // Read sample data to infer schema
  const sampleData = await readJSONSample(inputPath, format, 100)

  if (sampleData.length === 0) {
    throw new Error('No data found in input file')
  }

  // Infer column structure from first record
  const columns = inferColumnsFromJSON(sampleData)

  // Generate CREATE TABLE statement
  if (opts.schema) {
    statements.push(generateCreateTableSQL(opts.schema))
  } else {
    statements.push(
      `CREATE TABLE IF NOT EXISTS ${tableName} (${columns.map((c) => `${c.name} ${c.type}${c.nullable ? '' : ' NOT NULL'}`).join(', ')});`
    )
  }

  // Generate INSERT statements
  statements.push('')
  statements.push('BEGIN TRANSACTION;')

  const batchSize = opts.batchSize || 1000
  const columnNames = columns.map((c) => c.name)

  // For actual conversion, we'd stream through the file
  // Here we generate the pattern for insertion
  statements.push(`-- Insert data using prepared statements for better performance`)
  statements.push(`-- Values should be escaped properly before insertion`)
  statements.push('')

  // Generate sample INSERT for documentation
  if (sampleData.length > 0) {
    const sample = sampleData[0]
    const values = columnNames.map((col) => {
      const val = sample[col]
      if (val === null || val === undefined) return 'NULL'
      if (typeof val === 'string') return `'${escapeSQLString(val)}'`
      if (typeof val === 'object') return `'${escapeSQLString(JSON.stringify(val))}'`
      return String(val)
    })
    statements.push(`-- Example INSERT:`)
    statements.push(`-- INSERT INTO ${tableName} (${columnNames.join(', ')}) VALUES (${values.join(', ')});`)
  }

  statements.push('')
  statements.push(`-- Use the following pattern for bulk insert:`)
  statements.push(`-- INSERT INTO ${tableName} (${columnNames.join(', ')}) VALUES`)
  statements.push(`--   (val1, val2, ...),`)
  statements.push(`--   (val1, val2, ...);`)

  statements.push('')
  statements.push('COMMIT;')

  // Generate indexes
  if (opts.createIndexes && opts.schema?.indexes) {
    statements.push('')
    for (const idx of opts.schema.indexes) {
      const unique = idx.unique ? 'UNIQUE ' : ''
      statements.push(`CREATE ${unique}INDEX ${idx.name} ON ${tableName}(${idx.columns.join(', ')});`)
    }
  }

  return statements
}

/**
 * Convert delimited (CSV/TSV) file to SQLite
 */
async function convertDelimitedToSQLite(
  inputPath: string,
  format: 'csv' | 'tsv',
  opts: ToSQLiteOptions
): Promise<string[]> {
  const delimiter = opts.delimiter || (format === 'tsv' ? '\t' : ',')
  const tableName = opts.schema?.tableName || path.basename(inputPath, `.${format}`)
  const statements: string[] = []

  // Read header and sample rows
  const { headers, sampleRows } = await readDelimitedSample(inputPath, delimiter, 100)

  if (headers.length === 0) {
    throw new Error('No headers found in input file')
  }

  // Infer column types
  const columns: InferredColumn[] = opts.inferTypes
    ? headers.map((name, idx) => {
        const values = sampleRows.map((row) => row[idx])
        return {
          name: sanitizeColumnName(name),
          type: inferColumnTypes(values),
          nullable: values.some((v) => v === null || v === '' || v === '\\N'),
        }
      })
    : headers.map((name) => ({
        name: sanitizeColumnName(name),
        type: 'TEXT' as const,
        nullable: true,
      }))

  // Generate CREATE TABLE
  if (opts.schema) {
    statements.push(generateCreateTableSQL(opts.schema))
  } else {
    const columnDefs = columns.map(
      (c) => `${c.name} ${c.type}${c.nullable ? '' : ' NOT NULL'}`
    )
    statements.push(`CREATE TABLE IF NOT EXISTS ${tableName} (${columnDefs.join(', ')});`)
  }

  // SQLite native import
  statements.push('')
  statements.push('-- SQLite native CSV import:')
  statements.push(`.mode ${format === 'tsv' ? 'tabs' : 'csv'}`)
  statements.push(`.import --skip 1 ${inputPath} ${tableName}`)
  statements.push('')

  // Alternative: explicit INSERT statements
  statements.push('-- Or use explicit INSERT statements:')
  statements.push('BEGIN TRANSACTION;')

  const columnNames = columns.map((c) => c.name)
  statements.push(`-- INSERT INTO ${tableName} (${columnNames.join(', ')}) VALUES (...);`)

  statements.push('COMMIT;')

  // Generate indexes
  if (opts.createIndexes && opts.schema?.indexes) {
    statements.push('')
    for (const idx of opts.schema.indexes) {
      const unique = idx.unique ? 'UNIQUE ' : ''
      statements.push(`CREATE ${unique}INDEX ${idx.name} ON ${tableName}(${idx.columns.join(', ')});`)
    }
  }

  return statements
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
 * Infer columns from JSON sample data
 */
function inferColumnsFromJSON(data: Record<string, unknown>[]): InferredColumn[] {
  const columnMap = new Map<string, { values: unknown[]; nullable: boolean }>()

  for (const record of data) {
    for (const [key, value] of Object.entries(record)) {
      if (!columnMap.has(key)) {
        columnMap.set(key, { values: [], nullable: false })
      }
      const col = columnMap.get(key)!
      col.values.push(value)
      if (value === null || value === undefined) {
        col.nullable = true
      }
    }
  }

  return Array.from(columnMap.entries()).map(([name, info]) => ({
    name: sanitizeColumnName(name),
    type: inferColumnTypes(info.values),
    nullable: info.nullable,
  }))
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
  // Remove or replace invalid characters
  let sanitized = name
    .replace(/[^\w\s]/g, '_')
    .replace(/\s+/g, '_')
    .replace(/^(\d)/, '_$1')
    .toLowerCase()

  // SQLite reserved words
  const reserved = new Set([
    'abort', 'action', 'add', 'after', 'all', 'alter', 'analyze', 'and', 'as',
    'asc', 'attach', 'autoincrement', 'before', 'begin', 'between', 'by',
    'cascade', 'case', 'cast', 'check', 'collate', 'column', 'commit',
    'conflict', 'constraint', 'create', 'cross', 'current_date', 'current_time',
    'current_timestamp', 'database', 'default', 'deferrable', 'deferred',
    'delete', 'desc', 'detach', 'distinct', 'drop', 'each', 'else', 'end',
    'escape', 'except', 'exclusive', 'exists', 'explain', 'fail', 'for',
    'foreign', 'from', 'full', 'glob', 'group', 'having', 'if', 'ignore',
    'immediate', 'in', 'index', 'indexed', 'initially', 'inner', 'insert',
    'instead', 'intersect', 'into', 'is', 'isnull', 'join', 'key', 'left',
    'like', 'limit', 'match', 'natural', 'no', 'not', 'notnull', 'null', 'of',
    'offset', 'on', 'or', 'order', 'outer', 'plan', 'pragma', 'primary',
    'query', 'raise', 'recursive', 'references', 'regexp', 'reindex',
    'release', 'rename', 'replace', 'restrict', 'right', 'rollback', 'row',
    'savepoint', 'select', 'set', 'table', 'temp', 'temporary', 'then', 'to',
    'transaction', 'trigger', 'union', 'unique', 'update', 'using', 'vacuum',
    'values', 'view', 'virtual', 'when', 'where', 'with', 'without',
  ])

  if (reserved.has(sanitized)) {
    sanitized = `_${sanitized}`
  }

  return sanitized
}

/**
 * Escape string for SQL
 */
function escapeSQLString(str: string): string {
  return str.replace(/'/g, "''")
}

/**
 * Generate CREATE TABLE SQL from schema definition
 */
function generateCreateTableSQL(schema: SchemaDefinition): string {
  const columnDefs = schema.columns.map((col) => {
    const parts = [col.name, col.type]
    if (col.primaryKey) parts.push('PRIMARY KEY')
    if (!col.nullable && !col.primaryKey) parts.push('NOT NULL')
    if (col.unique && !col.primaryKey) parts.push('UNIQUE')
    if (col.default !== undefined) {
      parts.push(`DEFAULT ${typeof col.default === 'string' ? `'${col.default}'` : col.default}`)
    }
    return parts.join(' ')
  })

  return `CREATE TABLE IF NOT EXISTS ${schema.tableName} (\n  ${columnDefs.join(',\n  ')}\n);`
}

export default convert
