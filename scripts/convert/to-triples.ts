/**
 * Convert datasets with relationships to N-Triples format
 *
 * Converts relational datasets to RDF N-Triples (.nt) format.
 * Suitable for GraphDB/SDB and semantic web applications.
 *
 * Features:
 * - Converts tables to RDF entities
 * - Generates relationship triples from foreign keys
 * - Supports custom vocabulary/ontology prefixes
 * - Handles data types with XSD type annotations
 * - Streaming conversion for large datasets
 *
 * @module scripts/convert/to-triples
 *
 * Note: This script is designed to run in Node.js CLI environment.
 * Use with: npx tsx scripts/convert/to-triples.ts
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

/**
 * Supported input file formats
 */
type InputFormat = 'csv' | 'tsv' | 'json' | 'jsonl' | 'sqlite'

/**
 * Standard RDF/XSD namespaces
 */
const NAMESPACES = {
  rdf: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
  rdfs: 'http://www.w3.org/2000/01/rdf-schema#',
  xsd: 'http://www.w3.org/2001/XMLSchema#',
  owl: 'http://www.w3.org/2002/07/owl#',
  foaf: 'http://xmlns.com/foaf/0.1/',
  dc: 'http://purl.org/dc/elements/1.1/',
  dct: 'http://purl.org/dc/terms/',
  schema: 'http://schema.org/',
} as const

/**
 * XSD data types for typed literals
 */
type XSDType =
  | 'xsd:string'
  | 'xsd:integer'
  | 'xsd:long'
  | 'xsd:decimal'
  | 'xsd:float'
  | 'xsd:double'
  | 'xsd:boolean'
  | 'xsd:date'
  | 'xsd:dateTime'
  | 'xsd:time'
  | 'xsd:anyURI'

/**
 * Conversion options for N-Triples
 */
export interface ToTriplesOptions {
  format: 'triples'
  tables?: string[]
  /** Base URI for entities (default: http://example.org/) */
  baseUri?: string
  /** Vocabulary namespace (default: http://example.org/vocab/) */
  vocabUri?: string
  /** Primary key column name (default: 'id') */
  primaryKey?: string
  /** Relationship definitions */
  relationships?: RelationshipConfig[]
  /** Column to predicate mappings */
  predicateMappings?: Record<string, string>
  /** Column type hints for proper XSD typing */
  typeHints?: Record<string, XSDType>
  /** Generate rdf:type triples (default: true) */
  generateTypes?: boolean
  /** Include rdfs:label from name/title columns (default: true) */
  generateLabels?: boolean
  /** Label columns to check for rdfs:label */
  labelColumns?: string[]
  /** Schema definition for type inference */
  schema?: SchemaDefinition
  /** Batch size for streaming (default: 10000) */
  batchSize?: number
}

/**
 * Relationship configuration for generating relationship triples
 */
export interface RelationshipConfig {
  /** Source table/collection */
  from: string
  /** Target table/collection */
  to: string
  /** Foreign key column in source */
  foreignKey: string
  /** Predicate URI for the relationship */
  predicate: string
  /** Inverse predicate (optional) */
  inversePredicate?: string
}

/**
 * Schema definition for type inference
 */
export interface SchemaDefinition {
  tableName: string
  columns: Array<{
    name: string
    type: string
    primaryKey?: boolean
    references?: {
      table: string
      column: string
    }
  }>
}

/**
 * Convert a dataset to N-Triples format
 *
 * @param inputPath - Path to input file or directory
 * @param outputPath - Path to output .nt file or directory
 * @param options - Conversion options
 */
export async function convert(
  inputPath: string,
  outputPath: string,
  options?: ToTriplesOptions
): Promise<void> {
  const opts: ToTriplesOptions = {
    format: 'triples',
    baseUri: 'http://example.org/',
    vocabUri: 'http://example.org/vocab/',
    primaryKey: 'id',
    generateTypes: true,
    generateLabels: true,
    labelColumns: ['name', 'title', 'label', 'primaryName', 'primaryTitle'],
    batchSize: 10000,
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

  console.log(`Converted to N-Triples: ${outputPath}`)
}

/**
 * Detect input file format from extension
 */
function detectInputFormat(inputPath: string): InputFormat {
  if (fs.statSync(inputPath).isDirectory()) {
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
  opts: ToTriplesOptions
): Promise<void> {
  const files = fs.readdirSync(inputDir)

  for (const file of files) {
    const inputPath = path.join(inputDir, file)
    const baseName = path.basename(file, path.extname(file))
    const outputPath = path.join(outputDir, `${baseName}.nt`)

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
  opts: ToTriplesOptions
): Promise<void> {
  const tableName = path.basename(inputPath, path.extname(inputPath))

  switch (format) {
    case 'csv':
    case 'tsv':
      await convertDelimitedToTriples(inputPath, outputPath, format === 'tsv' ? '\t' : ',', tableName, opts)
      break
    case 'json':
      await convertJSONToTriples(inputPath, outputPath, tableName, opts)
      break
    case 'jsonl':
      await convertJSONLToTriples(inputPath, outputPath, tableName, opts)
      break
    case 'sqlite':
      await convertSQLiteToTriples(inputPath, outputPath, opts)
      break
    default:
      throw new Error(`Unsupported input format: ${format}`)
  }
}

/**
 * Convert delimited (CSV/TSV) file to N-Triples
 */
async function convertDelimitedToTriples(
  inputPath: string,
  outputPath: string,
  delimiter: string,
  tableName: string,
  opts: ToTriplesOptions
): Promise<void> {
  let headers: string[] = []
  let lineCount = 0
  let tripleCount = 0

  const writeStream = createWriteStream(outputPath)

  // Write namespace comments for reference
  writeStream.write(`# N-Triples converted from: ${inputPath}\n`)
  writeStream.write(`# Base URI: ${opts.baseUri}\n`)
  writeStream.write(`# Vocabulary: ${opts.vocabUri}\n`)
  writeStream.write(`# Table: ${tableName}\n\n`)

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
    const record = createRecord(headers, values)
    const triples = recordToTriples(record, tableName, opts)

    for (const triple of triples) {
      writeStream.write(triple + '\n')
      tripleCount++
    }

    lineCount++
  }

  writeStream.end()
  console.log(`Converted ${lineCount - 1} records to ${tripleCount} triples: ${outputPath}`)
}

/**
 * Convert JSON array file to N-Triples
 */
async function convertJSONToTriples(
  inputPath: string,
  outputPath: string,
  tableName: string,
  opts: ToTriplesOptions
): Promise<void> {
  const content = fs.readFileSync(inputPath, 'utf8')
  const data = JSON.parse(content)
  const records = Array.isArray(data) ? data : [data]

  const writeStream = createWriteStream(outputPath)

  // Write header comments
  writeStream.write(`# N-Triples converted from: ${inputPath}\n`)
  writeStream.write(`# Base URI: ${opts.baseUri}\n\n`)

  let tripleCount = 0

  for (const record of records) {
    const triples = recordToTriples(record, tableName, opts)
    for (const triple of triples) {
      writeStream.write(triple + '\n')
      tripleCount++
    }
  }

  writeStream.end()
  console.log(`Converted ${records.length} records to ${tripleCount} triples: ${outputPath}`)
}

/**
 * Convert JSONL file to N-Triples
 */
async function convertJSONLToTriples(
  inputPath: string,
  outputPath: string,
  tableName: string,
  opts: ToTriplesOptions
): Promise<void> {
  const writeStream = createWriteStream(outputPath)

  // Write header comments
  writeStream.write(`# N-Triples converted from: ${inputPath}\n`)
  writeStream.write(`# Base URI: ${opts.baseUri}\n\n`)

  let recordCount = 0
  let tripleCount = 0

  const rl = createInterface({
    input: createReadStream(inputPath),
    crlfDelay: Infinity,
  })

  for await (const line of rl) {
    if (!line.trim()) continue

    try {
      const record = JSON.parse(line)
      const triples = recordToTriples(record, tableName, opts)

      for (const triple of triples) {
        writeStream.write(triple + '\n')
        tripleCount++
      }

      recordCount++
    } catch (err) {
      console.error(`Error parsing line ${recordCount}: ${line.slice(0, 100)}...`)
    }
  }

  writeStream.end()
  console.log(`Converted ${recordCount} records to ${tripleCount} triples: ${outputPath}`)
}

/**
 * Convert SQLite database to N-Triples
 */
async function convertSQLiteToTriples(
  inputPath: string,
  outputPath: string,
  opts: ToTriplesOptions
): Promise<void> {
  // Generate conversion script for SQLite
  const outputDir = path.dirname(outputPath)
  const scriptPath = path.join(outputDir, 'convert_sqlite_to_nt.sh')

  const script = `#!/bin/bash
# Convert SQLite to N-Triples
# Run: bash ${scriptPath}

DB="${inputPath}"
OUTPUT_DIR="${outputDir}"
BASE_URI="${opts.baseUri}"
VOCAB_URI="${opts.vocabUri}"

# Get table names
TABLES=$(sqlite3 "$DB" "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")

for TABLE in $TABLES; do
  echo "Converting table: $TABLE"
  OUTPUT="$OUTPUT_DIR/$TABLE.nt"

  # Export as JSON and convert to N-Triples
  sqlite3 -json "$DB" "SELECT * FROM $TABLE;" | \\
    node -e "
      const fs = require('fs');
      const data = JSON.parse(fs.readFileSync('/dev/stdin', 'utf8'));
      const baseUri = '$BASE_URI';
      const vocabUri = '$VOCAB_URI';
      const table = '$TABLE';

      function escape(str) {
        return str.replace(/\\\\/g, '\\\\\\\\').replace(/\"/g, '\\\\\"').replace(/\\n/g, '\\\\n');
      }

      for (const record of data) {
        const id = record.id || record.uuid || record.tconst || record.nconst || Object.values(record)[0];
        const subject = '<' + baseUri + table + '/' + id + '>';

        // Type triple
        console.log(subject + ' <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <' + vocabUri + table + '> .');

        // Property triples
        for (const [key, value] of Object.entries(record)) {
          if (value === null || value === undefined) continue;
          const predicate = '<' + vocabUri + key + '>';
          let object;
          if (typeof value === 'number') {
            object = '\"' + value + '\"^^<http://www.w3.org/2001/XMLSchema#' + (Number.isInteger(value) ? 'integer' : 'decimal') + '>';
          } else if (typeof value === 'boolean') {
            object = '\"' + value + '\"^^<http://www.w3.org/2001/XMLSchema#boolean>';
          } else {
            object = '\"' + escape(String(value)) + '\"';
          }
          console.log(subject + ' ' + predicate + ' ' + object + ' .');
        }
      }
    " > "$OUTPUT"

  echo "Created: $OUTPUT"
done

echo "Conversion complete!"
`

  fs.writeFileSync(scriptPath, script, { mode: 0o755 })
  console.log(`Generated SQLite to N-Triples conversion script: ${scriptPath}`)
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
function createRecord(headers: string[], values: string[]): Record<string, unknown> {
  const record: Record<string, unknown> = {}

  for (let i = 0; i < headers.length; i++) {
    const header = headers[i]
    let value: unknown = values[i]

    // Handle null markers
    if (value === '\\N' || value === '' || value === undefined) {
      record[header] = null
      continue
    }

    // Type coercion
    const strVal = value as string

    // Boolean
    if (strVal.toLowerCase() === 'true') {
      value = true
    } else if (strVal.toLowerCase() === 'false') {
      value = false
    }
    // Integer
    else if (/^-?\d+$/.test(strVal)) {
      const num = parseInt(strVal, 10)
      if (num >= Number.MIN_SAFE_INTEGER && num <= Number.MAX_SAFE_INTEGER) {
        value = num
      }
    }
    // Float
    else if (/^-?\d+\.\d+$/.test(strVal)) {
      value = parseFloat(strVal)
    }

    record[header] = value
  }

  return record
}

/**
 * Convert a record to N-Triples
 */
function recordToTriples(
  record: Record<string, unknown>,
  tableName: string,
  opts: ToTriplesOptions
): string[] {
  const triples: string[] = []

  // Get subject ID
  const idField = opts.primaryKey || 'id'
  const id = record[idField] || record['uuid'] || record['_id'] ||
             record['tconst'] || record['nconst'] || Object.values(record)[0]

  if (!id) return triples

  const subject = `<${opts.baseUri}${tableName}/${encodeURIComponent(String(id))}>`

  // Generate rdf:type triple
  if (opts.generateTypes) {
    const typeUri = `<${opts.vocabUri}${capitalizeFirst(tableName)}>`
    triples.push(`${subject} <${NAMESPACES.rdf}type> ${typeUri} .`)
  }

  // Generate rdfs:label triple
  if (opts.generateLabels) {
    for (const labelCol of opts.labelColumns || []) {
      if (record[labelCol]) {
        const label = formatLiteral(record[labelCol], 'xsd:string')
        triples.push(`${subject} <${NAMESPACES.rdfs}label> ${label} .`)
        break
      }
    }
  }

  // Generate property triples
  for (const [key, value] of Object.entries(record)) {
    if (value === null || value === undefined) continue
    if (key === idField) continue // Skip primary key

    // Get predicate URI
    let predicate: string
    if (opts.predicateMappings?.[key]) {
      predicate = opts.predicateMappings[key]
    } else {
      predicate = `<${opts.vocabUri}${key}>`
    }

    // Check if this is a relationship
    const relationship = opts.relationships?.find(
      (r) => r.from === tableName && r.foreignKey === key
    )

    if (relationship) {
      // Generate relationship triple
      const targetUri = `<${opts.baseUri}${relationship.to}/${encodeURIComponent(String(value))}>`
      triples.push(`${subject} <${relationship.predicate}> ${targetUri} .`)
    } else {
      // Generate literal triple
      const xsdType = opts.typeHints?.[key] || inferXSDType(value)
      const object = formatLiteral(value, xsdType)
      triples.push(`${subject} ${predicate} ${object} .`)
    }
  }

  return triples
}

/**
 * Infer XSD type from value
 */
function inferXSDType(value: unknown): XSDType {
  if (typeof value === 'boolean') return 'xsd:boolean'
  if (typeof value === 'number') {
    if (Number.isInteger(value)) {
      if (value >= -2147483648 && value <= 2147483647) return 'xsd:integer'
      return 'xsd:long'
    }
    return 'xsd:decimal'
  }

  const strVal = String(value)

  // Date patterns
  if (/^\d{4}-\d{2}-\d{2}$/.test(strVal)) return 'xsd:date'
  if (/^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}/.test(strVal)) return 'xsd:dateTime'
  if (/^\d{2}:\d{2}:\d{2}/.test(strVal)) return 'xsd:time'

  // URI pattern
  if (/^https?:\/\//.test(strVal)) return 'xsd:anyURI'

  return 'xsd:string'
}

/**
 * Format a literal value with XSD type annotation
 */
function formatLiteral(value: unknown, xsdType: XSDType): string {
  const escaped = escapeNTriplesString(String(value))

  // Plain string doesn't need type annotation
  if (xsdType === 'xsd:string') {
    return `"${escaped}"`
  }

  // Expand xsd prefix
  const typeUri = xsdType.replace('xsd:', NAMESPACES.xsd)
  return `"${escaped}"^^<${typeUri}>`
}

/**
 * Escape string for N-Triples format
 */
function escapeNTriplesString(str: string): string {
  return str
    .replace(/\\/g, '\\\\')
    .replace(/"/g, '\\"')
    .replace(/\n/g, '\\n')
    .replace(/\r/g, '\\r')
    .replace(/\t/g, '\\t')
}

/**
 * Capitalize first letter
 */
function capitalizeFirst(str: string): string {
  return str.charAt(0).toUpperCase() + str.slice(1)
}

/**
 * Generate relationship triples from schema definitions
 */
export function generateRelationshipsFromSchema(
  schema: SchemaDefinition,
  vocabUri: string = 'http://example.org/vocab/'
): RelationshipConfig[] {
  const relationships: RelationshipConfig[] = []

  for (const col of schema.columns) {
    if (col.references) {
      relationships.push({
        from: schema.tableName,
        to: col.references.table,
        foreignKey: col.name,
        predicate: `${vocabUri}${col.name.replace(/_id$/, '')}`,
      })
    }
  }

  return relationships
}

/**
 * Parse N-Triples content to records
 */
export function parseTriplesFile(content: string): Map<string, Record<string, unknown>> {
  const entities = new Map<string, Record<string, unknown>>()

  for (const line of content.split('\n')) {
    if (!line.trim() || line.startsWith('#')) continue

    // Simple N-Triples parser
    const match = line.match(/^<([^>]+)>\s+<([^>]+)>\s+(.+)\s+\.$/)
    if (!match) continue

    const [, subject, predicate, objectStr] = match

    if (!entities.has(subject)) {
      entities.set(subject, { _uri: subject })
    }

    const entity = entities.get(subject)!

    // Extract predicate name
    const predicateName = predicate.split(/[/#]/).pop() || predicate

    // Parse object value
    let value: unknown
    if (objectStr.startsWith('<')) {
      // URI reference
      value = objectStr.slice(1, -1)
    } else if (objectStr.startsWith('"')) {
      // Literal
      const literalMatch = objectStr.match(/^"(.*)"\s*(?:\^\^<([^>]+)>)?$/s)
      if (literalMatch) {
        const [, strValue, typeUri] = literalMatch
        value = unescapeNTriplesString(strValue)

        // Type conversion
        if (typeUri?.includes('integer') || typeUri?.includes('long')) {
          value = parseInt(value as string, 10)
        } else if (typeUri?.includes('decimal') || typeUri?.includes('float') || typeUri?.includes('double')) {
          value = parseFloat(value as string)
        } else if (typeUri?.includes('boolean')) {
          value = value === 'true'
        }
      }
    }

    entity[predicateName] = value
  }

  return entities
}

/**
 * Unescape N-Triples string
 */
function unescapeNTriplesString(str: string): string {
  return str
    .replace(/\\n/g, '\n')
    .replace(/\\r/g, '\r')
    .replace(/\\t/g, '\t')
    .replace(/\\"/g, '"')
    .replace(/\\\\/g, '\\')
}

export default convert
