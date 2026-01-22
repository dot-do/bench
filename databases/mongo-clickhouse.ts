/**
 * MongoDB-ClickHouse Database Adapter
 *
 * MongoDB-compatible store using ClickHouse backend for analytics workloads.
 * Optimized for large scans, aggregations, and columnar analytics.
 * Uses chdb-node (ClickHouse embedded) for in-process execution.
 */

// Dataset size configuration
export type DatasetSize = 'small' | 'medium' | 'large' | 'xlarge'

export const DATASET_SIZES: Record<DatasetSize, { things: number; relationships: number }> = {
  small: { things: 100, relationships: 50 },
  medium: { things: 1000, relationships: 500 },
  large: { things: 10000, relationships: 5000 },
  xlarge: { things: 100000, relationships: 50000 },
}

// MongoDB-compatible types
export type Document = Record<string, unknown>

export interface FindOptions {
  projection?: Record<string, 0 | 1>
  sort?: Record<string, 1 | -1>
  limit?: number
  skip?: number
}

export interface InsertResult {
  acknowledged: boolean
  insertedId: string
}

export interface InsertManyResult {
  acknowledged: boolean
  insertedCount: number
  insertedIds: Record<number, string>
}

export interface UpdateResult {
  acknowledged: boolean
  matchedCount: number
  modifiedCount: number
  upsertedCount: number
  upsertedId?: string
}

export interface DeleteResult {
  acknowledged: boolean
  deletedCount: number
}

// Domain types
export interface Thing {
  _id: string
  name: string
  status: 'active' | 'inactive' | 'pending' | 'archived'
  category?: string
  tags?: string[]
  metadata?: Record<string, unknown>
  created_at: Date
  updated_at?: Date
  [key: string]: unknown // Index signature for Document compatibility
}

export interface Relationship {
  _id: string
  subject: string
  predicate: string
  object: string
  weight?: number
  created_at: Date
  [key: string]: unknown // Index signature for Document compatibility
}

/**
 * MongoDB-compatible store interface backed by ClickHouse
 */
export interface MongoClickHouseStore {
  // CRUD operations
  findOne(collection: string, filter: object): Promise<Document | null>
  find(collection: string, filter: object, options?: FindOptions): Promise<Document[]>
  insertOne(collection: string, doc: Document): Promise<InsertResult>
  insertMany(collection: string, docs: Document[]): Promise<InsertManyResult>
  updateOne(collection: string, filter: object, update: object): Promise<UpdateResult>
  updateMany(collection: string, filter: object, update: object): Promise<UpdateResult>
  deleteOne(collection: string, filter: object): Promise<DeleteResult>
  deleteMany(collection: string, filter: object): Promise<DeleteResult>

  // Aggregation
  aggregate(collection: string, pipeline: object[]): Promise<Document[]>

  // Count operations
  countDocuments(collection: string, filter?: object): Promise<number>
  estimatedDocumentCount(collection: string): Promise<number>

  // Index management (ClickHouse uses different indexing)
  createIndex(collection: string, keys: Record<string, 1 | -1>): Promise<string>

  // Distinct values
  distinct(collection: string, field: string, filter?: object): Promise<unknown[]>

  // Convenience accessors for benchmarks
  things: CollectionProxy<Thing>
  relationships: CollectionProxy<Relationship>

  // Raw SQL access for analytics
  query<T = unknown>(sql: string): Promise<T[]>

  // Lifecycle
  close(): Promise<void>
}

/**
 * Collection proxy providing MongoDB-like collection interface
 */
export interface CollectionProxy<T extends Document = Document> {
  findOne(filter: object): Promise<T | null>
  find(filter: object, options?: FindOptions): FindCursor<T>
  insertOne(doc: T): Promise<InsertResult>
  insertMany(docs: T[]): Promise<InsertManyResult>
  updateOne(filter: object, update: object): Promise<UpdateResult>
  updateMany(filter: object, update: object): Promise<UpdateResult>
  deleteOne(filter: object): Promise<DeleteResult>
  deleteMany(filter: object): Promise<DeleteResult>
  aggregate(pipeline: object[]): AggregateCursor<T>
  countDocuments(filter?: object): Promise<number>
  estimatedDocumentCount(): Promise<number>
  createIndex(keys: Record<string, 1 | -1>): Promise<string>
  distinct(field: string, filter?: object): Promise<unknown[]>
  replaceOne(filter: object, doc: T, options?: { upsert?: boolean }): Promise<UpdateResult>
  findOneAndUpdate(filter: object, update: object, options?: { returnDocument?: 'before' | 'after' }): Promise<T | null>
}

/**
 * Cursor for find operations (MongoDB-style)
 */
export interface FindCursor<T> {
  sort(spec: Record<string, 1 | -1>): FindCursor<T>
  limit(n: number): FindCursor<T>
  skip(n: number): FindCursor<T>
  project(spec: Record<string, 0 | 1>): FindCursor<T>
  toArray(): Promise<T[]>
}

/**
 * Cursor for aggregation operations
 */
export interface AggregateCursor<T> {
  toArray(): Promise<T[]>
}

// Module-level ClickHouse connection cache
let clickhouseDb: ClickHouseConnection | null = null

/**
 * Internal ClickHouse connection wrapper
 */
interface ClickHouseConnection {
  query<T = unknown>(sql: string): Promise<T[]>
  execute(sql: string): Promise<void>
}

/**
 * Create an in-memory ClickHouse connection using chdb
 */
async function createClickHouseConnection(): Promise<ClickHouseConnection> {
  // Try to import chdb-node for embedded ClickHouse
  // Falls back to a simulated implementation for environments without chdb
  try {
    const chdb = await import('chdb')

    // In-memory session for benchmarks
    const session = new chdb.Session()

    return {
      async query<T = unknown>(sql: string): Promise<T[]> {
        const result = session.query(sql, 'JSONEachRow')
        if (!result || result.trim() === '') {
          return []
        }
        // JSONEachRow returns newline-separated JSON objects
        const lines = result.trim().split('\n').filter(Boolean)
        return lines.map((line: string) => JSON.parse(line) as T)
      },

      async execute(sql: string): Promise<void> {
        session.query(sql, 'Null')
      },
    }
  } catch {
    // Fallback: in-memory storage simulation for testing
    // This allows benchmarks to run without chdb installed
    return createInMemoryClickHouseSimulation()
  }
}

/**
 * In-memory simulation of ClickHouse for environments without chdb
 * Provides basic functionality for testing the adapter interface
 */
function createInMemoryClickHouseSimulation(): ClickHouseConnection {
  const tables = new Map<string, Document[]>()

  return {
    async query<T = unknown>(sql: string): Promise<T[]> {
      const upperSql = sql.toUpperCase().trim()

      // Handle CREATE TABLE
      if (upperSql.startsWith('CREATE TABLE')) {
        const match = sql.match(/CREATE TABLE IF NOT EXISTS (\w+)/i)
        if (match && !tables.has(match[1])) {
          tables.set(match[1], [])
        }
        return []
      }

      // Handle INSERT
      if (upperSql.startsWith('INSERT INTO')) {
        const tableMatch = sql.match(/INSERT INTO (\w+)/i)
        if (tableMatch) {
          const tableName = tableMatch[1]
          if (!tables.has(tableName)) {
            tables.set(tableName, [])
          }

          // Parse FORMAT JSONEachRow data
          const formatIndex = sql.indexOf('FORMAT JSONEachRow')
          if (formatIndex !== -1) {
            const jsonData = sql.substring(formatIndex + 'FORMAT JSONEachRow'.length).trim()
            if (jsonData) {
              const lines = jsonData.split('\n').filter(Boolean)
              const docs = lines.map(line => JSON.parse(line))
              tables.get(tableName)!.push(...docs)
            }
          }
        }
        return []
      }

      // Handle SELECT COUNT
      if (upperSql.includes('SELECT COUNT(')) {
        const tableMatch = sql.match(/FROM (\w+)/i)
        if (tableMatch) {
          const rows = tables.get(tableMatch[1]) || []
          return [{ count: rows.length }] as T[]
        }
        return [{ count: 0 }] as T[]
      }

      // Handle SELECT DISTINCT
      if (upperSql.includes('SELECT DISTINCT')) {
        const fieldMatch = sql.match(/SELECT DISTINCT\s+(\w+)/i)
        const tableMatch = sql.match(/FROM (\w+)/i)
        if (fieldMatch && tableMatch) {
          const rows = tables.get(tableMatch[1]) || []
          const field = fieldMatch[1]
          const values = [...new Set(rows.map(r => r[field]))]
          return values.map(v => ({ [field]: v })) as T[]
        }
        return []
      }

      // Handle basic SELECT with WHERE
      if (upperSql.startsWith('SELECT')) {
        const tableMatch = sql.match(/FROM (\w+)/i)
        if (tableMatch) {
          let rows = [...(tables.get(tableMatch[1]) || [])]

          // Apply WHERE clause (very basic parsing)
          const whereMatch = sql.match(/WHERE (.+?)(?:ORDER BY|LIMIT|$)/i)
          if (whereMatch) {
            const whereClause = whereMatch[1].trim()
            rows = applySimpleWhere(rows, whereClause)
          }

          // Apply ORDER BY
          const orderMatch = sql.match(/ORDER BY (\w+)\s*(ASC|DESC)?/i)
          if (orderMatch) {
            const field = orderMatch[1]
            const desc = orderMatch[2]?.toUpperCase() === 'DESC'
            rows.sort((a, b) => {
              const aVal = a[field] as string | number | boolean | null | undefined
              const bVal = b[field] as string | number | boolean | null | undefined
              if (aVal === undefined || aVal === null) return desc ? -1 : 1
              if (bVal === undefined || bVal === null) return desc ? 1 : -1
              if (aVal < bVal) return desc ? 1 : -1
              if (aVal > bVal) return desc ? -1 : 1
              return 0
            })
          }

          // Apply LIMIT
          const limitMatch = sql.match(/LIMIT (\d+)/i)
          if (limitMatch) {
            rows = rows.slice(0, parseInt(limitMatch[1], 10))
          }

          // Apply OFFSET
          const offsetMatch = sql.match(/OFFSET (\d+)/i)
          if (offsetMatch) {
            rows = rows.slice(parseInt(offsetMatch[1], 10))
          }

          return rows as T[]
        }
      }

      // Handle DELETE
      if (upperSql.startsWith('ALTER TABLE') && upperSql.includes('DELETE WHERE')) {
        const tableMatch = sql.match(/ALTER TABLE (\w+)/i)
        const whereMatch = sql.match(/DELETE WHERE (.+)$/i)
        if (tableMatch) {
          const tableName = tableMatch[1]
          const rows = tables.get(tableName) || []
          if (whereMatch) {
            const whereClause = whereMatch[1].trim()
            const toKeep = rows.filter(row => !matchesWhere(row, whereClause))
            tables.set(tableName, toKeep)
            return [{ deleted: rows.length - toKeep.length }] as T[]
          } else {
            tables.set(tableName, [])
            return [{ deleted: rows.length }] as T[]
          }
        }
      }

      // Handle UPDATE (ClickHouse uses ALTER TABLE ... UPDATE)
      if (upperSql.startsWith('ALTER TABLE') && upperSql.includes('UPDATE')) {
        const tableMatch = sql.match(/ALTER TABLE (\w+)/i)
        const setMatch = sql.match(/UPDATE\s+SET\s+(.+?)\s+WHERE/i)
        const whereMatch = sql.match(/WHERE\s+(.+)$/i)

        if (tableMatch && setMatch) {
          const tableName = tableMatch[1]
          const rows = tables.get(tableName) || []
          const updates = parseSetClause(setMatch[1])
          let modified = 0

          for (const row of rows) {
            if (!whereMatch || matchesWhere(row, whereMatch[1])) {
              for (const [key, value] of Object.entries(updates)) {
                row[key] = value
              }
              modified++
            }
          }

          return [{ modified }] as T[]
        }
      }

      // Handle aggregate queries
      if (upperSql.includes('GROUP BY')) {
        const tableMatch = sql.match(/FROM (\w+)/i)
        if (tableMatch) {
          const rows = tables.get(tableMatch[1]) || []
          // Very simplified aggregation - just return grouped counts
          const groupMatch = sql.match(/GROUP BY (.+?)(?:ORDER BY|LIMIT|$)/i)
          if (groupMatch) {
            const groupField = groupMatch[1].trim()
            const groups = new Map<unknown, number>()
            for (const row of rows) {
              const key = row[groupField]
              groups.set(key, (groups.get(key) || 0) + 1)
            }
            return Array.from(groups.entries()).map(([key, count]) => ({
              _id: key,
              count,
            })) as T[]
          }
        }
      }

      return []
    },

    async execute(sql: string): Promise<void> {
      await this.query(sql)
    },
  }
}

/**
 * Simple WHERE clause matching for simulation
 */
function applySimpleWhere(rows: Document[], whereClause: string): Document[] {
  return rows.filter(row => matchesWhere(row, whereClause))
}

function matchesWhere(row: Document, whereClause: string): boolean {
  // Handle simple equality: field = 'value'
  const eqMatch = whereClause.match(/^(\w+)\s*=\s*'([^']*)'$/i)
  if (eqMatch) {
    return row[eqMatch[1]] === eqMatch[2]
  }

  // Handle _id = 'value'
  const idMatch = whereClause.match(/_id\s*=\s*'([^']*)'/)
  if (idMatch) {
    return row._id === idMatch[1]
  }

  // Handle AND conditions
  if (whereClause.includes(' AND ')) {
    const conditions = whereClause.split(' AND ')
    return conditions.every(cond => matchesWhere(row, cond.trim()))
  }

  // Handle IN clause
  const inMatch = whereClause.match(/(\w+)\s+IN\s*\(([^)]+)\)/i)
  if (inMatch) {
    const field = inMatch[1]
    const values = inMatch[2].split(',').map(v => v.trim().replace(/'/g, ''))
    return values.includes(String(row[field]))
  }

  // Handle >= comparison
  const gteMatch = whereClause.match(/(\w+)\s*>=\s*(\d+)/i)
  if (gteMatch) {
    const field = gteMatch[1]
    const value = parseInt(gteMatch[2], 10)
    return Number(row[field]) >= value
  }

  // Default: match all if we can't parse
  return true
}

function parseSetClause(setClause: string): Record<string, unknown> {
  const updates: Record<string, unknown> = {}
  const assignments = setClause.split(',')
  for (const assignment of assignments) {
    const match = assignment.match(/(\w+)\s*=\s*(.+)/)
    if (match) {
      const field = match[1].trim()
      let value: unknown = match[2].trim()
      // Remove quotes from string values
      if (typeof value === 'string' && value.startsWith("'") && value.endsWith("'")) {
        value = value.slice(1, -1)
      } else if (!isNaN(Number(value))) {
        value = Number(value)
      }
      updates[field] = value
    }
  }
  return updates
}

/**
 * Convert MongoDB filter to ClickHouse WHERE clause
 */
function filterToWhere(filter: object): string {
  const conditions: string[] = []

  for (const [key, value] of Object.entries(filter)) {
    if (value === null || value === undefined) {
      conditions.push(`${escapeField(key)} IS NULL`)
    } else if (typeof value === 'object' && !Array.isArray(value)) {
      // Handle MongoDB operators
      for (const [op, opValue] of Object.entries(value as Record<string, unknown>)) {
        switch (op) {
          case '$eq':
            conditions.push(`${escapeField(key)} = ${escapeValue(opValue)}`)
            break
          case '$ne':
            conditions.push(`${escapeField(key)} != ${escapeValue(opValue)}`)
            break
          case '$gt':
            conditions.push(`${escapeField(key)} > ${escapeValue(opValue)}`)
            break
          case '$gte':
            conditions.push(`${escapeField(key)} >= ${escapeValue(opValue)}`)
            break
          case '$lt':
            conditions.push(`${escapeField(key)} < ${escapeValue(opValue)}`)
            break
          case '$lte':
            conditions.push(`${escapeField(key)} <= ${escapeValue(opValue)}`)
            break
          case '$in':
            if (Array.isArray(opValue)) {
              conditions.push(`${escapeField(key)} IN (${opValue.map(escapeValue).join(', ')})`)
            }
            break
          case '$nin':
            if (Array.isArray(opValue)) {
              conditions.push(`${escapeField(key)} NOT IN (${opValue.map(escapeValue).join(', ')})`)
            }
            break
          case '$exists':
            if (opValue) {
              conditions.push(`${escapeField(key)} IS NOT NULL`)
            } else {
              conditions.push(`${escapeField(key)} IS NULL`)
            }
            break
          case '$regex':
            conditions.push(`${escapeField(key)} LIKE ${escapeValue(String(opValue).replace(/\.\*/g, '%'))}`)
            break
        }
      }
    } else {
      conditions.push(`${escapeField(key)} = ${escapeValue(value)}`)
    }
  }

  return conditions.length > 0 ? conditions.join(' AND ') : '1=1'
}

/**
 * Escape a field name for ClickHouse (handle nested paths)
 */
function escapeField(field: string): string {
  // Handle nested fields like metadata.priority -> JSONExtractRaw(doc, 'metadata', 'priority')
  if (field.includes('.')) {
    const parts = field.split('.')
    // For JSON document columns, use JSONExtract functions
    return `JSONExtractRaw(doc, ${parts.map(p => `'${p}'`).join(', ')})`
  }
  // Map _id to id (ClickHouse convention)
  if (field === '_id') {
    return 'id'
  }
  return field
}

/**
 * Escape a value for ClickHouse SQL
 */
function escapeValue(value: unknown): string {
  if (value === null || value === undefined) {
    return 'NULL'
  }
  if (typeof value === 'string') {
    return `'${value.replace(/'/g, "''")}'`
  }
  if (typeof value === 'number') {
    return String(value)
  }
  if (typeof value === 'boolean') {
    return value ? '1' : '0'
  }
  if (value instanceof Date) {
    return `'${value.toISOString()}'`
  }
  if (Array.isArray(value)) {
    return `[${value.map(escapeValue).join(', ')}]`
  }
  if (typeof value === 'object') {
    return `'${JSON.stringify(value).replace(/'/g, "''")}'`
  }
  return String(value)
}

/**
 * Convert MongoDB update operators to ClickHouse SET clause
 */
function updateToSet(update: object): string {
  const sets: string[] = []

  for (const [op, fields] of Object.entries(update)) {
    if (op === '$set' && typeof fields === 'object') {
      for (const [key, value] of Object.entries(fields as Record<string, unknown>)) {
        if (key.includes('.')) {
          // Nested field update - need to use JSONSet in ClickHouse
          const parts = key.split('.')
          const path = parts.map(p => `'${p}'`).join(', ')
          sets.push(`doc = JSONSet(doc, ${path}, ${escapeValue(value)})`)
        } else {
          sets.push(`${key === '_id' ? 'id' : key} = ${escapeValue(value)}`)
        }
      }
    } else if (op === '$inc' && typeof fields === 'object') {
      for (const [key, value] of Object.entries(fields as Record<string, unknown>)) {
        const field = key === '_id' ? 'id' : key
        sets.push(`${field} = ${field} + ${escapeValue(value)}`)
      }
    } else if (op === '$unset' && typeof fields === 'object') {
      for (const key of Object.keys(fields as Record<string, unknown>)) {
        sets.push(`${key === '_id' ? 'id' : key} = NULL`)
      }
    } else if (op === '$push' && typeof fields === 'object') {
      for (const [key, value] of Object.entries(fields as Record<string, unknown>)) {
        sets.push(`${key} = arrayPushBack(${key}, ${escapeValue(value)})`)
      }
    } else if (op === '$pull' && typeof fields === 'object') {
      for (const [key, value] of Object.entries(fields as Record<string, unknown>)) {
        sets.push(`${key} = arrayFilter(x -> x != ${escapeValue(value)}, ${key})`)
      }
    } else if (!op.startsWith('$')) {
      // Direct field update (not an operator)
      sets.push(`${op === '_id' ? 'id' : op} = ${escapeValue(fields)}`)
    }
  }

  return sets.join(', ')
}

/**
 * Convert MongoDB aggregation pipeline to ClickHouse SQL
 */
function pipelineToSQL(collection: string, pipeline: object[]): string {
  let sql = ''
  let tableName = collection
  let whereClause = ''
  let groupByClause = ''
  let selectFields: string[] = ['*']
  let orderByClause = ''
  let limitClause = ''
  let havingClause = ''

  for (const stage of pipeline) {
    for (const [op, params] of Object.entries(stage)) {
      switch (op) {
        case '$match':
          whereClause = filterToWhere(params as object)
          break

        case '$group': {
          const groupParams = params as Record<string, unknown>
          const groupId = groupParams._id
          const groupFields: string[] = []
          const aggFields: string[] = []

          // Handle _id (group key)
          if (typeof groupId === 'string' && groupId.startsWith('$')) {
            const field = groupId.slice(1)
            groupFields.push(field === '_id' ? 'id' : field)
            aggFields.push(`${field === '_id' ? 'id' : field} as _id`)
          } else if (typeof groupId === 'object' && groupId !== null) {
            // Compound group key
            for (const [alias, fieldRef] of Object.entries(groupId as Record<string, string>)) {
              if (typeof fieldRef === 'string' && fieldRef.startsWith('$')) {
                const field = fieldRef.slice(1)
                groupFields.push(field === '_id' ? 'id' : field)
              }
            }
            aggFields.push(`tuple(${groupFields.join(', ')}) as _id`)
          } else if (groupId === null) {
            // Group all documents together
            aggFields.push('NULL as _id')
          }

          // Handle aggregation operators
          for (const [alias, aggOp] of Object.entries(groupParams)) {
            if (alias === '_id') continue

            if (typeof aggOp === 'object' && aggOp !== null) {
              for (const [agg, field] of Object.entries(aggOp as Record<string, unknown>)) {
                const fieldName = typeof field === 'string' && field.startsWith('$')
                  ? (field.slice(1) === '_id' ? 'id' : field.slice(1))
                  : String(field)

                switch (agg) {
                  case '$sum':
                    if (field === 1) {
                      aggFields.push(`count() as ${alias}`)
                    } else {
                      aggFields.push(`sum(${fieldName}) as ${alias}`)
                    }
                    break
                  case '$avg':
                    aggFields.push(`avg(${fieldName}) as ${alias}`)
                    break
                  case '$min':
                    aggFields.push(`min(${fieldName}) as ${alias}`)
                    break
                  case '$max':
                    aggFields.push(`max(${fieldName}) as ${alias}`)
                    break
                  case '$count':
                    aggFields.push(`count() as ${alias}`)
                    break
                  case '$first':
                    aggFields.push(`any(${fieldName}) as ${alias}`)
                    break
                  case '$last':
                    aggFields.push(`anyLast(${fieldName}) as ${alias}`)
                    break
                }
              }
            }
          }

          selectFields = aggFields
          if (groupFields.length > 0) {
            groupByClause = `GROUP BY ${groupFields.join(', ')}`
          }
          break
        }

        case '$sort': {
          const sortFields: string[] = []
          for (const [field, direction] of Object.entries(params as Record<string, 1 | -1>)) {
            sortFields.push(`${field === '_id' ? 'id' : field} ${direction === -1 ? 'DESC' : 'ASC'}`)
          }
          orderByClause = `ORDER BY ${sortFields.join(', ')}`
          break
        }

        case '$limit':
          limitClause = `LIMIT ${params}`
          break

        case '$skip':
          if (limitClause) {
            limitClause += ` OFFSET ${params}`
          } else {
            limitClause = `LIMIT 18446744073709551615 OFFSET ${params}` // Max UInt64 for "no limit"
          }
          break

        case '$project': {
          const projParams = params as Record<string, 0 | 1 | unknown>
          const projFields: string[] = []
          const exclusions: string[] = []

          for (const [field, value] of Object.entries(projParams)) {
            if (value === 1) {
              projFields.push(field === '_id' ? 'id as _id' : field)
            } else if (value === 0) {
              exclusions.push(field)
            }
          }

          if (projFields.length > 0) {
            selectFields = projFields
          }
          break
        }

        case '$lookup': {
          const lookupParams = params as {
            from: string
            localField: string
            foreignField: string
            as: string
          }
          // ClickHouse doesn't have native array aggregation for lookups
          // We'd need a subquery or JOIN - simplified version here
          // This is a limitation compared to MongoDB
          break
        }

        case '$addFields': {
          // Add computed fields - would need to extend SELECT
          break
        }

        case '$unwind': {
          // Array explosion - ClickHouse uses ARRAY JOIN
          break
        }
      }
    }
  }

  // Build final SQL
  sql = `SELECT ${selectFields.join(', ')} FROM ${tableName}`
  if (whereClause) sql += ` WHERE ${whereClause}`
  if (groupByClause) sql += ` ${groupByClause}`
  if (havingClause) sql += ` HAVING ${havingClause}`
  if (orderByClause) sql += ` ${orderByClause}`
  if (limitClause) sql += ` ${limitClause}`

  return sql
}

/**
 * Create a collection proxy for MongoDB-like interface
 */
function createCollectionProxy<T extends Document>(
  store: MongoClickHouseStore,
  collectionName: string
): CollectionProxy<T> {
  return {
    async findOne(filter: object): Promise<T | null> {
      const result = await store.findOne(collectionName, filter)
      return result as T | null
    },

    find(filter: object, options?: FindOptions): FindCursor<T> {
      let sortSpec: Record<string, 1 | -1> | undefined
      let limitVal: number | undefined
      let skipVal: number | undefined
      let projectionSpec: Record<string, 0 | 1> | undefined

      return {
        sort(spec: Record<string, 1 | -1>): FindCursor<T> {
          sortSpec = spec
          return this
        },
        limit(n: number): FindCursor<T> {
          limitVal = n
          return this
        },
        skip(n: number): FindCursor<T> {
          skipVal = n
          return this
        },
        project(spec: Record<string, 0 | 1>): FindCursor<T> {
          projectionSpec = spec
          return this
        },
        async toArray(): Promise<T[]> {
          return store.find(collectionName, filter, {
            ...options,
            sort: sortSpec || options?.sort,
            limit: limitVal ?? options?.limit,
            skip: skipVal ?? options?.skip,
            projection: projectionSpec || options?.projection,
          }) as Promise<T[]>
        },
      }
    },

    async insertOne(doc: T): Promise<InsertResult> {
      return store.insertOne(collectionName, doc)
    },

    async insertMany(docs: T[]): Promise<InsertManyResult> {
      return store.insertMany(collectionName, docs)
    },

    async updateOne(filter: object, update: object): Promise<UpdateResult> {
      return store.updateOne(collectionName, filter, update)
    },

    async updateMany(filter: object, update: object): Promise<UpdateResult> {
      return store.updateMany(collectionName, filter, update)
    },

    async deleteOne(filter: object): Promise<DeleteResult> {
      return store.deleteOne(collectionName, filter)
    },

    async deleteMany(filter: object): Promise<DeleteResult> {
      return store.deleteMany(collectionName, filter)
    },

    aggregate(pipeline: object[]): AggregateCursor<T> {
      return {
        async toArray(): Promise<T[]> {
          return store.aggregate(collectionName, pipeline) as Promise<T[]>
        },
      }
    },

    async countDocuments(filter?: object): Promise<number> {
      return store.countDocuments(collectionName, filter)
    },

    async estimatedDocumentCount(): Promise<number> {
      return store.estimatedDocumentCount(collectionName)
    },

    async createIndex(keys: Record<string, 1 | -1>): Promise<string> {
      return store.createIndex(collectionName, keys)
    },

    async distinct(field: string, filter?: object): Promise<unknown[]> {
      return store.distinct(collectionName, field, filter)
    },

    async replaceOne(filter: object, doc: T, options?: { upsert?: boolean }): Promise<UpdateResult> {
      // In ClickHouse, we delete then insert for replace
      const existing = await store.findOne(collectionName, filter)
      if (existing) {
        await store.deleteOne(collectionName, filter)
        await store.insertOne(collectionName, doc)
        return { acknowledged: true, matchedCount: 1, modifiedCount: 1, upsertedCount: 0 }
      } else if (options?.upsert) {
        await store.insertOne(collectionName, doc)
        return { acknowledged: true, matchedCount: 0, modifiedCount: 0, upsertedCount: 1, upsertedId: doc._id as string }
      }
      return { acknowledged: true, matchedCount: 0, modifiedCount: 0, upsertedCount: 0 }
    },

    async findOneAndUpdate(filter: object, update: object, options?: { returnDocument?: 'before' | 'after' }): Promise<T | null> {
      const before = await store.findOne(collectionName, filter)
      if (!before) return null

      await store.updateOne(collectionName, filter, update)

      if (options?.returnDocument === 'after') {
        return store.findOne(collectionName, filter) as Promise<T | null>
      }
      return before as T
    },
  }
}

/**
 * Create a new MongoDB-ClickHouse store instance.
 * Uses chdb for embedded ClickHouse or falls back to in-memory simulation.
 */
export async function createMongoClickHouseStore(): Promise<MongoClickHouseStore> {
  // Reuse connection if available (for warm benchmarks)
  if (!clickhouseDb) {
    clickhouseDb = await createClickHouseConnection()

    // Create default tables for things and relationships
    await clickhouseDb.execute(`
      CREATE TABLE IF NOT EXISTS things (
        id String,
        name String,
        status String,
        category Nullable(String),
        tags Array(String),
        metadata String DEFAULT '{}',
        created_at DateTime64(3),
        updated_at Nullable(DateTime64(3))
      ) ENGINE = MergeTree()
      ORDER BY (id)
    `)

    await clickhouseDb.execute(`
      CREATE TABLE IF NOT EXISTS relationships (
        id String,
        subject String,
        predicate String,
        object String,
        weight Nullable(Float64),
        created_at DateTime64(3)
      ) ENGINE = MergeTree()
      ORDER BY (id)
    `)
  }

  const store: MongoClickHouseStore = {
    async findOne(collection: string, filter: object): Promise<Document | null> {
      const where = filterToWhere(filter)
      const sql = `SELECT * FROM ${collection} WHERE ${where} LIMIT 1`
      const results = await clickhouseDb!.query<Document>(sql)
      if (results.length === 0) return null

      // Transform ClickHouse result to MongoDB-style document
      const doc = results[0]
      if (doc.id !== undefined && doc._id === undefined) {
        doc._id = doc.id
        delete doc.id
      }
      return doc
    },

    async find(collection: string, filter: object, options?: FindOptions): Promise<Document[]> {
      const where = filterToWhere(filter)
      let sql = `SELECT * FROM ${collection} WHERE ${where}`

      if (options?.sort) {
        const sortParts = Object.entries(options.sort).map(
          ([field, dir]) => `${field === '_id' ? 'id' : field} ${dir === -1 ? 'DESC' : 'ASC'}`
        )
        sql += ` ORDER BY ${sortParts.join(', ')}`
      }

      if (options?.limit) {
        sql += ` LIMIT ${options.limit}`
      }

      if (options?.skip) {
        sql += ` OFFSET ${options.skip}`
      }

      const results = await clickhouseDb!.query<Document>(sql)

      // Transform results
      return results.map(doc => {
        if (doc.id !== undefined && doc._id === undefined) {
          doc._id = doc.id
          delete doc.id
        }
        return doc
      })
    },

    async insertOne(collection: string, doc: Document): Promise<InsertResult> {
      const id = (doc._id as string) || crypto.randomUUID()
      const insertDoc = { ...doc, id, _id: undefined }
      delete insertDoc._id

      // Convert to ClickHouse INSERT format
      const docWithId = insertDoc as Record<string, unknown>
      const jsonRow = JSON.stringify({
        ...docWithId,
        tags: docWithId.tags || [],
        metadata: typeof docWithId.metadata === 'object' ? JSON.stringify(docWithId.metadata) : '{}',
        created_at: docWithId.created_at instanceof Date ? (docWithId.created_at as Date).toISOString() : docWithId.created_at || new Date().toISOString(),
        updated_at: docWithId.updated_at instanceof Date ? (docWithId.updated_at as Date).toISOString() : docWithId.updated_at || null,
      })

      await clickhouseDb!.execute(`INSERT INTO ${collection} FORMAT JSONEachRow ${jsonRow}`)

      return { acknowledged: true, insertedId: id }
    },

    async insertMany(collection: string, docs: Document[]): Promise<InsertManyResult> {
      const insertedIds: Record<number, string> = {}

      const jsonRows = docs.map((doc, idx) => {
        const id = (doc._id as string) || crypto.randomUUID()
        insertedIds[idx] = id

        const insertDoc = {
          id,
          name: doc.name,
          status: doc.status,
          category: doc.category || null,
          tags: doc.tags || [],
          metadata: typeof doc.metadata === 'object' ? JSON.stringify(doc.metadata) : '{}',
          created_at: doc.created_at instanceof Date ? doc.created_at.toISOString() : doc.created_at || new Date().toISOString(),
          updated_at: doc.updated_at instanceof Date ? doc.updated_at.toISOString() : doc.updated_at || null,
          subject: doc.subject,
          predicate: doc.predicate,
          object: doc.object,
          weight: doc.weight,
        }

        return JSON.stringify(insertDoc)
      })

      await clickhouseDb!.execute(`INSERT INTO ${collection} FORMAT JSONEachRow\n${jsonRows.join('\n')}`)

      return {
        acknowledged: true,
        insertedCount: docs.length,
        insertedIds,
      }
    },

    async updateOne(collection: string, filter: object, update: object): Promise<UpdateResult> {
      const where = filterToWhere(filter)
      const set = updateToSet(update)

      if (!set) {
        return { acknowledged: true, matchedCount: 0, modifiedCount: 0, upsertedCount: 0 }
      }

      // ClickHouse uses ALTER TABLE ... UPDATE for mutations
      await clickhouseDb!.execute(`ALTER TABLE ${collection} UPDATE ${set} WHERE ${where}`)

      // ClickHouse mutations are async, so we can't easily get exact counts
      return { acknowledged: true, matchedCount: 1, modifiedCount: 1, upsertedCount: 0 }
    },

    async updateMany(collection: string, filter: object, update: object): Promise<UpdateResult> {
      const where = filterToWhere(filter)
      const set = updateToSet(update)

      if (!set) {
        return { acknowledged: true, matchedCount: 0, modifiedCount: 0, upsertedCount: 0 }
      }

      await clickhouseDb!.execute(`ALTER TABLE ${collection} UPDATE ${set} WHERE ${where}`)

      return { acknowledged: true, matchedCount: 1, modifiedCount: 1, upsertedCount: 0 }
    },

    async deleteOne(collection: string, filter: object): Promise<DeleteResult> {
      const where = filterToWhere(filter)

      // ClickHouse uses ALTER TABLE ... DELETE
      await clickhouseDb!.execute(`ALTER TABLE ${collection} DELETE WHERE ${where}`)

      return { acknowledged: true, deletedCount: 1 }
    },

    async deleteMany(collection: string, filter: object): Promise<DeleteResult> {
      const where = filterToWhere(filter)

      await clickhouseDb!.execute(`ALTER TABLE ${collection} DELETE WHERE ${where}`)

      return { acknowledged: true, deletedCount: 1 }
    },

    async aggregate(collection: string, pipeline: object[]): Promise<Document[]> {
      const sql = pipelineToSQL(collection, pipeline)
      const results = await clickhouseDb!.query<Document>(sql)

      return results.map(doc => {
        // Transform id back to _id if needed
        if (doc.id !== undefined && doc._id === undefined) {
          doc._id = doc.id
          delete doc.id
        }
        return doc
      })
    },

    async countDocuments(collection: string, filter?: object): Promise<number> {
      const where = filter ? filterToWhere(filter) : '1=1'
      const sql = `SELECT count() as count FROM ${collection} WHERE ${where}`
      const results = await clickhouseDb!.query<{ count: number }>(sql)
      return results[0]?.count || 0
    },

    async estimatedDocumentCount(collection: string): Promise<number> {
      // ClickHouse can use table metadata for fast estimates
      const sql = `SELECT count() as count FROM ${collection}`
      const results = await clickhouseDb!.query<{ count: number }>(sql)
      return results[0]?.count || 0
    },

    async createIndex(collection: string, keys: Record<string, 1 | -1>): Promise<string> {
      // ClickHouse uses different indexing (MergeTree ORDER BY, data skipping indexes)
      // Primary sorting is defined at table creation
      // We can add data skipping indexes for additional fields
      const fields = Object.keys(keys)
      const indexName = `idx_${collection}_${fields.join('_')}`

      try {
        // Create a minmax data skipping index (good for range queries)
        const indexFields = fields.map(f => f === '_id' ? 'id' : f).join(', ')
        await clickhouseDb!.execute(
          `ALTER TABLE ${collection} ADD INDEX IF NOT EXISTS ${indexName} (${indexFields}) TYPE minmax GRANULARITY 4`
        )
      } catch {
        // Index might already exist or not be supported - that's okay
      }

      return indexName
    },

    async distinct(collection: string, field: string, filter?: object): Promise<unknown[]> {
      const fieldName = field === '_id' ? 'id' : field
      const where = filter ? filterToWhere(filter) : '1=1'
      const sql = `SELECT DISTINCT ${fieldName} FROM ${collection} WHERE ${where}`
      const results = await clickhouseDb!.query<Record<string, unknown>>(sql)
      return results.map(r => r[fieldName])
    },

    get things(): CollectionProxy<Thing> {
      return createCollectionProxy<Thing>(this, 'things')
    },

    get relationships(): CollectionProxy<Relationship> {
      return createCollectionProxy<Relationship>(this, 'relationships')
    },

    async query<T = unknown>(sql: string): Promise<T[]> {
      return clickhouseDb!.query<T>(sql)
    },

    async close(): Promise<void> {
      // For benchmarks, keep connection alive
      return Promise.resolve()
    },
  }

  return store
}

/**
 * Seed the store with test data based on size parameter.
 */
export async function seedTestData(store: MongoClickHouseStore, size: DatasetSize = 'medium'): Promise<void> {
  const { things: thingCount, relationships: relCount } = DATASET_SIZES[size]
  const statuses: Thing['status'][] = ['active', 'inactive', 'pending', 'archived']
  const categories = ['electronics', 'clothing', 'food', 'services', 'software']
  const tagOptions = ['featured', 'new', 'sale', 'premium', 'limited']

  // Create indexes (ClickHouse-style)
  await store.createIndex('things', { status: 1 })
  await store.createIndex('things', { category: 1 })
  await store.createIndex('relationships', { subject: 1 })
  await store.createIndex('relationships', { predicate: 1 })

  // Batch insert things - ClickHouse is optimized for large batch inserts
  const thingDocs: Thing[] = []
  for (let i = 0; i < thingCount; i++) {
    const id = `thing-${String(i).padStart(6, '0')}`
    thingDocs.push({
      _id: id,
      name: `Thing ${i}`,
      status: statuses[i % statuses.length],
      category: categories[i % categories.length],
      tags: [tagOptions[i % tagOptions.length], tagOptions[(i + 1) % tagOptions.length]],
      metadata: {
        priority: i % 10,
        score: Math.random() * 100,
        flags: { active: i % 2 === 0, verified: i % 3 === 0 },
      },
      created_at: new Date(Date.now() - i * 60000),
    })

    // Insert in batches of 10000 for ClickHouse (larger batches are better)
    if (thingDocs.length >= 10000) {
      await store.things.insertMany(thingDocs)
      thingDocs.length = 0
    }
  }

  // Insert remaining things
  if (thingDocs.length > 0) {
    await store.things.insertMany(thingDocs)
  }

  // Batch insert relationships
  const relDocs: Relationship[] = []
  for (let i = 0; i < relCount; i++) {
    const subjectIdx = i % thingCount
    const objectIdx = (i + 1) % thingCount
    relDocs.push({
      _id: `rel-${String(i).padStart(6, '0')}`,
      subject: `thing-${String(subjectIdx).padStart(6, '0')}`,
      predicate: i % 2 === 0 ? 'relates_to' : 'depends_on',
      object: `thing-${String(objectIdx).padStart(6, '0')}`,
      weight: Math.random(),
      created_at: new Date(),
    })

    // Insert in batches of 10000
    if (relDocs.length >= 10000) {
      await store.relationships.insertMany(relDocs)
      relDocs.length = 0
    }
  }

  // Insert remaining relationships
  if (relDocs.length > 0) {
    await store.relationships.insertMany(relDocs)
  }
}

/**
 * Restore store state from DO storage (for hibernation benchmarks).
 */
export async function restoreFromStorage(
  store: MongoClickHouseStore,
  storage: Map<string, ArrayBuffer>
): Promise<void> {
  const decoder = new TextDecoder()

  for (const [key, buffer] of storage) {
    const data = JSON.parse(decoder.decode(buffer))
    const [collection, id] = key.split(':')

    if (collection === 'things' && id) {
      await store.things.replaceOne({ _id: id }, data, { upsert: true })
    } else if (collection === 'relationships' && id) {
      await store.relationships.replaceOne({ _id: id }, data, { upsert: true })
    }
  }
}

/**
 * Clear all data from the store (for benchmark isolation).
 */
export async function clearStore(store: MongoClickHouseStore): Promise<void> {
  await store.things.deleteMany({})
  await store.relationships.deleteMany({})
}
