/**
 * Container Durable Objects
 *
 * Simulated container Durable Objects for database benchmarking.
 * These DOs provide in-memory database implementations that can be used
 * for benchmarking when real Cloudflare Containers are not available.
 *
 * Each DO:
 * - Maintains an in-memory data store
 * - Tracks startup time and query latency metrics
 * - Supports the fetch() handler pattern for Durable Objects
 *
 * @see https://developers.cloudflare.com/durable-objects/
 */

// =============================================================================
// Types
// =============================================================================

/** Query result type */
interface QueryResult<T = unknown> {
  rows: T[]
  rowCount: number
  command: string
}

/** Execution result type */
interface ExecuteResult {
  rowsAffected: number
  command: string
}

/** Metrics tracked by each DO */
interface DOMetrics {
  startupTimeMs: number
  totalQueries: number
  totalExecutes: number
  avgQueryLatencyMs: number
  avgExecuteLatencyMs: number
  lastActivityAt: number
}

/** Table schema for SQL-like databases */
interface TableSchema {
  columns: Map<string, 'TEXT' | 'INTEGER' | 'REAL' | 'BLOB' | 'JSON' | 'TIMESTAMP'>
  primaryKey?: string
}

/** Row type */
type Row = Record<string, unknown>

// =============================================================================
// Base Container DO
// =============================================================================

/**
 * Base class for simulated container DOs.
 * Provides common functionality for all database types.
 * Implements the DurableObject interface.
 */
abstract class BaseContainerDO implements DurableObject {
  protected state: DurableObjectState
  protected env: unknown
  protected tables: Map<string, Row[]> = new Map()
  protected schemas: Map<string, TableSchema> = new Map()
  protected metrics: DOMetrics
  protected startTime: number

  constructor(state: DurableObjectState, env: unknown) {
    this.state = state
    this.env = env
    this.startTime = performance.now()
    this.metrics = {
      startupTimeMs: 0,
      totalQueries: 0,
      totalExecutes: 0,
      avgQueryLatencyMs: 0,
      avgExecuteLatencyMs: 0,
      lastActivityAt: Date.now(),
    }
  }

  /** Initialize the DO (simulate cold start) */
  protected async initialize(): Promise<void> {
    // Simulate database startup time (varies by database type)
    const startupDelay = this.getSimulatedStartupDelay()
    await this.sleep(startupDelay)
    this.metrics.startupTimeMs = performance.now() - this.startTime
  }

  /** Get simulated startup delay in ms */
  protected abstract getSimulatedStartupDelay(): number

  /** Get database type name */
  protected abstract getDatabaseType(): string

  /** Handle incoming fetch requests */
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)
    const path = url.pathname

    try {
      // Health check endpoints
      if (path === '/health' || path === '/ready') {
        return this.jsonResponse({ status: 'ok', database: this.getDatabaseType() })
      }

      // Metrics endpoint
      if (path === '/metrics') {
        return this.jsonResponse(this.metrics)
      }

      // Query endpoint
      if (path === '/query' && request.method === 'POST') {
        const body = await request.json() as { query: string; params?: unknown[] }
        const result = await this.handleQuery(body.query, body.params)
        return this.jsonResponse(result)
      }

      // Execute endpoint
      if (path === '/execute' && request.method === 'POST') {
        const body = await request.json() as { query: string; params?: unknown[] }
        const result = await this.handleExecute(body.query, body.params)
        return this.jsonResponse(result)
      }

      // Ping endpoint
      if (path === '/ping') {
        return this.jsonResponse({ pong: true, timestamp: Date.now() })
      }

      return new Response(JSON.stringify({ error: 'Not found' }), {
        status: 404,
        headers: { 'Content-Type': 'application/json' },
      })
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      return new Response(JSON.stringify({ error: message }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      })
    }
  }

  /** Handle a query request */
  protected async handleQuery(sql: string, params?: unknown[]): Promise<QueryResult> {
    const start = performance.now()
    this.metrics.lastActivityAt = Date.now()

    try {
      const result = this.executeSQL(sql, params, 'query')
      const latency = performance.now() - start

      // Update metrics
      this.metrics.totalQueries++
      this.metrics.avgQueryLatencyMs =
        (this.metrics.avgQueryLatencyMs * (this.metrics.totalQueries - 1) + latency) /
        this.metrics.totalQueries

      // Simulate query latency based on complexity
      await this.simulateQueryLatency(sql)

      return result as QueryResult
    } catch (error) {
      throw error
    }
  }

  /** Handle an execute request */
  protected async handleExecute(sql: string, params?: unknown[]): Promise<ExecuteResult> {
    const start = performance.now()
    this.metrics.lastActivityAt = Date.now()

    try {
      const result = this.executeSQL(sql, params, 'execute')
      const latency = performance.now() - start

      // Update metrics
      this.metrics.totalExecutes++
      this.metrics.avgExecuteLatencyMs =
        (this.metrics.avgExecuteLatencyMs * (this.metrics.totalExecutes - 1) + latency) /
        this.metrics.totalExecutes

      // Simulate execute latency
      await this.simulateExecuteLatency(sql)

      return result as ExecuteResult
    } catch (error) {
      throw error
    }
  }

  /** Execute SQL statement */
  protected executeSQL(sql: string, params?: unknown[], mode: 'query' | 'execute' = 'query'): QueryResult | ExecuteResult {
    const normalizedSQL = sql.trim().toUpperCase()

    // CREATE TABLE
    if (normalizedSQL.startsWith('CREATE TABLE')) {
      return this.handleCreateTable(sql)
    }

    // INSERT
    if (normalizedSQL.startsWith('INSERT')) {
      return this.handleInsert(sql, params)
    }

    // SELECT
    if (normalizedSQL.startsWith('SELECT')) {
      return this.handleSelect(sql, params)
    }

    // UPDATE
    if (normalizedSQL.startsWith('UPDATE')) {
      return this.handleUpdate(sql, params)
    }

    // DELETE
    if (normalizedSQL.startsWith('DELETE')) {
      return this.handleDelete(sql, params)
    }

    // DROP TABLE
    if (normalizedSQL.startsWith('DROP TABLE')) {
      return this.handleDropTable(sql)
    }

    // Default: return empty result
    if (mode === 'query') {
      return { rows: [], rowCount: 0, command: 'UNKNOWN' }
    }
    return { rowsAffected: 0, command: 'UNKNOWN' }
  }

  /** Handle CREATE TABLE */
  protected handleCreateTable(sql: string): ExecuteResult {
    // Extract table name (simplified parser)
    const match = sql.match(/CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?["`]?(\w+)["`]?/i)
    if (!match) {
      throw new Error('Invalid CREATE TABLE syntax')
    }

    const tableName = match[1]
    if (!this.tables.has(tableName)) {
      this.tables.set(tableName, [])
      this.schemas.set(tableName, { columns: new Map() })
    }

    return { rowsAffected: 0, command: 'CREATE TABLE' }
  }

  /** Handle INSERT */
  protected handleInsert(sql: string, params?: unknown[]): ExecuteResult {
    // Extract table name and handle ON CONFLICT
    const match = sql.match(/INSERT\s+INTO\s+["`]?(\w+)["`]?\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)/i)
    if (!match) {
      throw new Error('Invalid INSERT syntax')
    }

    const [, tableName, columnsStr, valuesStr] = match
    const columns = columnsStr.split(',').map(c => c.trim().replace(/["`]/g, ''))
    const values = this.parseValues(valuesStr, params)

    // Create table if it doesn't exist
    if (!this.tables.has(tableName)) {
      this.tables.set(tableName, [])
    }

    const row: Row = {}
    columns.forEach((col, i) => {
      row[col] = values[i]
    })

    // Handle ON CONFLICT (upsert)
    const hasOnConflict = sql.toUpperCase().includes('ON CONFLICT')
    const table = this.tables.get(tableName)!

    if (hasOnConflict && row.id) {
      const existingIndex = table.findIndex(r => r.id === row.id)
      if (existingIndex >= 0) {
        table[existingIndex] = { ...table[existingIndex], ...row }
        return { rowsAffected: 1, command: 'UPDATE' }
      }
    }

    table.push(row)
    return { rowsAffected: 1, command: 'INSERT' }
  }

  /** Handle SELECT */
  protected handleSelect(sql: string, params?: unknown[]): QueryResult {
    // Extract table name
    const match = sql.match(/FROM\s+["`]?(\w+)["`]?/i)
    if (!match) {
      // Handle SELECT without FROM (e.g., SELECT 1)
      return { rows: [{ result: 1 }], rowCount: 1, command: 'SELECT' }
    }

    const tableName = match[1]
    const table = this.tables.get(tableName) || []

    // Apply WHERE clause if present
    let filteredRows = [...table]
    const whereMatch = sql.match(/WHERE\s+(.+?)(?:\s+ORDER|\s+LIMIT|\s+GROUP|\s*$)/i)
    if (whereMatch && params) {
      filteredRows = this.applyWhereClause(filteredRows, whereMatch[1], params)
    }

    // Apply ORDER BY if present
    const orderMatch = sql.match(/ORDER\s+BY\s+(\w+)(?:\s+(ASC|DESC))?/i)
    if (orderMatch) {
      const [, orderCol, orderDir] = orderMatch
      filteredRows.sort((a, b) => {
        const aVal = a[orderCol] as string | number | null
        const bVal = b[orderCol] as string | number | null
        if (aVal === bVal) return 0
        if (aVal === null || aVal === undefined) return 1
        if (bVal === null || bVal === undefined) return -1
        const cmp = aVal < bVal ? -1 : 1
        return orderDir?.toUpperCase() === 'DESC' ? -cmp : cmp
      })
    }

    // Apply LIMIT if present
    const limitMatch = sql.match(/LIMIT\s+(\d+)/i)
    if (limitMatch) {
      filteredRows = filteredRows.slice(0, parseInt(limitMatch[1], 10))
    }

    // Handle GROUP BY with aggregations
    const groupMatch = sql.match(/GROUP\s+BY\s+(\w+)/i)
    if (groupMatch) {
      filteredRows = this.applyGroupBy(filteredRows, sql, groupMatch[1])
    }

    return { rows: filteredRows, rowCount: filteredRows.length, command: 'SELECT' }
  }

  /** Handle UPDATE */
  protected handleUpdate(sql: string, params?: unknown[]): ExecuteResult {
    const match = sql.match(/UPDATE\s+["`]?(\w+)["`]?\s+SET\s+(.+?)\s+WHERE\s+(.+)/i)
    if (!match) {
      throw new Error('Invalid UPDATE syntax')
    }

    const [, tableName, setClause, whereClause] = match
    const table = this.tables.get(tableName)
    if (!table) {
      return { rowsAffected: 0, command: 'UPDATE' }
    }

    const updates = this.parseSetClause(setClause, params)
    const matchingRows = this.applyWhereClause(table, whereClause, params)

    let affected = 0
    for (const row of matchingRows) {
      const index = table.indexOf(row)
      if (index >= 0) {
        Object.assign(table[index], updates)
        affected++
      }
    }

    return { rowsAffected: affected, command: 'UPDATE' }
  }

  /** Handle DELETE */
  protected handleDelete(sql: string, params?: unknown[]): ExecuteResult {
    const match = sql.match(/DELETE\s+FROM\s+["`]?(\w+)["`]?(?:\s+WHERE\s+(.+))?/i)
    if (!match) {
      throw new Error('Invalid DELETE syntax')
    }

    const [, tableName, whereClause] = match
    const table = this.tables.get(tableName)
    if (!table) {
      return { rowsAffected: 0, command: 'DELETE' }
    }

    if (!whereClause) {
      const count = table.length
      table.length = 0
      return { rowsAffected: count, command: 'DELETE' }
    }

    const toDelete = this.applyWhereClause(table, whereClause, params)
    let affected = 0
    for (const row of toDelete) {
      const index = table.indexOf(row)
      if (index >= 0) {
        table.splice(index, 1)
        affected++
      }
    }

    return { rowsAffected: affected, command: 'DELETE' }
  }

  /** Handle DROP TABLE */
  protected handleDropTable(sql: string): ExecuteResult {
    const match = sql.match(/DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?["`]?(\w+)["`]?/i)
    if (!match) {
      throw new Error('Invalid DROP TABLE syntax')
    }

    const tableName = match[1]
    this.tables.delete(tableName)
    this.schemas.delete(tableName)

    return { rowsAffected: 0, command: 'DROP TABLE' }
  }

  /** Parse VALUES clause */
  protected parseValues(valuesStr: string, params?: unknown[]): unknown[] {
    const values: unknown[] = []
    const parts = valuesStr.split(',')

    for (let i = 0; i < parts.length; i++) {
      const part = parts[i].trim()

      // Handle parameterized values ($1, $2, etc.)
      if (part.match(/^\$(\d+)$/)) {
        const paramIndex = parseInt(part.slice(1), 10) - 1
        values.push(params?.[paramIndex] ?? null)
      }
      // Handle string literals
      else if (part.startsWith("'") && part.endsWith("'")) {
        values.push(part.slice(1, -1))
      }
      // Handle numbers
      else if (!isNaN(Number(part))) {
        values.push(Number(part))
      }
      // Handle NULL
      else if (part.toUpperCase() === 'NULL') {
        values.push(null)
      }
      // Default to string
      else {
        values.push(part)
      }
    }

    return values
  }

  /** Parse SET clause for UPDATE */
  protected parseSetClause(setClause: string, params?: unknown[]): Record<string, unknown> {
    const updates: Record<string, unknown> = {}
    const assignments = setClause.split(',')

    for (const assignment of assignments) {
      const match = assignment.match(/["`]?(\w+)["`]?\s*=\s*(.+)/)
      if (match) {
        const [, field, valueStr] = match
        const trimmedValue = valueStr.trim()

        if (trimmedValue.match(/^\$(\d+)$/)) {
          const paramIndex = parseInt(trimmedValue.slice(1), 10) - 1
          updates[field] = params?.[paramIndex] ?? null
        } else if (trimmedValue.startsWith("'") && trimmedValue.endsWith("'")) {
          updates[field] = trimmedValue.slice(1, -1)
        } else if (!isNaN(Number(trimmedValue))) {
          updates[field] = Number(trimmedValue)
        } else {
          updates[field] = trimmedValue
        }
      }
    }

    return updates
  }

  /** Apply WHERE clause filtering */
  protected applyWhereClause(rows: Row[], whereClause: string, params?: unknown[]): Row[] {
    // Simple WHERE clause parsing (supports = and AND)
    const conditions = whereClause.split(/\s+AND\s+/i)

    return rows.filter(row => {
      for (const condition of conditions) {
        const match = condition.match(/["`]?(\w+)["`]?\s*=\s*(.+)/)
        if (match) {
          const [, field, valueStr] = match
          const trimmedValue = valueStr.trim()

          let compareValue: unknown
          if (trimmedValue.match(/^\$(\d+)$/)) {
            const paramIndex = parseInt(trimmedValue.slice(1), 10) - 1
            compareValue = params?.[paramIndex]
          } else if (trimmedValue.startsWith("'") && trimmedValue.endsWith("'")) {
            compareValue = trimmedValue.slice(1, -1)
          } else if (!isNaN(Number(trimmedValue))) {
            compareValue = Number(trimmedValue)
          } else {
            compareValue = trimmedValue
          }

          if (row[field] !== compareValue) {
            return false
          }
        }
      }
      return true
    })
  }

  /** Apply GROUP BY clause */
  protected applyGroupBy(rows: Row[], sql: string, groupCol: string): Row[] {
    const groups = new Map<unknown, Row[]>()

    for (const row of rows) {
      const key = row[groupCol]
      if (!groups.has(key)) {
        groups.set(key, [])
      }
      groups.get(key)!.push(row)
    }

    const result: Row[] = []
    for (const [key, groupRows] of Array.from(groups.entries())) {
      const aggregatedRow: Row = { [groupCol]: key }

      // Handle COUNT(*)
      if (sql.toUpperCase().includes('COUNT(*)') || sql.toUpperCase().includes('COUNT()')) {
        aggregatedRow.count = groupRows.length
      }

      // Handle AVG
      const avgMatch = sql.match(/AVG\((\w+)\)/i)
      if (avgMatch) {
        const col = avgMatch[1]
        const sum = groupRows.reduce((acc, r) => acc + (Number(r[col]) || 0), 0)
        aggregatedRow.avg_value = sum / groupRows.length
      }

      // Handle SUM
      const sumMatch = sql.match(/SUM\((\w+)\)/i)
      if (sumMatch) {
        const col = sumMatch[1]
        aggregatedRow.sum = groupRows.reduce((acc, r) => acc + (Number(r[col]) || 0), 0)
      }

      result.push(aggregatedRow)
    }

    return result
  }

  /** Simulate query latency based on complexity */
  protected async simulateQueryLatency(sql: string): Promise<void> {
    // Base latency varies by database type (overridden in subclasses)
    const baseLatency = this.getBaseQueryLatency()

    // Add complexity-based latency
    let multiplier = 1
    if (sql.toUpperCase().includes('JOIN')) multiplier += 0.5
    if (sql.toUpperCase().includes('GROUP BY')) multiplier += 0.3
    if (sql.toUpperCase().includes('ORDER BY')) multiplier += 0.2

    await this.sleep(baseLatency * multiplier)
  }

  /** Simulate execute latency */
  protected async simulateExecuteLatency(sql: string): Promise<void> {
    const baseLatency = this.getBaseExecuteLatency()
    await this.sleep(baseLatency)
  }

  /** Get base query latency (override in subclasses) */
  protected getBaseQueryLatency(): number {
    return 0.5 // Default 0.5ms
  }

  /** Get base execute latency (override in subclasses) */
  protected getBaseExecuteLatency(): number {
    return 1 // Default 1ms
  }

  /** Helper: Create JSON response */
  protected jsonResponse(data: unknown): Response {
    return new Response(JSON.stringify(data), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  /** Helper: Sleep */
  protected sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms))
  }
}

// =============================================================================
// PostgreSQL Container DO
// =============================================================================

/**
 * Simulated PostgreSQL container DO.
 * Emulates PostgreSQL's behavior with realistic latency characteristics.
 */
export class PostgresContainerDO extends BaseContainerDO {
  protected getDatabaseType(): string {
    return 'PostgreSQL'
  }

  protected getSimulatedStartupDelay(): number {
    // PostgreSQL typically takes 1-3 seconds to start
    return 50 + Math.random() * 100 // Simulated: 50-150ms
  }

  protected getBaseQueryLatency(): number {
    return 0.3 + Math.random() * 0.4 // 0.3-0.7ms
  }

  protected getBaseExecuteLatency(): number {
    return 0.5 + Math.random() * 0.5 // 0.5-1ms
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)

    // PostgreSQL-specific endpoints
    if (url.pathname === '/transaction/begin') {
      return this.jsonResponse({ status: 'ok', transaction: 'started' })
    }
    if (url.pathname === '/transaction/commit') {
      return this.jsonResponse({ status: 'ok', transaction: 'committed' })
    }
    if (url.pathname === '/transaction/rollback') {
      return this.jsonResponse({ status: 'ok', transaction: 'rolled_back' })
    }
    if (url.pathname === '/admin/databases') {
      return this.jsonResponse({ databases: ['postgres', 'template0', 'template1'] })
    }

    return super.fetch(request)
  }
}

// =============================================================================
// ClickHouse Container DO
// =============================================================================

/**
 * Simulated ClickHouse container DO.
 * Emulates ClickHouse's analytical query characteristics.
 */
export class ClickHouseContainerDO extends BaseContainerDO {
  protected getDatabaseType(): string {
    return 'ClickHouse'
  }

  protected getSimulatedStartupDelay(): number {
    // ClickHouse typically takes 2-5 seconds to start
    return 100 + Math.random() * 150 // Simulated: 100-250ms
  }

  protected getBaseQueryLatency(): number {
    // ClickHouse is fast for analytical queries
    return 0.2 + Math.random() * 0.3 // 0.2-0.5ms
  }

  protected getBaseExecuteLatency(): number {
    // Inserts are batched, so slightly slower
    return 1 + Math.random() * 1 // 1-2ms
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)

    // ClickHouse-specific endpoint for native HTTP interface
    if (url.pathname === '/' && request.method === 'POST') {
      const sql = await request.text()
      const result = await this.handleQuery(sql, [])
      return this.jsonResponse({
        data: result.rows,
        rows: result.rowCount,
        meta: [],
        statistics: {
          elapsed: 0.001,
          rows_read: result.rowCount,
          bytes_read: 0,
        },
      })
    }

    return super.fetch(request)
  }
}

// =============================================================================
// MongoDB Container DO
// =============================================================================

/**
 * Simulated MongoDB container DO.
 * Emulates MongoDB's document database behavior.
 */
export class MongoContainerDO extends BaseContainerDO {
  private collections: Map<string, Row[]> = new Map()

  protected getDatabaseType(): string {
    return 'MongoDB'
  }

  protected getSimulatedStartupDelay(): number {
    // MongoDB typically takes 1-2 seconds to start
    return 75 + Math.random() * 100 // Simulated: 75-175ms
  }

  protected getBaseQueryLatency(): number {
    return 0.4 + Math.random() * 0.4 // 0.4-0.8ms
  }

  protected getBaseExecuteLatency(): number {
    return 0.6 + Math.random() * 0.6 // 0.6-1.2ms
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)
    const path = url.pathname

    try {
      // MongoDB-specific endpoints
      if (path === '/find' && request.method === 'POST') {
        const body = await request.json() as {
          collection: string
          filter?: Record<string, unknown>
          sort?: Record<string, 1 | -1>
          limit?: number
        }
        const result = await this.mongoFind(body.collection, body.filter, body)
        return this.jsonResponse({ documents: result })
      }

      if (path === '/findOne' && request.method === 'POST') {
        const body = await request.json() as { collection: string; filter?: Record<string, unknown> }
        const results = await this.mongoFind(body.collection, body.filter, { limit: 1 })
        return this.jsonResponse({ document: results[0] || null })
      }

      if (path === '/insertOne' && request.method === 'POST') {
        const body = await request.json() as { collection: string; document: Record<string, unknown> }
        const result = await this.mongoInsertOne(body.collection, body.document)
        return this.jsonResponse(result)
      }

      if (path === '/insertMany' && request.method === 'POST') {
        const body = await request.json() as { collection: string; documents: Record<string, unknown>[] }
        const result = await this.mongoInsertMany(body.collection, body.documents)
        return this.jsonResponse(result)
      }

      if (path === '/updateOne' && request.method === 'POST') {
        const body = await request.json() as {
          collection: string
          filter: Record<string, unknown>
          update: Record<string, unknown>
        }
        const result = await this.mongoUpdateOne(body.collection, body.filter, body.update)
        return this.jsonResponse(result)
      }

      if (path === '/deleteMany' && request.method === 'POST') {
        const body = await request.json() as { collection: string; filter: Record<string, unknown> }
        const result = await this.mongoDeleteMany(body.collection, body.filter)
        return this.jsonResponse(result)
      }

      if (path === '/aggregate' && request.method === 'POST') {
        const body = await request.json() as {
          collection: string
          pipeline: Record<string, unknown>[]
        }
        const result = await this.mongoAggregate(body.collection, body.pipeline)
        return this.jsonResponse({ documents: result })
      }

      if (path === '/count' && request.method === 'POST') {
        const body = await request.json() as { collection: string; filter?: Record<string, unknown> }
        const docs = await this.mongoFind(body.collection, body.filter)
        return this.jsonResponse({ count: docs.length })
      }

      if (path === '/listCollections' && request.method === 'POST') {
        return this.jsonResponse({ collections: Array.from(this.collections.keys()) })
      }

      return super.fetch(request)
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      return new Response(JSON.stringify({ error: message }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      })
    }
  }

  private async mongoFind(
    collection: string,
    filter?: Record<string, unknown>,
    options?: { sort?: Record<string, 1 | -1>; limit?: number }
  ): Promise<Row[]> {
    await this.simulateQueryLatency('SELECT')
    this.metrics.totalQueries++

    const coll = this.collections.get(collection) || []
    let results = filter ? coll.filter(doc => this.matchesFilter(doc, filter)) : [...coll]

    if (options?.sort) {
      const [sortField, sortDir] = Object.entries(options.sort)[0]
      results.sort((a, b) => {
        const aVal = a[sortField] as string | number | null
        const bVal = b[sortField] as string | number | null
        if (aVal === bVal) return 0
        if (aVal === null || aVal === undefined) return 1
        if (bVal === null || bVal === undefined) return -1
        const cmp = aVal < bVal ? -1 : 1
        return sortDir === -1 ? -cmp : cmp
      })
    }

    if (options?.limit) {
      results = results.slice(0, options.limit)
    }

    return results
  }

  private async mongoInsertOne(
    collection: string,
    document: Record<string, unknown>
  ): Promise<{ acknowledged: boolean; insertedId?: string; insertedCount: number }> {
    await this.simulateExecuteLatency('INSERT')
    this.metrics.totalExecutes++

    if (!this.collections.has(collection)) {
      this.collections.set(collection, [])
    }

    const id = (document._id as string) || crypto.randomUUID()
    const doc = { ...document, _id: id }
    this.collections.get(collection)!.push(doc)

    return { acknowledged: true, insertedId: id, insertedCount: 1 }
  }

  private async mongoInsertMany(
    collection: string,
    documents: Record<string, unknown>[]
  ): Promise<{ acknowledged: boolean; insertedCount: number }> {
    await this.simulateExecuteLatency('INSERT')
    this.metrics.totalExecutes++

    if (!this.collections.has(collection)) {
      this.collections.set(collection, [])
    }

    const coll = this.collections.get(collection)!
    for (const document of documents) {
      const id = (document._id as string) || crypto.randomUUID()
      coll.push({ ...document, _id: id })
    }

    return { acknowledged: true, insertedCount: documents.length }
  }

  private async mongoUpdateOne(
    collection: string,
    filter: Record<string, unknown>,
    update: Record<string, unknown>
  ): Promise<{ acknowledged: boolean; matchedCount: number; modifiedCount: number }> {
    await this.simulateExecuteLatency('UPDATE')
    this.metrics.totalExecutes++

    const coll = this.collections.get(collection) || []
    const doc = coll.find(d => this.matchesFilter(d, filter))

    if (doc) {
      const $set = update.$set as Record<string, unknown> | undefined
      if ($set) {
        Object.assign(doc, $set)
      }
      return { acknowledged: true, matchedCount: 1, modifiedCount: 1 }
    }

    return { acknowledged: true, matchedCount: 0, modifiedCount: 0 }
  }

  private async mongoDeleteMany(
    collection: string,
    filter: Record<string, unknown>
  ): Promise<{ acknowledged: boolean; deletedCount: number }> {
    await this.simulateExecuteLatency('DELETE')
    this.metrics.totalExecutes++

    const coll = this.collections.get(collection)
    if (!coll) {
      return { acknowledged: true, deletedCount: 0 }
    }

    const initialLength = coll.length
    const filtered = coll.filter(doc => !this.matchesFilter(doc, filter))
    this.collections.set(collection, filtered)

    return { acknowledged: true, deletedCount: initialLength - filtered.length }
  }

  private async mongoAggregate(
    collection: string,
    pipeline: Record<string, unknown>[]
  ): Promise<Row[]> {
    await this.simulateQueryLatency('SELECT GROUP BY')
    this.metrics.totalQueries++

    let results = this.collections.get(collection) || []

    for (const stage of pipeline) {
      if (stage.$match) {
        results = results.filter(doc => this.matchesFilter(doc, stage.$match as Record<string, unknown>))
      }
      if (stage.$group) {
        const groupSpec = stage.$group as Record<string, unknown>
        const groupKey = groupSpec._id as string
        const groups = new Map<unknown, Row[]>()

        for (const doc of results) {
          const keyValue = groupKey.startsWith('$') ? doc[groupKey.slice(1)] : groupKey
          if (!groups.has(keyValue)) {
            groups.set(keyValue, [])
          }
          groups.get(keyValue)!.push(doc)
        }

        results = []
        for (const [key, groupDocs] of Array.from(groups.entries())) {
          const aggregated: Row = { _id: key }

          for (const [field, spec] of Object.entries(groupSpec)) {
            if (field === '_id') continue
            const specObj = spec as Record<string, unknown>

            if (specObj.$sum !== undefined) {
              aggregated[field] = specObj.$sum === 1
                ? groupDocs.length
                : groupDocs.reduce((acc, d) => acc + (Number(d[(specObj.$sum as string).slice(1)]) || 0), 0)
            }
            if (specObj.$avg !== undefined) {
              const col = (specObj.$avg as string).slice(1)
              aggregated[field] = groupDocs.reduce((acc, d) => acc + (Number(d[col]) || 0), 0) / groupDocs.length
            }
          }

          results.push(aggregated)
        }
      }
    }

    return results
  }

  private matchesFilter(doc: Row, filter: Record<string, unknown>): boolean {
    for (const [key, value] of Object.entries(filter)) {
      if (doc[key] !== value) {
        return false
      }
    }
    return true
  }
}

// =============================================================================
// DuckDB Container DO
// =============================================================================

/**
 * Simulated DuckDB container DO.
 * Emulates DuckDB's analytical query characteristics.
 */
export class DuckDBContainerDO extends BaseContainerDO {
  protected getDatabaseType(): string {
    return 'DuckDB'
  }

  protected getSimulatedStartupDelay(): number {
    // DuckDB is very fast to start
    return 20 + Math.random() * 50 // Simulated: 20-70ms
  }

  protected getBaseQueryLatency(): number {
    // DuckDB is very fast for analytical queries
    return 0.1 + Math.random() * 0.2 // 0.1-0.3ms
  }

  protected getBaseExecuteLatency(): number {
    return 0.3 + Math.random() * 0.3 // 0.3-0.6ms
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)

    // DuckDB-specific endpoints
    if (url.pathname === '/export' && request.method === 'POST') {
      const body = await request.json() as { query: string; format: string }
      const result = await this.handleQuery(body.query, [])
      return this.jsonResponse({
        data: result.rows,
        format: body.format,
        rowCount: result.rowCount,
      })
    }

    return super.fetch(request)
  }
}

// =============================================================================
// SQLite Container DO
// =============================================================================

/**
 * Simulated SQLite container DO.
 * Emulates SQLite's lightweight database behavior.
 */
export class SQLiteContainerDO extends BaseContainerDO {
  protected getDatabaseType(): string {
    return 'SQLite'
  }

  protected getSimulatedStartupDelay(): number {
    // SQLite is very fast to start
    return 10 + Math.random() * 30 // Simulated: 10-40ms
  }

  protected getBaseQueryLatency(): number {
    return 0.2 + Math.random() * 0.3 // 0.2-0.5ms
  }

  protected getBaseExecuteLatency(): number {
    return 0.4 + Math.random() * 0.4 // 0.4-0.8ms
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)

    // SQLite-specific endpoints
    if (url.pathname === '/backup' && request.method === 'POST') {
      return this.jsonResponse({ status: 'ok', message: 'Backup not implemented in simulation' })
    }

    return super.fetch(request)
  }
}
