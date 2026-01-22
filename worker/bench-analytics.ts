/**
 * Analytics Benchmark Worker - ClickBench Suite
 *
 * Cloudflare Worker that runs the ClickBench 43 queries against analytical databases.
 * Uses an in-memory mock database for benchmarking query patterns.
 *
 * Benchmarks:
 * - Full table scans
 * - Aggregations (COUNT, SUM, AVG, MIN, MAX)
 * - Complex GROUP BY with ORDER BY
 * - High-cardinality string operations
 *
 * Endpoint: POST /benchmark/analytics
 * Query params: ?database=sqlite&dataset=clickbench
 *
 * @see https://github.com/ClickHouse/ClickBench
 * @see datasets/analytics/clickbench.ts
 *
 * NOTE: Database Availability
 * - SQLite: Uses InMemoryDatabase (JavaScript mock with basic SQL support)
 * - PostgreSQL: DISABLED - @dotdo/pglite not available in this workspace
 * - DuckDB: DISABLED - Requires complex WASM setup
 */

import { Hono } from 'hono'

// ============================================================================
// Types
// ============================================================================

type BenchmarkEnvironment = 'worker' | 'do' | 'container' | 'local'

interface BenchmarkResult {
  benchmark: string
  database: string
  dataset: string
  p50_ms: number
  p99_ms: number
  min_ms: number
  max_ms: number
  mean_ms: number
  stddev_ms?: number
  ops_per_sec: number
  iterations: number
  total_duration_ms?: number
  vfs_reads: number
  vfs_writes: number
  vfs_bytes_read: number
  vfs_bytes_written: number
  timestamp: string
  environment: BenchmarkEnvironment
  colo?: string
  run_id: string
}

// Only sqlite is currently available
type DatabaseType = 'sqlite'
type DatasetType = 'clickbench' | 'imdb'
type QueryComplexity = 'simple' | 'moderate' | 'complex' | 'expert'

interface BenchmarkQuery {
  id: string
  name: string
  description: string
  complexity: QueryComplexity
  sql: string
  benchmarks: string[]
}

interface QueryTiming {
  queryId: string
  queryName: string
  complexity: QueryComplexity
  iterations: number
  times: number[]
  stats: {
    min: number
    max: number
    mean: number
    p50: number
    p99: number
    stddev: number
  }
  rowCount?: number
  error?: string
}

interface AnalyticsBenchmarkResults {
  runId: string
  timestamp: string
  environment: BenchmarkEnvironment
  colo?: string
  database: DatabaseType
  dataset: DatasetType
  queryResults: QueryTiming[]
  summary: {
    totalDurationMs: number
    totalQueries: number
    successfulQueries: number
    failedQueries: number
    simpleAvgMs: number
    moderateAvgMs: number
    complexAvgMs: number
    expertAvgMs: number
  }
}

// Cloudflare Workers types - Using inline declarations for portability
interface R2Bucket {
  get(key: string): Promise<R2Object | null>
  put(key: string, value: string | ArrayBuffer | ReadableStream): Promise<R2Object>
  list(options?: { prefix?: string; limit?: number }): Promise<{ objects: R2Object[]; truncated: boolean }>
}

interface R2Object {
  key: string
  size: number
  uploaded: Date
  text(): Promise<string>
}

interface Env {
  // R2 bucket containing analytics datasets
  ANALYTICS_BUCKET: R2Bucket
  // R2 bucket for storing benchmark results
  RESULTS: R2Bucket
}

// ============================================================================
// In-Memory Database Mock
// ============================================================================

/**
 * Simple in-memory database for analytics benchmarking.
 * Provides basic SQL support for SELECT queries with aggregations.
 *
 * Supported operations:
 * - SELECT with COUNT(*), SUM, AVG, MIN, MAX
 * - WHERE with =, <>, <, >, <=, >=, LIKE, AND, OR
 * - GROUP BY (single column)
 * - ORDER BY with ASC/DESC
 * - LIMIT
 *
 * Limitations:
 * - No JOINs
 * - No subqueries
 * - No window functions
 * - Single GROUP BY column only
 */
class InMemoryDatabase {
  private rows: Record<string, unknown>[] = []
  private columns: string[] = []

  /**
   * Initialize database with schema and sample data
   */
  initialize(schema: string[], data: Record<string, unknown>[]): void {
    this.columns = schema
    this.rows = data
  }

  /**
   * Execute a SQL query and return results
   */
  execute(sql: string): { columns: string[]; rows: unknown[][]; rowCount: number } {
    const normalizedSql = sql.trim().replace(/\s+/g, ' ')
    const upperSql = normalizedSql.toUpperCase()

    // Handle simple COUNT(*)
    if (upperSql.includes('COUNT(*)') && !upperSql.includes('GROUP BY')) {
      const whereMatch = normalizedSql.match(/WHERE\s+(.+?)(?:\s+ORDER|\s+LIMIT|\s*$)/i)
      let filteredRows = this.rows
      if (whereMatch) {
        filteredRows = this.filterRows(filteredRows, whereMatch[1])
      }
      return {
        columns: ['COUNT(*)'],
        rows: [[filteredRows.length]],
        rowCount: 1,
      }
    }

    // Handle COUNT(DISTINCT column)
    const countDistinctMatch = upperSql.match(/COUNT\s*\(\s*DISTINCT\s+(\w+)\s*\)/i)
    if (countDistinctMatch && !upperSql.includes('GROUP BY')) {
      const col = countDistinctMatch[1]
      const whereMatch = normalizedSql.match(/WHERE\s+(.+?)(?:\s+ORDER|\s+LIMIT|\s*$)/i)
      let filteredRows = this.rows
      if (whereMatch) {
        filteredRows = this.filterRows(filteredRows, whereMatch[1])
      }
      const uniqueValues = new Set(filteredRows.map((r) => r[col]))
      return {
        columns: [`COUNT(DISTINCT ${col})`],
        rows: [[uniqueValues.size]],
        rowCount: 1,
      }
    }

    // Handle simple aggregations without GROUP BY
    if (this.hasAggregation(upperSql) && !upperSql.includes('GROUP BY')) {
      return this.executeSimpleAggregation(normalizedSql)
    }

    // Handle GROUP BY queries
    if (upperSql.includes('GROUP BY')) {
      return this.executeGroupBy(normalizedSql)
    }

    // Handle simple SELECT
    return this.executeSimpleSelect(normalizedSql)
  }

  private hasAggregation(sql: string): boolean {
    return /\b(COUNT|SUM|AVG|MIN|MAX)\s*\(/i.test(sql)
  }

  private executeSimpleAggregation(sql: string): { columns: string[]; rows: unknown[][]; rowCount: number } {
    const whereMatch = sql.match(/WHERE\s+(.+?)(?:\s+ORDER|\s+LIMIT|\s*$)/i)
    let filteredRows = this.rows
    if (whereMatch) {
      filteredRows = this.filterRows(filteredRows, whereMatch[1])
    }

    // Parse SELECT clause
    const selectMatch = sql.match(/SELECT\s+(.+?)\s+FROM/i)
    if (!selectMatch) {
      throw new Error(`Invalid SELECT syntax: ${sql}`)
    }

    const selectExpressions = this.parseSelectExpressions(selectMatch[1])
    const resultRow: unknown[] = []
    const resultColumns: string[] = []

    for (const expr of selectExpressions) {
      const { value, alias } = this.evaluateAggregation(expr, filteredRows)
      resultRow.push(value)
      resultColumns.push(alias)
    }

    return {
      columns: resultColumns,
      rows: [resultRow],
      rowCount: 1,
    }
  }

  private parseSelectExpressions(selectClause: string): string[] {
    // Simple comma split (doesn't handle nested functions well)
    const expressions: string[] = []
    let depth = 0
    let current = ''

    for (const char of selectClause) {
      if (char === '(') depth++
      else if (char === ')') depth--
      else if (char === ',' && depth === 0) {
        expressions.push(current.trim())
        current = ''
        continue
      }
      current += char
    }
    if (current.trim()) {
      expressions.push(current.trim())
    }

    return expressions
  }

  private evaluateAggregation(
    expr: string,
    rows: Record<string, unknown>[]
  ): { value: unknown; alias: string } {
    const trimmed = expr.trim()

    // COUNT(*)
    if (/COUNT\s*\(\s*\*\s*\)/i.test(trimmed)) {
      return { value: rows.length, alias: 'COUNT(*)' }
    }

    // COUNT(DISTINCT column)
    const countDistinctMatch = trimmed.match(/COUNT\s*\(\s*DISTINCT\s+(\w+)\s*\)/i)
    if (countDistinctMatch) {
      const col = countDistinctMatch[1]
      const unique = new Set(rows.map((r) => r[col]))
      return { value: unique.size, alias: `COUNT(DISTINCT ${col})` }
    }

    // SUM(column)
    const sumMatch = trimmed.match(/SUM\s*\(\s*(\w+)\s*\)/i)
    if (sumMatch) {
      const col = sumMatch[1]
      const sum = rows.reduce((acc, r) => acc + (Number(r[col]) || 0), 0)
      return { value: sum, alias: `SUM(${col})` }
    }

    // AVG(column)
    const avgMatch = trimmed.match(/AVG\s*\(\s*(\w+)\s*\)/i)
    if (avgMatch) {
      const col = avgMatch[1]
      const sum = rows.reduce((acc, r) => acc + (Number(r[col]) || 0), 0)
      const avg = rows.length > 0 ? sum / rows.length : 0
      return { value: avg, alias: `AVG(${col})` }
    }

    // MIN(column)
    const minMatch = trimmed.match(/MIN\s*\(\s*(\w+)\s*\)/i)
    if (minMatch) {
      const col = minMatch[1]
      const values = rows.map((r) => r[col]).filter((v) => v !== null && v !== undefined)
      const min = values.length > 0 ? Math.min(...values.map(Number)) : null
      return { value: min, alias: `MIN(${col})` }
    }

    // MAX(column)
    const maxMatch = trimmed.match(/MAX\s*\(\s*(\w+)\s*\)/i)
    if (maxMatch) {
      const col = maxMatch[1]
      const values = rows.map((r) => r[col]).filter((v) => v !== null && v !== undefined)
      const max = values.length > 0 ? Math.max(...values.map(Number)) : null
      return { value: max, alias: `MAX(${col})` }
    }

    // COUNT(column) - count non-null values
    const countMatch = trimmed.match(/COUNT\s*\(\s*(\w+)\s*\)/i)
    if (countMatch) {
      const col = countMatch[1]
      const count = rows.filter((r) => r[col] !== null && r[col] !== undefined).length
      return { value: count, alias: `COUNT(${col})` }
    }

    // Plain column reference
    return { value: null, alias: trimmed }
  }

  private executeGroupBy(sql: string): { columns: string[]; rows: unknown[][]; rowCount: number } {
    // Parse GROUP BY column
    const groupByMatch = sql.match(/GROUP\s+BY\s+(\w+)/i)
    if (!groupByMatch) {
      throw new Error(`Invalid GROUP BY: ${sql}`)
    }
    const groupCol = groupByMatch[1]

    // Parse WHERE clause
    const whereMatch = sql.match(/WHERE\s+(.+?)(?:\s+GROUP)/i)
    let filteredRows = this.rows
    if (whereMatch) {
      filteredRows = this.filterRows(filteredRows, whereMatch[1])
    }

    // Parse SELECT expressions
    const selectMatch = sql.match(/SELECT\s+(.+?)\s+FROM/i)
    if (!selectMatch) {
      throw new Error(`Invalid SELECT: ${sql}`)
    }
    const selectExpressions = this.parseSelectExpressions(selectMatch[1])

    // Group rows
    const groups = new Map<unknown, Record<string, unknown>[]>()
    for (const row of filteredRows) {
      const key = row[groupCol]
      if (!groups.has(key)) {
        groups.set(key, [])
      }
      groups.get(key)!.push(row)
    }

    // Calculate aggregations for each group
    const resultRows: unknown[][] = []
    const resultColumns: string[] = []
    let columnsSet = false

    for (const [groupKey, groupRows] of Array.from(groups.entries())) {
      const resultRow: unknown[] = []

      for (const expr of selectExpressions) {
        const trimmed = expr.trim()

        // Check if it's the group column
        if (trimmed.toUpperCase() === groupCol.toUpperCase()) {
          resultRow.push(groupKey)
          if (!columnsSet) resultColumns.push(groupCol)
        } else {
          // Evaluate aggregation
          const { value, alias } = this.evaluateAggregation(trimmed, groupRows)
          resultRow.push(value)
          if (!columnsSet) resultColumns.push(alias)
        }
      }

      columnsSet = true
      resultRows.push(resultRow)
    }

    // Parse ORDER BY
    const orderByMatch = sql.match(/ORDER\s+BY\s+(\w+)(?:\s+(ASC|DESC))?/i)
    if (orderByMatch) {
      const orderCol = orderByMatch[1]
      const orderDir = (orderByMatch[2] || 'ASC').toUpperCase()
      const colIndex = resultColumns.findIndex(
        (c) => c.toUpperCase() === orderCol.toUpperCase() || c.toUpperCase().includes(orderCol.toUpperCase())
      )
      if (colIndex >= 0) {
        resultRows.sort((a, b) => {
          const aVal = a[colIndex]
          const bVal = b[colIndex]
          const aNum = typeof aVal === 'number' ? aVal : Number(aVal) || 0
          const bNum = typeof bVal === 'number' ? bVal : Number(bVal) || 0
          return orderDir === 'DESC' ? bNum - aNum : aNum - bNum
        })
      }
    }

    // Parse LIMIT
    const limitMatch = sql.match(/LIMIT\s+(\d+)/i)
    const limit = limitMatch ? parseInt(limitMatch[1], 10) : resultRows.length

    return {
      columns: resultColumns,
      rows: resultRows.slice(0, limit),
      rowCount: Math.min(resultRows.length, limit),
    }
  }

  private executeSimpleSelect(sql: string): { columns: string[]; rows: unknown[][]; rowCount: number } {
    // Parse WHERE
    const whereMatch = sql.match(/WHERE\s+(.+?)(?:\s+ORDER|\s+GROUP|\s+LIMIT|\s*$)/i)
    let filteredRows = this.rows
    if (whereMatch) {
      filteredRows = this.filterRows(filteredRows, whereMatch[1])
    }

    // Parse SELECT columns
    const selectMatch = sql.match(/SELECT\s+(.+?)\s+FROM/i)
    if (!selectMatch) {
      throw new Error(`Invalid SELECT: ${sql}`)
    }

    let selectedColumns: string[]
    if (selectMatch[1].trim() === '*') {
      selectedColumns = this.columns
    } else {
      selectedColumns = selectMatch[1].split(',').map((c) => c.trim())
    }

    // Build result rows
    const resultRows = filteredRows.map((row) =>
      selectedColumns.map((col) => row[col])
    )

    // Parse ORDER BY
    const orderByMatch = sql.match(/ORDER\s+BY\s+(\w+)(?:\s+(ASC|DESC))?/i)
    if (orderByMatch) {
      const orderCol = orderByMatch[1]
      const orderDir = (orderByMatch[2] || 'ASC').toUpperCase()
      const colIndex = selectedColumns.indexOf(orderCol)
      if (colIndex >= 0) {
        resultRows.sort((a, b) => {
          const aVal = a[colIndex]
          const bVal = b[colIndex]
          if (typeof aVal === 'number' && typeof bVal === 'number') {
            return orderDir === 'DESC' ? bVal - aVal : aVal - bVal
          }
          return orderDir === 'DESC'
            ? String(bVal).localeCompare(String(aVal))
            : String(aVal).localeCompare(String(bVal))
        })
      }
    }

    // Parse LIMIT
    const limitMatch = sql.match(/LIMIT\s+(\d+)/i)
    const limit = limitMatch ? parseInt(limitMatch[1], 10) : resultRows.length

    return {
      columns: selectedColumns,
      rows: resultRows.slice(0, limit),
      rowCount: Math.min(resultRows.length, limit),
    }
  }

  private filterRows(rows: Record<string, unknown>[], whereClause: string): Record<string, unknown>[] {
    return rows.filter((row) => this.evaluateCondition(row, whereClause))
  }

  private evaluateCondition(row: Record<string, unknown>, condition: string): boolean {
    const trimmed = condition.trim()

    // Handle OR
    if (/\bOR\b/i.test(trimmed)) {
      const parts = trimmed.split(/\s+OR\s+/i)
      return parts.some((part) => this.evaluateCondition(row, part))
    }

    // Handle AND
    if (/\bAND\b/i.test(trimmed)) {
      const parts = trimmed.split(/\s+AND\s+/i)
      return parts.every((part) => this.evaluateCondition(row, part))
    }

    // Handle NOT
    if (trimmed.toUpperCase().startsWith('NOT ')) {
      return !this.evaluateCondition(row, trimmed.slice(4))
    }

    // Handle LIKE
    const likeMatch = trimmed.match(/(\w+)\s+LIKE\s+'([^']+)'/i)
    if (likeMatch) {
      const col = likeMatch[1]
      const pattern = likeMatch[2]
      const value = String(row[col] || '')
      const regex = new RegExp('^' + pattern.replace(/%/g, '.*') + '$', 'i')
      return regex.test(value)
    }

    // Handle comparisons: >=, <=, <>, !=, =, <, >
    const comparisonMatch = trimmed.match(/(\w+)\s*(>=|<=|<>|!=|=|<|>)\s*(.+)/)
    if (comparisonMatch) {
      const col = comparisonMatch[1]
      const op = comparisonMatch[2]
      let compareValue: unknown = comparisonMatch[3].trim()

      // Parse compare value
      if (compareValue === "''") {
        compareValue = ''
      } else if (typeof compareValue === 'string' && compareValue.startsWith("'") && compareValue.endsWith("'")) {
        compareValue = compareValue.slice(1, -1)
      } else if (!isNaN(Number(compareValue))) {
        compareValue = Number(compareValue)
      }

      const rowValue = row[col]

      switch (op) {
        case '=':
          return rowValue === compareValue
        case '<>':
        case '!=':
          return rowValue !== compareValue
        case '<':
          return Number(rowValue) < Number(compareValue)
        case '>':
          return Number(rowValue) > Number(compareValue)
        case '<=':
          return Number(rowValue) <= Number(compareValue)
        case '>=':
          return Number(rowValue) >= Number(compareValue)
      }
    }

    // Default: return true (no condition)
    return true
  }
}

// ============================================================================
// ClickBench 43 Queries
// ============================================================================

const CLICKBENCH_QUERIES: BenchmarkQuery[] = [
  // Simple aggregations (Q0-Q9)
  {
    id: 'q0',
    name: 'Count all rows',
    description: 'Simple full table scan and count',
    complexity: 'simple',
    sql: 'SELECT COUNT(*) FROM hits',
    benchmarks: ['scan-speed', 'row-counting'],
  },
  {
    id: 'q1',
    name: 'Count with single filter',
    description: 'Filter on AdvEngineID with count',
    complexity: 'simple',
    sql: "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0",
    benchmarks: ['filter-performance', 'predicate-pushdown'],
  },
  {
    id: 'q2',
    name: 'Sum with filter',
    description: 'Aggregate sum with filter condition',
    complexity: 'simple',
    sql: "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits",
    benchmarks: ['aggregation-speed', 'column-read'],
  },
  {
    id: 'q3',
    name: 'Average with filter',
    description: 'Average calculation with predicate',
    complexity: 'simple',
    sql: "SELECT AVG(UserID) FROM hits WHERE UserID <> 0",
    benchmarks: ['numeric-aggregation', 'null-handling'],
  },
  {
    id: 'q4',
    name: 'Distinct count',
    description: 'Count distinct users',
    complexity: 'simple',
    sql: "SELECT COUNT(DISTINCT UserID) FROM hits",
    benchmarks: ['distinct-count', 'hash-aggregation'],
  },
  {
    id: 'q5',
    name: 'Distinct count with filter',
    description: 'Count distinct with predicate',
    complexity: 'simple',
    sql: "SELECT COUNT(DISTINCT SearchPhrase) FROM hits WHERE SearchPhrase <> ''",
    benchmarks: ['string-distinct', 'filter-then-aggregate'],
  },
  {
    id: 'q6',
    name: 'Min/Max/Count',
    description: 'Multiple aggregates in single query',
    complexity: 'simple',
    sql: "SELECT MIN(EventDate), MAX(EventDate), COUNT(*) FROM hits",
    benchmarks: ['multi-aggregate', 'date-handling'],
  },
  {
    id: 'q7',
    name: 'Sum integer column',
    description: 'Sum of large integer column',
    complexity: 'simple',
    sql: "SELECT SUM(RegionID) FROM hits",
    benchmarks: ['integer-sum', 'vectorized-aggregation'],
  },
  {
    id: 'q8',
    name: 'Count filter on string empty',
    description: 'String empty check performance',
    complexity: 'simple',
    sql: "SELECT COUNT(*) FROM hits WHERE URL <> ''",
    benchmarks: ['string-predicate', 'empty-check'],
  },
  {
    id: 'q9',
    name: 'Count filter on integer range',
    description: 'Integer range filter',
    complexity: 'simple',
    sql: "SELECT COUNT(*) FROM hits WHERE RegionID >= 100 AND RegionID <= 150",
    benchmarks: ['range-filter', 'integer-comparison'],
  },

  // Group by queries (Q10-Q19)
  {
    id: 'q10',
    name: 'Group by small cardinality',
    description: 'Group by column with few distinct values',
    complexity: 'moderate',
    sql: "SELECT RegionID, COUNT(*) FROM hits GROUP BY RegionID",
    benchmarks: ['group-by', 'hash-table-small'],
  },
  {
    id: 'q11',
    name: 'Group by medium cardinality',
    description: 'Group by with moderate distinct values',
    complexity: 'moderate',
    sql: "SELECT RegionID, SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID",
    benchmarks: ['multi-aggregate-groupby', 'distinct-per-group'],
  },
  {
    id: 'q12',
    name: 'Group by with order',
    description: 'Group by with result ordering',
    complexity: 'moderate',
    sql: "SELECT RegionID, COUNT(*) AS c FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10",
    benchmarks: ['topk', 'sort-after-aggregate'],
  },
  {
    id: 'q13',
    name: 'Group by high cardinality string',
    description: 'Group by URL (high cardinality)',
    complexity: 'moderate',
    sql: "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    benchmarks: ['string-groupby', 'high-cardinality'],
  },
  {
    id: 'q14',
    name: 'Group by with multiple aggregates',
    description: 'Multiple aggregates per group',
    complexity: 'moderate',
    sql: "SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10",
    benchmarks: ['distinct-per-group-ordered', 'string-key-performance'],
  },
  {
    id: 'q15',
    name: 'Group by two columns',
    description: 'Composite group by key',
    complexity: 'moderate',
    sql: "SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10",
    benchmarks: ['mobile-analytics', 'device-grouping'],
  },
  {
    id: 'q16',
    name: 'Group by with string function',
    description: 'Group by with string manipulation',
    complexity: 'moderate',
    sql: "SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10",
    benchmarks: ['multi-column-groupby', 'string-groupby'],
  },
  {
    id: 'q17',
    name: 'Group by three columns',
    description: 'Three-column composite key',
    complexity: 'moderate',
    sql: "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    benchmarks: ['search-analytics', 'phrase-grouping'],
  },
  {
    id: 'q18',
    name: 'Group by date',
    description: 'Time-series aggregation',
    complexity: 'moderate',
    sql: "SELECT EventDate, COUNT(*) FROM hits GROUP BY EventDate ORDER BY EventDate",
    benchmarks: ['date-groupby', 'time-series'],
  },
  {
    id: 'q19',
    name: 'Group by date with filter',
    description: 'Filtered time-series',
    complexity: 'moderate',
    sql: "SELECT EventDate, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY EventDate ORDER BY EventDate",
    benchmarks: ['filtered-time-series', 'predicate-date-combo'],
  },

  // Complex analytical queries (Q20-Q32)
  {
    id: 'q20',
    name: 'Top referrers',
    description: 'Top referring URLs',
    complexity: 'complex',
    sql: "SELECT Referer, COUNT(*) AS c FROM hits WHERE Referer <> '' GROUP BY Referer ORDER BY c DESC LIMIT 10",
    benchmarks: ['referrer-analytics', 'url-groupby'],
  },
  {
    id: 'q21',
    name: 'Top URLs',
    description: 'Most visited URLs',
    complexity: 'complex',
    sql: "SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10",
    benchmarks: ['url-analytics', 'page-views'],
  },
  {
    id: 'q22',
    name: 'URL fragment analysis',
    description: 'Analyze URL parameters',
    complexity: 'complex',
    sql: "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    benchmarks: ['like-filter', 'search-referrer'],
  },
  {
    id: 'q23',
    name: 'User session analysis',
    description: 'Sessions per user',
    complexity: 'complex',
    sql: "SELECT UserID, COUNT(*) AS c FROM hits GROUP BY UserID ORDER BY c DESC LIMIT 10",
    benchmarks: ['user-analytics', 'high-cardinality-groupby'],
  },
  {
    id: 'q24',
    name: 'User activity over time',
    description: 'User engagement metrics',
    complexity: 'complex',
    sql: "SELECT UserID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY UserID, SearchPhrase ORDER BY c DESC LIMIT 10",
    benchmarks: ['user-search-analytics', 'multi-dimension'],
  },
  {
    id: 'q25',
    name: 'Geographic distribution',
    description: 'Hits by region and city',
    complexity: 'complex',
    sql: "SELECT RegionID, CounterID, COUNT(*) AS c FROM hits GROUP BY RegionID, CounterID ORDER BY c DESC LIMIT 10",
    benchmarks: ['geo-analytics', 'counter-distribution'],
  },
  {
    id: 'q26',
    name: 'Device analysis',
    description: 'Resolution distribution',
    complexity: 'complex',
    sql: "SELECT ResolutionWidth, ResolutionHeight, COUNT(*) AS c FROM hits GROUP BY ResolutionWidth, ResolutionHeight ORDER BY c DESC LIMIT 10",
    benchmarks: ['device-analytics', 'resolution-groupby'],
  },
  {
    id: 'q27',
    name: 'Browser analysis',
    description: 'Browser family distribution',
    complexity: 'complex',
    sql: "SELECT BrowserCountry, COUNT(*) AS c FROM hits GROUP BY BrowserCountry ORDER BY c DESC LIMIT 10",
    benchmarks: ['browser-analytics', 'country-distribution'],
  },
  {
    id: 'q28',
    name: 'Social traffic',
    description: 'Social media referrers',
    complexity: 'complex',
    sql: "SELECT SocialNetwork, COUNT(*) AS c FROM hits WHERE SocialNetwork <> '' GROUP BY SocialNetwork ORDER BY c DESC LIMIT 10",
    benchmarks: ['social-analytics', 'traffic-source'],
  },
  {
    id: 'q29',
    name: 'OS distribution',
    description: 'Operating system breakdown',
    complexity: 'complex',
    sql: "SELECT OS, COUNT(*) AS c FROM hits GROUP BY OS ORDER BY c DESC LIMIT 10",
    benchmarks: ['os-analytics', 'platform-distribution'],
  },
  {
    id: 'q30',
    name: 'Window client analysis',
    description: 'Client area dimensions',
    complexity: 'complex',
    sql: "SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS c FROM hits WHERE WindowClientWidth > 0 AND WindowClientHeight > 0 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY c DESC LIMIT 10",
    benchmarks: ['viewport-analytics', 'client-dimensions'],
  },
  {
    id: 'q31',
    name: 'Time analysis',
    description: 'Activity by minute',
    complexity: 'complex',
    sql: "SELECT ClientIP, COUNT(*) AS c FROM hits GROUP BY ClientIP ORDER BY c DESC LIMIT 10",
    benchmarks: ['ip-analytics', 'visitor-tracking'],
  },
  {
    id: 'q32',
    name: 'Ad engine performance',
    description: 'Advertising metrics',
    complexity: 'complex',
    sql: "SELECT AdvEngineID, COUNT(*) AS c FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY c DESC",
    benchmarks: ['ad-analytics', 'engine-performance'],
  },

  // Expert-level queries (Q33-Q42)
  {
    id: 'q33',
    name: 'Funnel analysis',
    description: 'Conversion funnel metrics',
    complexity: 'expert',
    sql: `SELECT
  SearchEngineID,
  SearchPhrase,
  COUNT(*) AS c
FROM hits
WHERE SearchPhrase <> '' AND SearchEngineID <> 0
GROUP BY SearchEngineID, SearchPhrase
ORDER BY c DESC
LIMIT 10`,
    benchmarks: ['funnel-analytics', 'search-engine-phrase'],
  },
  {
    id: 'q34',
    name: 'Goal conversion',
    description: 'Goal completion by source',
    complexity: 'expert',
    sql: `SELECT
  GoalID,
  COUNT(*) AS c
FROM hits
WHERE GoalID <> 0
GROUP BY GoalID
ORDER BY c DESC
LIMIT 10`,
    benchmarks: ['goal-analytics', 'conversion-tracking'],
  },
  {
    id: 'q35',
    name: 'E-commerce analysis',
    description: 'Currency and order metrics',
    complexity: 'expert',
    sql: `SELECT
  TraficSourceID,
  COUNT(*) AS c,
  SUM(IsRefresh) AS refreshes
FROM hits
GROUP BY TraficSourceID
ORDER BY c DESC
LIMIT 10`,
    benchmarks: ['traffic-source-analytics', 'refresh-tracking'],
  },
  {
    id: 'q36',
    name: 'Cohort analysis',
    description: 'User cohort metrics',
    complexity: 'expert',
    sql: `SELECT
  EventDate,
  COUNT(DISTINCT UserID) AS users,
  COUNT(*) AS hits
FROM hits
GROUP BY EventDate
ORDER BY EventDate`,
    benchmarks: ['cohort-analytics', 'daily-users'],
  },
  {
    id: 'q37',
    name: 'Retention analysis',
    description: 'Multi-day retention metrics',
    complexity: 'expert',
    sql: `SELECT
  DATE(EventTime) as dt,
  COUNT(*) AS c,
  COUNT(DISTINCT UserID) AS users
FROM hits
GROUP BY dt
ORDER BY dt
LIMIT 30`,
    benchmarks: ['retention-analytics', 'daily-aggregation'],
  },
  {
    id: 'q38',
    name: 'Complex filter chain',
    description: 'Multiple filter conditions',
    complexity: 'expert',
    sql: `SELECT
  URL,
  COUNT(*) AS c
FROM hits
WHERE CounterID = 62
  AND EventDate >= '2013-07-01'
  AND EventDate <= '2013-07-31'
  AND NOT DontCountHits
  AND NOT IsRefresh
  AND URL <> ''
GROUP BY URL
ORDER BY c DESC
LIMIT 10`,
    benchmarks: ['complex-filter', 'date-range', 'boolean-filter'],
  },
  {
    id: 'q39',
    name: 'Title analysis',
    description: 'Page title metrics',
    complexity: 'expert',
    sql: `SELECT
  Title,
  COUNT(*) AS c
FROM hits
WHERE Title <> ''
GROUP BY Title
ORDER BY c DESC
LIMIT 10`,
    benchmarks: ['title-analytics', 'content-analysis'],
  },
  {
    id: 'q40',
    name: 'Param analysis',
    description: 'URL parameter extraction',
    complexity: 'expert',
    sql: `SELECT
  ParamPrice,
  COUNT(*) AS c
FROM hits
WHERE ParamPrice > 0
GROUP BY ParamPrice
ORDER BY c DESC
LIMIT 10`,
    benchmarks: ['param-analytics', 'price-distribution'],
  },
  {
    id: 'q41',
    name: 'HAVING clause',
    description: 'Post-aggregation filtering',
    complexity: 'expert',
    sql: `SELECT
  CounterID,
  COUNT(*) AS c
FROM hits
GROUP BY CounterID
HAVING c > 100000
ORDER BY c DESC`,
    benchmarks: ['having-clause', 'post-aggregate-filter'],
  },
  {
    id: 'q42',
    name: 'Complex join simulation',
    description: 'Self-aggregation patterns',
    complexity: 'expert',
    sql: `SELECT
  SearchPhrase,
  COUNT(*) AS c,
  COUNT(DISTINCT UserID) AS u,
  MIN(EventTime) AS first_hit,
  MAX(EventTime) AS last_hit
FROM hits
WHERE SearchPhrase <> ''
GROUP BY SearchPhrase
ORDER BY c DESC
LIMIT 10`,
    benchmarks: ['complex-aggregation', 'first-last-touch'],
  },
]

// ============================================================================
// Utility Functions
// ============================================================================

function generateRunId(): string {
  const timestamp = Date.now().toString(36)
  const random = Math.random().toString(36).substring(2, 8)
  return `analytics-${timestamp}-${random}`
}

function calculatePercentile(sortedValues: number[], percentile: number): number {
  if (sortedValues.length === 0) return 0
  const index = Math.ceil((percentile / 100) * sortedValues.length) - 1
  return sortedValues[Math.max(0, index)]
}

function calculateStdDev(values: number[], mean: number): number {
  if (values.length <= 1) return 0
  const squaredDiffs = values.map((v) => Math.pow(v - mean, 2))
  const variance = squaredDiffs.reduce((a, b) => a + b, 0) / (values.length - 1)
  return Math.sqrt(variance)
}

function calculateStats(times: number[]): QueryTiming['stats'] {
  if (times.length === 0) {
    return { min: 0, max: 0, mean: 0, p50: 0, p99: 0, stddev: 0 }
  }

  const sorted = [...times].sort((a, b) => a - b)
  const sum = times.reduce((a, b) => a + b, 0)
  const mean = sum / times.length

  return {
    min: sorted[0],
    max: sorted[sorted.length - 1],
    mean,
    p50: calculatePercentile(sorted, 50),
    p99: calculatePercentile(sorted, 99),
    stddev: calculateStdDev(times, mean),
  }
}

// ============================================================================
// Database Interfaces
// ============================================================================

interface DatabaseAdapter {
  name: DatabaseType
  query<T = unknown>(sql: string): Promise<{ rows: T[]; rowCount: number }>
  close(): Promise<void>
}

/**
 * Create a SQLite adapter using InMemoryDatabase
 *
 * This is a JavaScript mock that provides basic SQL functionality for benchmarking.
 * It supports:
 * - SELECT with WHERE, GROUP BY, ORDER BY, LIMIT
 * - Aggregates: COUNT(*), COUNT(DISTINCT), SUM, AVG, MIN, MAX
 *
 * Limitations:
 * - No window functions
 * - No CTEs
 * - No JOINs
 * - Single GROUP BY column only
 * - Simplified SQL parsing
 */
async function createSQLiteAdapter(
  _bucket: R2Bucket,
  _dataset: DatasetType
): Promise<DatabaseAdapter> {
  const db = new InMemoryDatabase()

  // Define schema for ClickBench hits table
  const schema = [
    'WatchID', 'JavaEnable', 'Title', 'GoodEvent', 'EventTime', 'EventDate',
    'CounterID', 'ClientIP', 'RegionID', 'UserID', 'CounterClass', 'OS',
    'UserAgent', 'URL', 'Referer', 'IsRefresh', 'AdvEngineID', 'ResolutionWidth',
    'ResolutionHeight', 'MobilePhoneModel', 'MobilePhone', 'SearchPhrase',
    'SearchEngineID', 'TraficSourceID', 'WindowClientWidth', 'WindowClientHeight',
    'BrowserCountry', 'SocialNetwork', 'ParamPrice', 'GoalID', 'DontCountHits',
  ]

  // Generate sample data for benchmarking
  const sampleSize = 10000
  const data: Record<string, unknown>[] = []

  for (let i = 1; i <= sampleSize; i++) {
    const eventTime = new Date(Date.now() - i * 60000).toISOString()
    const eventDate = eventTime.split('T')[0]

    data.push({
      WatchID: i,
      JavaEnable: 1,
      Title: `Page ${i}`,
      GoodEvent: 1,
      EventTime: eventTime,
      EventDate: eventDate,
      CounterID: (i % 100) + 1,
      ClientIP: i * 1000,
      RegionID: i % 1000,
      UserID: i * 10,
      CounterClass: 1,
      OS: i % 10,
      UserAgent: i % 5,
      URL: `https://example.com/${i}`,
      Referer: i % 3 === 0 ? 'https://google.com' : '',
      IsRefresh: i % 2,
      AdvEngineID: i % 4,
      ResolutionWidth: 1024 + (i % 1920),
      ResolutionHeight: 768 + (i % 1080),
      MobilePhoneModel: i % 10 === 0 ? 'iPhone' : i % 10 === 1 ? 'Samsung' : '',
      MobilePhone: i % 10,
      SearchPhrase: i % 5 === 0 ? `search ${i}` : '',
      SearchEngineID: i % 3,
      TraficSourceID: i % 5,
      WindowClientWidth: 800 + (i % 1200),
      WindowClientHeight: 600 + (i % 800),
      BrowserCountry: 'US',
      SocialNetwork: i % 10 === 0 ? 'Facebook' : i % 10 === 1 ? 'Twitter' : '',
      ParamPrice: (i % 100) * 100,
      GoalID: i % 100,
      DontCountHits: 0,
    })
  }

  db.initialize(schema, data)

  return {
    name: 'sqlite',
    async query<T = unknown>(sql: string): Promise<{ rows: T[]; rowCount: number }> {
      try {
        const result = db.execute(sql)
        // Convert from [columns[], values[][]] format to objects
        const rows = result.rows.map((row: unknown[]) => {
          const obj: Record<string, unknown> = {}
          result.columns.forEach((col: string, idx: number) => {
            obj[col] = row[idx]
          })
          return obj as T
        })
        return { rows, rowCount: result.rowCount }
      } catch (error) {
        // Re-throw with more context
        throw new Error(`SQL error: ${error instanceof Error ? error.message : String(error)} - SQL: ${sql.slice(0, 100)}`)
      }
    },
    async close(): Promise<void> {
      // InMemoryDatabase doesn't need explicit cleanup
    },
  }
}

/**
 * Create database adapter based on type
 *
 * Currently only SQLite is available.
 * PostgreSQL and DuckDB adapters are disabled until their dependencies are resolved.
 */
async function createDatabaseAdapter(
  database: DatabaseType,
  bucket: R2Bucket,
  dataset: DatasetType
): Promise<DatabaseAdapter> {
  switch (database) {
    case 'sqlite':
      return createSQLiteAdapter(bucket, dataset)
    default:
      throw new Error(`Unsupported database: ${database}. Only 'sqlite' is currently available.`)
  }
}

// ============================================================================
// Benchmark Runner
// ============================================================================

async function runQueryBenchmark(
  adapter: DatabaseAdapter,
  query: BenchmarkQuery,
  iterations: number
): Promise<QueryTiming> {
  const times: number[] = []
  let rowCount: number | undefined
  let error: string | undefined

  // Warm-up query (not counted)
  try {
    await adapter.query(query.sql)
  } catch (e) {
    // Warm-up failed, continue anyway
  }

  // Run benchmark iterations
  for (let i = 0; i < iterations; i++) {
    try {
      const start = performance.now()
      const result = await adapter.query(query.sql)
      const end = performance.now()
      times.push(end - start)
      if (i === 0) {
        rowCount = result.rowCount
      }
    } catch (e) {
      error = e instanceof Error ? e.message : String(e)
      break
    }
  }

  return {
    queryId: query.id,
    queryName: query.name,
    complexity: query.complexity,
    iterations: times.length,
    times,
    stats: calculateStats(times),
    rowCount,
    error,
  }
}

async function runAnalyticsBenchmark(
  adapter: DatabaseAdapter,
  queries: BenchmarkQuery[],
  iterationsPerQuery: number
): Promise<QueryTiming[]> {
  const results: QueryTiming[] = []

  for (const query of queries) {
    const timing = await runQueryBenchmark(adapter, query, iterationsPerQuery)
    results.push(timing)
  }

  return results
}

// ============================================================================
// Hono App
// ============================================================================

const app = new Hono<{ Bindings: Env }>()

// Health check
app.get('/health', (c) => {
  return c.json({
    status: 'ok',
    service: 'bench-analytics',
    timestamp: new Date().toISOString(),
  })
})

// List available queries
app.get('/queries', (c) => {
  const complexity = c.req.query('complexity') as QueryComplexity | undefined
  let queries = CLICKBENCH_QUERIES

  if (complexity) {
    queries = queries.filter((q) => q.complexity === complexity)
  }

  return c.json({
    total: queries.length,
    queries: queries.map((q) => ({
      id: q.id,
      name: q.name,
      complexity: q.complexity,
      benchmarks: q.benchmarks,
    })),
  })
})

// Main benchmark endpoint
app.post('/benchmark/analytics', async (c) => {
  const database = (c.req.query('database') || 'sqlite') as DatabaseType
  const dataset = (c.req.query('dataset') || 'clickbench') as DatasetType
  const iterationsParam = c.req.query('iterations')
  const iterations = iterationsParam ? parseInt(iterationsParam, 10) : 3
  const complexityFilter = c.req.query('complexity') as QueryComplexity | undefined
  const queryIds = c.req.query('queries')?.split(',')

  // Validate inputs
  // Note: Only sqlite is currently available. PostgreSQL and DuckDB are disabled.
  const validDatabases: DatabaseType[] = ['sqlite']
  const validDatasets: DatasetType[] = ['clickbench', 'imdb']

  if (!validDatabases.includes(database)) {
    return c.json({
      error: `Invalid database: ${database}. Currently only 'sqlite' is available.`,
      note: 'PostgreSQL and DuckDB adapters are disabled until their dependencies are resolved.',
    }, 400)
  }

  if (!validDatasets.includes(dataset)) {
    return c.json({ error: `Invalid dataset. Valid options: ${validDatasets.join(', ')}` }, 400)
  }

  if (iterations < 1 || iterations > 100) {
    return c.json({ error: 'Iterations must be between 1 and 100' }, 400)
  }

  const runId = generateRunId()
  const startTime = performance.now()

  let adapter: DatabaseAdapter | null = null

  try {
    // Create database adapter
    adapter = await createDatabaseAdapter(database, c.env.ANALYTICS_BUCKET, dataset)

    // Filter queries
    let queries = CLICKBENCH_QUERIES

    if (complexityFilter) {
      queries = queries.filter((q) => q.complexity === complexityFilter)
    }

    if (queryIds && queryIds.length > 0) {
      queries = queries.filter((q) => queryIds.includes(q.id))
    }

    // Run benchmarks
    const queryResults = await runAnalyticsBenchmark(adapter, queries, iterations)

    const totalDurationMs = performance.now() - startTime

    // Calculate summary statistics
    const successfulQueries = queryResults.filter((r) => !r.error).length
    const failedQueries = queryResults.filter((r) => r.error).length

    const byComplexity = {
      simple: queryResults.filter((r) => r.complexity === 'simple' && !r.error),
      moderate: queryResults.filter((r) => r.complexity === 'moderate' && !r.error),
      complex: queryResults.filter((r) => r.complexity === 'complex' && !r.error),
      expert: queryResults.filter((r) => r.complexity === 'expert' && !r.error),
    }

    const avgByComplexity = (arr: QueryTiming[]) =>
      arr.length > 0 ? arr.reduce((sum, r) => sum + r.stats.mean, 0) / arr.length : 0

    // Get colo from request (Cloudflare-specific property)
    const cfRequest = c.req.raw as Request & { cf?: { colo?: string } }
    const colo = cfRequest.cf?.colo

    const results: AnalyticsBenchmarkResults = {
      runId,
      timestamp: new Date().toISOString(),
      environment: 'worker',
      colo,
      database,
      dataset,
      queryResults,
      summary: {
        totalDurationMs,
        totalQueries: queries.length,
        successfulQueries,
        failedQueries,
        simpleAvgMs: avgByComplexity(byComplexity.simple),
        moderateAvgMs: avgByComplexity(byComplexity.moderate),
        complexAvgMs: avgByComplexity(byComplexity.complex),
        expertAvgMs: avgByComplexity(byComplexity.expert),
      },
    }

    // Convert to JSONL format for R2 storage
    const jsonlResults = queryResults
      .filter((r) => !r.error)
      .map((r) => {
        const result: BenchmarkResult = {
          benchmark: `analytics/clickbench-${r.queryId}`,
          database,
          dataset,
          p50_ms: r.stats.p50,
          p99_ms: r.stats.p99,
          min_ms: r.stats.min,
          max_ms: r.stats.max,
          mean_ms: r.stats.mean,
          stddev_ms: r.stats.stddev,
          ops_per_sec: r.stats.mean > 0 ? 1000 / r.stats.mean : 0,
          iterations: r.iterations,
          vfs_reads: 0,
          vfs_writes: 0,
          vfs_bytes_read: 0,
          vfs_bytes_written: 0,
          timestamp: results.timestamp,
          environment: results.environment,
          run_id: runId,
          colo: results.colo,
          total_duration_ms: r.stats.mean * r.iterations,
        }
        return JSON.stringify(result)
      })

    // Store results in R2
    if (c.env.RESULTS) {
      const resultsKey = `analytics/${database}/${runId}.jsonl`
      await c.env.RESULTS.put(resultsKey, jsonlResults.join('\n'))
    }

    return c.json(results, 200, {
      'Content-Type': 'application/json',
      'X-Run-Id': runId,
      'X-Database': database,
      'X-Dataset': dataset,
    })
  } catch (error) {
    return c.json(
      {
        error: error instanceof Error ? error.message : String(error),
        runId,
        database,
        dataset,
      },
      500
    )
  } finally {
    if (adapter) {
      await adapter.close()
    }
  }
})

// Get stored results
app.get('/benchmark/analytics/results', async (c) => {
  if (!c.env.RESULTS) {
    return c.json({ error: 'Results bucket not configured' }, 500)
  }

  const database = c.req.query('database')
  const prefix = database ? `analytics/${database}/` : 'analytics/'
  const limit = parseInt(c.req.query('limit') || '100', 10)

  const list = await c.env.RESULTS.list({ prefix, limit })

  const results = list.objects.map((obj) => ({
    key: obj.key,
    size: obj.size,
    uploaded: obj.uploaded.toISOString(),
  }))

  return c.json({ results, truncated: list.truncated })
})

// Get specific result
app.get('/benchmark/analytics/results/:runId', async (c) => {
  if (!c.env.RESULTS) {
    return c.json({ error: 'Results bucket not configured' }, 500)
  }

  const runId = c.req.param('runId')
  const database = c.req.query('database') || 'sqlite'
  const key = `analytics/${database}/${runId}.jsonl`

  const object = await c.env.RESULTS.get(key)

  if (!object) {
    return c.json({ error: 'Result not found' }, 404)
  }

  return c.text(await object.text(), 200, {
    'Content-Type': 'application/x-ndjson',
  })
})

// API documentation
app.get('/', (c) => {
  return c.json({
    name: 'Analytics Benchmark Worker',
    description: 'ClickBench 43 queries benchmark runner on Cloudflare Workers',
    status: {
      sqlite: 'available',
      postgres: 'disabled - @dotdo/pglite not available',
      duckdb: 'disabled - requires WASM setup',
    },
    endpoints: {
      'GET /health': 'Health check',
      'GET /queries': 'List available benchmark queries',
      'POST /benchmark/analytics': 'Run analytics benchmarks',
      'GET /benchmark/analytics/results': 'List stored benchmark results',
      'GET /benchmark/analytics/results/:runId': 'Get specific benchmark result',
    },
    queryParams: {
      database: 'Database to benchmark: sqlite (default: sqlite)',
      dataset: 'Dataset to use: clickbench, imdb (default: clickbench)',
      iterations: 'Iterations per query (default: 3, max: 100)',
      complexity: 'Filter queries by complexity: simple, moderate, complex, expert',
      queries: 'Comma-separated list of query IDs to run (e.g., q0,q1,q2)',
    },
    example: 'POST /benchmark/analytics?database=sqlite&dataset=clickbench&iterations=5',
    queryCount: CLICKBENCH_QUERIES.length,
    notes: [
      'Using InMemoryDatabase (JavaScript mock with basic SQL support)',
      'Limited SQL support (no CTEs, window functions, no JOINs)',
      'Sample data: 10,000 rows seeded on startup',
      'Supports: COUNT, SUM, AVG, MIN, MAX, GROUP BY, ORDER BY, LIMIT, WHERE',
    ],
  })
})

export default app
