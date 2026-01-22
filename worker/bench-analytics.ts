/**
 * Analytics Benchmark Worker - ClickBench Suite
 *
 * Cloudflare Worker that runs the ClickBench 43 queries against analytical databases.
 * Supports postgres, sqlite, and duckdb with data loaded from R2.
 *
 * Benchmarks:
 * - Full table scans
 * - Aggregations (COUNT, SUM, AVG, MIN, MAX)
 * - Complex GROUP BY with ORDER BY
 * - Window functions (via complex aggregations)
 * - High-cardinality string operations
 *
 * Endpoint: POST /benchmark/analytics
 * Query params: ?database=duckdb&dataset=clickbench
 *
 * @see https://github.com/ClickHouse/ClickBench
 * @see datasets/analytics/clickbench.ts
 */

import { Hono } from 'hono'

// Note: These imports are resolved at deploy time via wrangler
// @dotdo/duckdb, @dotdo/postgres, @dotdo/sqlite are internal package aliases
// If deploying standalone, replace with:
// - @duckdb/duckdb-wasm
// - @electric-sql/pglite
// - sql.js

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

type DatabaseType = 'postgres' | 'sqlite' | 'duckdb'
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

interface Env {
  // R2 bucket containing analytics datasets
  ANALYTICS_BUCKET: R2Bucket
  // R2 bucket for storing benchmark results
  RESULTS: R2Bucket
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
 * Create a DuckDB adapter for querying Parquet files from R2
 */
async function createDuckDBAdapter(
  bucket: R2Bucket,
  dataset: DatasetType
): Promise<DatabaseAdapter> {
  // Dynamic import DuckDB WASM
  // Use @dotdo/duckdb if available, fallback to @duckdb/duckdb-wasm
  const duckdb = await import('@dotdo/duckdb').catch(() => import('@duckdb/duckdb-wasm')) as typeof import('@dotdo/duckdb')

  // Initialize DuckDB
  const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles()
  const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES as Parameters<typeof duckdb.selectBundle>[0])

  const worker_url = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
  )

  const worker = new Worker(worker_url)
  const logger = new duckdb.ConsoleLogger()
  const db = new duckdb.AsyncDuckDB(logger, worker)
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker as string)

  const conn = await db.connect()

  // Load data from R2 based on dataset
  if (dataset === 'clickbench') {
    // Try to fetch the parquet file from R2 and register it
    const parquetObject = await bucket.get('analytics/clickbench/hits.parquet')
    if (parquetObject) {
      const arrayBuffer = await parquetObject.arrayBuffer()
      await db.registerFileBuffer('hits.parquet', new Uint8Array(arrayBuffer))
      await conn.query("CREATE TABLE hits AS SELECT * FROM read_parquet('hits.parquet')")
    } else {
      // Create a sample hits table for testing (when no data available)
      await conn.query(`
        CREATE TABLE hits AS
        SELECT
          i AS WatchID,
          1 AS JavaEnable,
          'Page Title ' || i AS Title,
          1 AS GoodEvent,
          NOW() AS EventTime,
          CURRENT_DATE AS EventDate,
          (i % 100) + 1 AS CounterID,
          i * 1000 AS ClientIP,
          (i % 1000) AS RegionID,
          i * 10 AS UserID,
          1 AS CounterClass,
          (i % 10) AS OS,
          (i % 5) AS UserAgent,
          'https://example.com/page/' || i AS URL,
          CASE WHEN i % 3 = 0 THEN 'https://google.com' ELSE '' END AS Referer,
          (i % 2) AS IsRefresh,
          (i % 10) AS RefererCategoryID,
          (i % 500) AS RefererRegionID,
          (i % 20) AS URLCategoryID,
          (i % 500) AS URLRegionID,
          (1024 + (i % 1920)) AS ResolutionWidth,
          (768 + (i % 1080)) AS ResolutionHeight,
          24 AS ResolutionDepth,
          10 AS FlashMajor,
          3 AS FlashMinor,
          '3.0' AS FlashMinor2,
          4 AS NetMajor,
          0 AS NetMinor,
          100 AS UserAgentMajor,
          '0' AS UserAgentMinor,
          1 AS CookieEnable,
          1 AS JavascriptEnable,
          (i % 2) AS IsMobile,
          (i % 10) AS MobilePhone,
          CASE WHEN i % 10 = 0 THEN 'iPhone' WHEN i % 10 = 1 THEN 'Samsung Galaxy' ELSE '' END AS MobilePhoneModel,
          '' AS Params,
          i * 100 AS IPNetworkID,
          (i % 5) AS TraficSourceID,
          (i % 3) AS SearchEngineID,
          CASE WHEN i % 5 = 0 THEN 'search query ' || i ELSE '' END AS SearchPhrase,
          (i % 4) AS AdvEngineID,
          0 AS IsArtifical,
          (800 + (i % 1200)) AS WindowClientWidth,
          (600 + (i % 800)) AS WindowClientHeight,
          -5 AS ClientTimeZone,
          NOW() AS ClientEventTime,
          0 AS SilverlightVersion1,
          0 AS SilverlightVersion2,
          0 AS SilverlightVersion3,
          0 AS SilverlightVersion4,
          'UTF-8' AS PageCharset,
          123 AS CodeVersion,
          0 AS IsLink,
          0 AS IsDownload,
          1 AS IsNotBounce,
          i * 100 AS FUniqID,
          'https://example.com/page/' || i AS OriginalURL,
          i AS HID,
          0 AS IsOldCounter,
          0 AS IsEvent,
          0 AS IsParameter,
          0 AS DontCountHits,
          0 AS WithHash,
          'W' AS HitColor,
          NOW() AS LocalEventTime,
          (18 + (i % 60)) AS Age,
          (i % 2) AS Sex,
          (i % 5) AS Income,
          i % 256 AS Interests,
          0 AS Robotness,
          i * 1000 AS RemoteIP,
          i AS WindowName,
          0 AS OpenerName,
          (i % 20) AS HistoryLength,
          'en' AS BrowserLanguage,
          'US' AS BrowserCountry,
          CASE WHEN i % 10 = 0 THEN 'Facebook' WHEN i % 10 = 1 THEN 'Twitter' ELSE '' END AS SocialNetwork,
          '' AS SocialAction,
          0 AS HTTPError,
          (i % 100) AS SendTiming,
          (i % 50) AS DNSTiming,
          (i % 200) AS ConnectTiming,
          (i % 500) AS ResponseStartTiming,
          (i % 1000) AS ResponseEndTiming,
          (i % 2000) AS FetchTiming,
          0 AS SocialSourceNetworkID,
          '' AS SocialSourcePage,
          (i % 100) * 100 AS ParamPrice,
          '' AS ParamOrderID,
          '' AS ParamCurrency,
          0 AS ParamCurrencyID,
          '' AS OpenstatServiceName,
          '' AS OpenstatCampaignID,
          '' AS OpenstatAdID,
          '' AS OpenstatSourceID,
          '' AS UTMSource,
          '' AS UTMMedium,
          '' AS UTMCampaign,
          '' AS UTMContent,
          '' AS UTMTerm,
          '' AS FromTag,
          0 AS HasGCLID,
          i * 12345 AS RefererHash,
          i * 54321 AS URLHash,
          i AS CLID,
          (i % 100) AS GoalID
        FROM generate_series(1, 100000) AS t(i)
      `)
    }
  }

  return {
    name: 'duckdb',
    async query<T = unknown>(sql: string): Promise<{ rows: T[]; rowCount: number }> {
      const result = await conn.query(sql)
      const rows = result.toArray().map((row: { toJSON(): unknown }) => row.toJSON() as T)
      return { rows, rowCount: rows.length }
    },
    async close(): Promise<void> {
      await conn.close()
      await db.terminate()
      worker.terminate()
    },
  }
}

/**
 * Create a Postgres adapter (PGLite WASM)
 */
async function createPostgresAdapter(
  bucket: R2Bucket,
  dataset: DatasetType
): Promise<DatabaseAdapter> {
  // Dynamic import PGLite
  // Use @dotdo/postgres if available, fallback to @electric-sql/pglite
  const pgliteModule = await import('@dotdo/postgres').catch(() => import('@electric-sql/pglite'))
  const PGlite = (pgliteModule as any).PGlite

  const db = new PGlite()
  await db.waitReady

  // Create and seed the hits table
  // For Postgres, we create a simpler schema and seed with sample data
  await db.query(`
    CREATE TABLE IF NOT EXISTS hits (
      WatchID BIGINT,
      JavaEnable SMALLINT,
      Title TEXT,
      GoodEvent SMALLINT,
      EventTime TIMESTAMP,
      EventDate DATE,
      CounterID INTEGER,
      ClientIP INTEGER,
      RegionID INTEGER,
      UserID BIGINT,
      CounterClass SMALLINT,
      OS SMALLINT,
      UserAgent SMALLINT,
      URL TEXT,
      Referer TEXT,
      IsRefresh SMALLINT,
      AdvEngineID SMALLINT,
      ResolutionWidth SMALLINT,
      ResolutionHeight SMALLINT,
      MobilePhoneModel TEXT,
      MobilePhone SMALLINT,
      SearchPhrase TEXT,
      SearchEngineID SMALLINT,
      TraficSourceID SMALLINT,
      WindowClientWidth SMALLINT,
      WindowClientHeight SMALLINT,
      BrowserCountry TEXT,
      SocialNetwork TEXT,
      ParamPrice BIGINT,
      GoalID INTEGER,
      DontCountHits SMALLINT
    )
  `)

  // Seed some sample data using batch insert
  const batchSize = 1000
  for (let batch = 0; batch < 10; batch++) {
    const insertValues: string[] = []
    for (let i = batch * batchSize + 1; i <= (batch + 1) * batchSize; i++) {
      insertValues.push(`(
        ${i}, 1, 'Page ${i}', 1, NOW(), CURRENT_DATE, ${(i % 100) + 1}, ${i * 1000},
        ${i % 1000}, ${i * 10}, 1, ${i % 10}, ${i % 5}, 'https://example.com/${i}',
        ${i % 3 === 0 ? "'https://google.com'" : "''"}, ${i % 2}, ${i % 4},
        ${1024 + (i % 1920)}, ${768 + (i % 1080)},
        ${i % 10 === 0 ? "'iPhone'" : i % 10 === 1 ? "'Samsung'" : "''"},
        ${i % 10}, ${i % 5 === 0 ? `'search ${i}'` : "''"},
        ${i % 3}, ${i % 5}, ${800 + (i % 1200)}, ${600 + (i % 800)},
        'US', ${i % 10 === 0 ? "'Facebook'" : i % 10 === 1 ? "'Twitter'" : "''"},
        ${(i % 100) * 100}, ${i % 100}, 0
      )`)
    }
    await db.query(`INSERT INTO hits VALUES ${insertValues.join(',')}`)
  }

  return {
    name: 'postgres',
    async query<T = unknown>(sql: string): Promise<{ rows: T[]; rowCount: number }> {
      const result = await db.query(sql)
      return { rows: result.rows as T[], rowCount: result.rows.length }
    },
    async close(): Promise<void> {
      await db.close()
    },
  }
}

/**
 * Create a SQLite adapter (libsql WASM)
 */
async function createSQLiteAdapter(
  bucket: R2Bucket,
  dataset: DatasetType
): Promise<DatabaseAdapter> {
  // Dynamic import SQLite WASM
  // Use @dotdo/sqlite if available, fallback to sql.js
  const sqlModule = await import('@dotdo/sqlite').catch(() => import('sql.js'))
  const initSqlJs = (sqlModule as any).default || sqlModule
  const SQL = await initSqlJs()
  const db = new SQL.Database()

  // Create the hits table
  db.run(`
    CREATE TABLE IF NOT EXISTS hits (
      WatchID INTEGER,
      JavaEnable INTEGER,
      Title TEXT,
      GoodEvent INTEGER,
      EventTime TEXT,
      EventDate TEXT,
      CounterID INTEGER,
      ClientIP INTEGER,
      RegionID INTEGER,
      UserID INTEGER,
      CounterClass INTEGER,
      OS INTEGER,
      UserAgent INTEGER,
      URL TEXT,
      Referer TEXT,
      IsRefresh INTEGER,
      AdvEngineID INTEGER,
      ResolutionWidth INTEGER,
      ResolutionHeight INTEGER,
      MobilePhoneModel TEXT,
      MobilePhone INTEGER,
      SearchPhrase TEXT,
      SearchEngineID INTEGER,
      TraficSourceID INTEGER,
      WindowClientWidth INTEGER,
      WindowClientHeight INTEGER,
      BrowserCountry TEXT,
      SocialNetwork TEXT,
      ParamPrice INTEGER,
      GoalID INTEGER,
      DontCountHits INTEGER
    )
  `)

  // Seed sample data using batch insert
  db.run('BEGIN TRANSACTION')
  const stmt = db.prepare(`
    INSERT INTO hits VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `)

  for (let i = 1; i <= 10000; i++) {
    stmt.run([
      i, // WatchID
      1, // JavaEnable
      `Page ${i}`, // Title
      1, // GoodEvent
      new Date().toISOString(), // EventTime
      new Date().toISOString().split('T')[0], // EventDate
      (i % 100) + 1, // CounterID
      i * 1000, // ClientIP
      i % 1000, // RegionID
      i * 10, // UserID
      1, // CounterClass
      i % 10, // OS
      i % 5, // UserAgent
      `https://example.com/${i}`, // URL
      i % 3 === 0 ? 'https://google.com' : '', // Referer
      i % 2, // IsRefresh
      i % 4, // AdvEngineID
      1024 + (i % 1920), // ResolutionWidth
      768 + (i % 1080), // ResolutionHeight
      i % 10 === 0 ? 'iPhone' : i % 10 === 1 ? 'Samsung' : '', // MobilePhoneModel
      i % 10, // MobilePhone
      i % 5 === 0 ? `search ${i}` : '', // SearchPhrase
      i % 3, // SearchEngineID
      i % 5, // TraficSourceID
      800 + (i % 1200), // WindowClientWidth
      600 + (i % 800), // WindowClientHeight
      'US', // BrowserCountry
      i % 10 === 0 ? 'Facebook' : i % 10 === 1 ? 'Twitter' : '', // SocialNetwork
      (i % 100) * 100, // ParamPrice
      i % 100, // GoalID
      0, // DontCountHits
    ])
  }
  stmt.free()
  db.run('COMMIT')

  return {
    name: 'sqlite',
    async query<T = unknown>(sql: string): Promise<{ rows: T[]; rowCount: number }> {
      const results = db.exec(sql)
      if (results.length === 0) {
        return { rows: [], rowCount: 0 }
      }
      const columns = results[0].columns
      const rows = results[0].values.map((row: unknown[]) => {
        const obj: Record<string, unknown> = {}
        columns.forEach((col: string, idx: number) => {
          obj[col] = row[idx]
        })
        return obj as T
      })
      return { rows, rowCount: rows.length }
    },
    async close(): Promise<void> {
      db.close()
    },
  }
}

/**
 * Create database adapter based on type
 */
async function createDatabaseAdapter(
  database: DatabaseType,
  bucket: R2Bucket,
  dataset: DatasetType
): Promise<DatabaseAdapter> {
  switch (database) {
    case 'duckdb':
      return createDuckDBAdapter(bucket, dataset)
    case 'postgres':
      return createPostgresAdapter(bucket, dataset)
    case 'sqlite':
      return createSQLiteAdapter(bucket, dataset)
    default:
      throw new Error(`Unsupported database: ${database}`)
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
  const database = (c.req.query('database') || 'duckdb') as DatabaseType
  const dataset = (c.req.query('dataset') || 'clickbench') as DatasetType
  const iterationsParam = c.req.query('iterations')
  const iterations = iterationsParam ? parseInt(iterationsParam, 10) : 3
  const complexityFilter = c.req.query('complexity') as QueryComplexity | undefined
  const queryIds = c.req.query('queries')?.split(',')

  // Validate inputs
  const validDatabases: DatabaseType[] = ['postgres', 'sqlite', 'duckdb']
  const validDatasets: DatasetType[] = ['clickbench', 'imdb']

  if (!validDatabases.includes(database)) {
    return c.json({ error: `Invalid database. Valid options: ${validDatabases.join(', ')}` }, 400)
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

    // Get colo from request
    const colo = c.req.raw.cf?.colo as string | undefined

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

  const results = list.objects.map((obj: R2Object) => ({
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
  const database = c.req.query('database') || 'duckdb'
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
    endpoints: {
      'GET /health': 'Health check',
      'GET /queries': 'List available benchmark queries',
      'POST /benchmark/analytics': 'Run analytics benchmarks',
      'GET /benchmark/analytics/results': 'List stored benchmark results',
      'GET /benchmark/analytics/results/:runId': 'Get specific benchmark result',
    },
    queryParams: {
      database: 'Database to benchmark: postgres, sqlite, duckdb (default: duckdb)',
      dataset: 'Dataset to use: clickbench, imdb (default: clickbench)',
      iterations: 'Iterations per query (default: 3, max: 100)',
      complexity: 'Filter queries by complexity: simple, moderate, complex, expert',
      queries: 'Comma-separated list of query IDs to run (e.g., q0,q1,q2)',
    },
    example: 'POST /benchmark/analytics?database=duckdb&dataset=clickbench&iterations=5',
    queryCount: CLICKBENCH_QUERIES.length,
  })
})

export default app
