/**
 * ClickBench Dataset
 *
 * Based on https://github.com/ClickHouse/ClickBench
 *
 * 99M rows of real web analytics data from Yandex Metrica.
 * The de-facto standard for benchmarking analytical databases.
 *
 * Characteristics:
 * - 99,997,497 rows of anonymized web analytics hits
 * - 105 columns covering all aspects of web tracking
 * - Highly compressible (columnar-optimized)
 * - 43 standard benchmark queries
 *
 * Best suited for:
 * - DuckDB (excellent columnar performance)
 * - ClickHouse (purpose-built for this)
 * - db4 (SQLite-based analytics)
 */

import type { DatasetConfig, BenchmarkQuery, DatabaseType } from './index'

/**
 * ClickBench-specific configuration
 */
export interface ClickBenchConfig extends DatasetConfig {
  /** Original benchmark repository */
  benchmarkRepo: string
  /** List of databases with official results */
  officialResults: string[]
}

/**
 * ClickBench benchmark queries
 * Queries 0-42 from the official benchmark
 */
const clickbenchQueries: BenchmarkQuery[] = [
  // Simple aggregations (Q0-Q9)
  {
    id: 'q0',
    name: 'Count all rows',
    description: 'Simple full table scan and count',
    complexity: 'simple',
    sql: 'SELECT COUNT(*) FROM hits',
    benchmarks: ['scan-speed', 'row-counting'],
    expectedResults: { rowCount: 1, columns: ['count'] },
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

/**
 * ClickBench dataset configuration
 */
export const clickbench: ClickBenchConfig = {
  id: 'clickbench',
  name: 'ClickBench',
  description: `99M rows of real web analytics data from Yandex Metrica. The standard benchmark for analytical databases with 43 queries testing various aggregation, filtering, and grouping patterns.`,
  category: 'web-analytics',
  size: 'large',
  rowCount: '99,997,497',
  compressedSize: '14GB (Parquet)',
  uncompressedSize: '75GB (CSV)',
  sourceUrl: 'https://github.com/ClickHouse/ClickBench',
  license: 'CC BY 4.0',
  benchmarkRepo: 'https://github.com/ClickHouse/ClickBench',
  officialResults: ['ClickHouse', 'DuckDB', 'SQLite', 'PostgreSQL', 'MySQL', 'MariaDB'],
  suitedFor: ['duckdb', 'clickhouse', 'db4'],

  downloadConfigs: {
    local: {
      urls: ['https://datasets.clickhouse.com/hits_compatible/hits.parquet'],
      size: '14GB',
      rowCount: '99,997,497',
      checksum: 'sha256:...',
      instructions: [
        '# Download ClickBench dataset (14GB Parquet)',
        'curl -O https://datasets.clickhouse.com/hits_compatible/hits.parquet',
        '',
        '# For smaller local testing, use first 1M rows:',
        'duckdb -c "COPY (SELECT * FROM read_parquet(\'hits.parquet\') LIMIT 1000000) TO \'hits_sample.parquet\'"',
      ],
      setupCommands: [
        '# Load into DuckDB',
        'duckdb clickbench.db "CREATE TABLE hits AS SELECT * FROM read_parquet(\'hits.parquet\')"',
        '',
        '# Or load into SQLite (slower)',
        'sqlite3 clickbench.db < create_table.sql',
        'sqlite3 clickbench.db ".import --csv hits.csv hits"',
      ],
    },
    development: {
      urls: ['https://datasets.clickhouse.com/hits_compatible/hits.parquet'],
      size: '1.4GB',
      rowCount: '10,000,000',
      instructions: [
        '# Extract 10M row sample',
        'duckdb -c "COPY (SELECT * FROM read_parquet(\'hits.parquet\') LIMIT 10000000) TO \'hits_dev.parquet\'"',
      ],
      setupCommands: [
        'duckdb clickbench_dev.db "CREATE TABLE hits AS SELECT * FROM read_parquet(\'hits_dev.parquet\')"',
      ],
    },
    production: {
      urls: ['https://datasets.clickhouse.com/hits_compatible/hits.parquet'],
      size: '14GB',
      rowCount: '99,997,497',
      instructions: [
        '# Full dataset download',
        'curl -O https://datasets.clickhouse.com/hits_compatible/hits.parquet',
      ],
      setupCommands: [
        '# Create optimized table with proper compression',
        'duckdb clickbench.db "CREATE TABLE hits AS SELECT * FROM read_parquet(\'hits.parquet\')"',
        'duckdb clickbench.db "CHECKPOINT"',
      ],
    },
  },

  schema: {
    tableName: 'hits',
    columns: [
      { name: 'WatchID', type: 'BIGINT', nullable: false, description: 'Unique hit identifier' },
      { name: 'JavaEnable', type: 'SMALLINT', nullable: false, description: 'Java enabled flag' },
      { name: 'Title', type: 'TEXT', nullable: false, description: 'Page title' },
      { name: 'GoodEvent', type: 'SMALLINT', nullable: false, description: 'Valid event flag' },
      { name: 'EventTime', type: 'TIMESTAMP', nullable: false, description: 'Event timestamp' },
      { name: 'EventDate', type: 'DATE', nullable: false, description: 'Event date' },
      { name: 'CounterID', type: 'INTEGER', nullable: false, description: 'Counter identifier' },
      { name: 'ClientIP', type: 'INTEGER', nullable: false, description: 'Client IP (encoded)' },
      { name: 'RegionID', type: 'INTEGER', nullable: false, description: 'Geographic region' },
      { name: 'UserID', type: 'BIGINT', nullable: false, description: 'User identifier' },
      {
        name: 'CounterClass',
        type: 'SMALLINT',
        nullable: false,
        description: 'Counter class type',
      },
      { name: 'OS', type: 'SMALLINT', nullable: false, description: 'Operating system' },
      { name: 'UserAgent', type: 'SMALLINT', nullable: false, description: 'User agent type' },
      { name: 'URL', type: 'TEXT', nullable: false, description: 'Page URL' },
      { name: 'Referer', type: 'TEXT', nullable: false, description: 'Referrer URL' },
      { name: 'IsRefresh', type: 'SMALLINT', nullable: false, description: 'Page refresh flag' },
      { name: 'RefererCategoryID', type: 'SMALLINT', nullable: false, description: 'Referrer category' },
      { name: 'RefererRegionID', type: 'INTEGER', nullable: false, description: 'Referrer region' },
      { name: 'URLCategoryID', type: 'SMALLINT', nullable: false, description: 'URL category' },
      { name: 'URLRegionID', type: 'INTEGER', nullable: false, description: 'URL region' },
      { name: 'ResolutionWidth', type: 'SMALLINT', nullable: false, description: 'Screen width' },
      { name: 'ResolutionHeight', type: 'SMALLINT', nullable: false, description: 'Screen height' },
      { name: 'ResolutionDepth', type: 'SMALLINT', nullable: false, description: 'Color depth' },
      { name: 'FlashMajor', type: 'SMALLINT', nullable: false, description: 'Flash major version' },
      { name: 'FlashMinor', type: 'SMALLINT', nullable: false, description: 'Flash minor version' },
      { name: 'FlashMinor2', type: 'TEXT', nullable: false, description: 'Flash minor2' },
      { name: 'NetMajor', type: 'SMALLINT', nullable: false, description: '.NET major version' },
      { name: 'NetMinor', type: 'SMALLINT', nullable: false, description: '.NET minor version' },
      { name: 'UserAgentMajor', type: 'SMALLINT', nullable: false, description: 'UA major version' },
      { name: 'UserAgentMinor', type: 'VARCHAR(255)', nullable: false, description: 'UA minor version' },
      { name: 'CookieEnable', type: 'SMALLINT', nullable: false, description: 'Cookies enabled' },
      { name: 'JavascriptEnable', type: 'SMALLINT', nullable: false, description: 'JS enabled' },
      { name: 'IsMobile', type: 'SMALLINT', nullable: false, description: 'Mobile device flag' },
      { name: 'MobilePhone', type: 'SMALLINT', nullable: false, description: 'Mobile phone type' },
      { name: 'MobilePhoneModel', type: 'TEXT', nullable: false, description: 'Phone model' },
      { name: 'Params', type: 'TEXT', nullable: false, description: 'URL parameters' },
      { name: 'IPNetworkID', type: 'INTEGER', nullable: false, description: 'IP network' },
      { name: 'TraficSourceID', type: 'SMALLINT', nullable: false, description: 'Traffic source' },
      { name: 'SearchEngineID', type: 'SMALLINT', nullable: false, description: 'Search engine' },
      { name: 'SearchPhrase', type: 'TEXT', nullable: false, description: 'Search phrase' },
      { name: 'AdvEngineID', type: 'SMALLINT', nullable: false, description: 'Ad engine' },
      { name: 'IsArtifical', type: 'SMALLINT', nullable: false, description: 'Artificial flag' },
      { name: 'WindowClientWidth', type: 'SMALLINT', nullable: false, description: 'Window width' },
      { name: 'WindowClientHeight', type: 'SMALLINT', nullable: false, description: 'Window height' },
      { name: 'ClientTimeZone', type: 'SMALLINT', nullable: false, description: 'Timezone offset' },
      { name: 'ClientEventTime', type: 'TIMESTAMP', nullable: false, description: 'Client event time' },
      { name: 'SilverlightVersion1', type: 'SMALLINT', nullable: false, description: 'Silverlight v1' },
      { name: 'SilverlightVersion2', type: 'SMALLINT', nullable: false, description: 'Silverlight v2' },
      { name: 'SilverlightVersion3', type: 'INTEGER', nullable: false, description: 'Silverlight v3' },
      { name: 'SilverlightVersion4', type: 'SMALLINT', nullable: false, description: 'Silverlight v4' },
      { name: 'PageCharset', type: 'TEXT', nullable: false, description: 'Page charset' },
      { name: 'CodeVersion', type: 'INTEGER', nullable: false, description: 'Code version' },
      { name: 'IsLink', type: 'SMALLINT', nullable: false, description: 'Is link click' },
      { name: 'IsDownload', type: 'SMALLINT', nullable: false, description: 'Is download' },
      { name: 'IsNotBounce', type: 'SMALLINT', nullable: false, description: 'Not bounce flag' },
      { name: 'FUniqID', type: 'BIGINT', nullable: false, description: 'Unique fingerprint' },
      { name: 'OriginalURL', type: 'TEXT', nullable: false, description: 'Original URL' },
      { name: 'HID', type: 'INTEGER', nullable: false, description: 'Hit ID' },
      { name: 'IsOldCounter', type: 'SMALLINT', nullable: false, description: 'Old counter flag' },
      { name: 'IsEvent', type: 'SMALLINT', nullable: false, description: 'Is event' },
      { name: 'IsParameter', type: 'SMALLINT', nullable: false, description: 'Has parameters' },
      { name: 'DontCountHits', type: 'SMALLINT', nullable: false, description: 'Skip counting' },
      { name: 'WithHash', type: 'SMALLINT', nullable: false, description: 'Has hash' },
      { name: 'HitColor', type: 'CHAR(1)', nullable: false, description: 'Hit color code' },
      { name: 'LocalEventTime', type: 'TIMESTAMP', nullable: false, description: 'Local time' },
      { name: 'Age', type: 'SMALLINT', nullable: false, description: 'User age' },
      { name: 'Sex', type: 'SMALLINT', nullable: false, description: 'User sex' },
      { name: 'Income', type: 'SMALLINT', nullable: false, description: 'Income bracket' },
      { name: 'Interests', type: 'SMALLINT', nullable: false, description: 'Interest flags' },
      { name: 'Robotness', type: 'SMALLINT', nullable: false, description: 'Bot probability' },
      { name: 'RemoteIP', type: 'INTEGER', nullable: false, description: 'Remote IP' },
      { name: 'WindowName', type: 'INTEGER', nullable: false, description: 'Window name hash' },
      { name: 'OpenerName', type: 'INTEGER', nullable: false, description: 'Opener name hash' },
      { name: 'HistoryLength', type: 'SMALLINT', nullable: false, description: 'History length' },
      { name: 'BrowserLanguage', type: 'TEXT', nullable: false, description: 'Browser language' },
      { name: 'BrowserCountry', type: 'TEXT', nullable: false, description: 'Browser country' },
      { name: 'SocialNetwork', type: 'TEXT', nullable: false, description: 'Social network' },
      { name: 'SocialAction', type: 'TEXT', nullable: false, description: 'Social action' },
      { name: 'HTTPError', type: 'SMALLINT', nullable: false, description: 'HTTP error code' },
      { name: 'SendTiming', type: 'INTEGER', nullable: false, description: 'Send timing ms' },
      { name: 'DNSTiming', type: 'INTEGER', nullable: false, description: 'DNS timing ms' },
      { name: 'ConnectTiming', type: 'INTEGER', nullable: false, description: 'Connect timing ms' },
      { name: 'ResponseStartTiming', type: 'INTEGER', nullable: false, description: 'Response start ms' },
      { name: 'ResponseEndTiming', type: 'INTEGER', nullable: false, description: 'Response end ms' },
      { name: 'FetchTiming', type: 'INTEGER', nullable: false, description: 'Fetch timing ms' },
      { name: 'SocialSourceNetworkID', type: 'SMALLINT', nullable: false, description: 'Social source' },
      { name: 'SocialSourcePage', type: 'TEXT', nullable: false, description: 'Social source page' },
      { name: 'ParamPrice', type: 'BIGINT', nullable: false, description: 'Price parameter' },
      { name: 'ParamOrderID', type: 'TEXT', nullable: false, description: 'Order ID parameter' },
      { name: 'ParamCurrency', type: 'TEXT', nullable: false, description: 'Currency parameter' },
      { name: 'ParamCurrencyID', type: 'SMALLINT', nullable: false, description: 'Currency ID' },
      { name: 'OpenstatServiceName', type: 'TEXT', nullable: false, description: 'Openstat service' },
      { name: 'OpenstatCampaignID', type: 'TEXT', nullable: false, description: 'Campaign ID' },
      { name: 'OpenstatAdID', type: 'TEXT', nullable: false, description: 'Ad ID' },
      { name: 'OpenstatSourceID', type: 'TEXT', nullable: false, description: 'Source ID' },
      { name: 'UTMSource', type: 'TEXT', nullable: false, description: 'UTM source' },
      { name: 'UTMMedium', type: 'TEXT', nullable: false, description: 'UTM medium' },
      { name: 'UTMCampaign', type: 'TEXT', nullable: false, description: 'UTM campaign' },
      { name: 'UTMContent', type: 'TEXT', nullable: false, description: 'UTM content' },
      { name: 'UTMTerm', type: 'TEXT', nullable: false, description: 'UTM term' },
      { name: 'FromTag', type: 'TEXT', nullable: false, description: 'From tag' },
      { name: 'HasGCLID', type: 'SMALLINT', nullable: false, description: 'Has Google Click ID' },
      { name: 'RefererHash', type: 'BIGINT', nullable: false, description: 'Referrer hash' },
      { name: 'URLHash', type: 'BIGINT', nullable: false, description: 'URL hash' },
      { name: 'CLID', type: 'INTEGER', nullable: false, description: 'Click ID' },
      { name: 'GoalID', type: 'INTEGER', nullable: false, description: 'Goal ID' },
    ],
    primaryKey: ['WatchID'],
    indexes: [
      { name: 'idx_event_date', columns: ['EventDate'], type: 'btree', description: 'Date range queries' },
      { name: 'idx_counter', columns: ['CounterID'], type: 'btree', description: 'Counter filtering' },
      { name: 'idx_user', columns: ['UserID'], type: 'btree', description: 'User queries' },
      { name: 'idx_region', columns: ['RegionID'], type: 'btree', description: 'Geographic queries' },
    ],
    partitioning: {
      type: 'range',
      column: 'EventDate',
      granularity: 'month',
      description: 'Partition by month for time-range queries',
    },
    createTableSQL: {
      duckdb: `CREATE TABLE hits AS SELECT * FROM read_parquet('hits.parquet')`,
      clickhouse: `-- Use official ClickBench schema
-- https://github.com/ClickHouse/ClickBench/blob/main/clickhouse/create.sql`,
      db4: `-- Create table and import from Parquet via DuckDB
-- Then attach as SQLite database`,
      evodb: `-- Similar to db4, use DuckDB for initial load`,
      sqlite: `-- See full CREATE TABLE at:
-- https://github.com/ClickHouse/ClickBench/blob/main/sqlite/create.sql`,
      postgres: `-- See full CREATE TABLE at:
-- https://github.com/ClickHouse/ClickBench/blob/main/postgresql/create.sql`,
    },
  },

  queries: clickbenchQueries,

  performanceExpectations: {
    duckdb: {
      loadTime: '~2 minutes for 14GB Parquet',
      simpleQueryLatency: '<100ms',
      complexQueryLatency: '100ms-2s',
      storageEfficiency: 'Excellent (columnar compression)',
      concurrency: 'Good for read-heavy workloads',
      notes: [
        'Best-in-class for single-node analytics',
        'Vectorized execution engine',
        'Excellent Parquet integration',
        'Memory-efficient streaming',
      ],
    },
    clickhouse: {
      loadTime: '~1 minute',
      simpleQueryLatency: '<50ms',
      complexQueryLatency: '<1s',
      storageEfficiency: 'Excellent (native columnar)',
      concurrency: 'Excellent (designed for high QPS)',
      notes: [
        'Purpose-built for this workload',
        'Fastest for most queries',
        'Excellent compression (LZ4/ZSTD)',
        'Distributed capabilities',
      ],
    },
    db4: {
      loadTime: '~10 minutes',
      simpleQueryLatency: '100ms-500ms',
      complexQueryLatency: '1-10s',
      storageEfficiency: 'Moderate (SQLite row-based)',
      concurrency: 'Limited (single-writer)',
      notes: [
        'Good for edge deployments',
        'Embedded use cases',
        'Consider DuckDB extension for analytics',
      ],
    },
    evodb: {
      loadTime: '~10 minutes',
      simpleQueryLatency: '100ms-500ms',
      complexQueryLatency: '1-10s',
      storageEfficiency: 'Moderate',
      concurrency: 'Limited',
      notes: [
        'Event-sourced variant',
        'Better for temporal queries',
        'Consider hybrid approach with DuckDB',
      ],
    },
    sqlite: {
      loadTime: '~30 minutes',
      simpleQueryLatency: '500ms-2s',
      complexQueryLatency: '5-30s',
      storageEfficiency: 'Poor (row-based, minimal compression)',
      concurrency: 'Very limited',
      notes: [
        'Not recommended for large analytics',
        'Use for small subsets only',
        'Consider DuckDB instead',
      ],
    },
    postgres: {
      loadTime: '~15 minutes',
      simpleQueryLatency: '200ms-1s',
      complexQueryLatency: '2-15s',
      storageEfficiency: 'Moderate (TOAST compression)',
      concurrency: 'Good (MVCC)',
      notes: [
        'Better with columnar extensions (citus, timescaledb)',
        'Good for mixed workloads',
        'Consider partitioning by date',
      ],
    },
  },

  r2Config: {
    bucketName: 'bench-datasets',
    pathPrefix: 'clickbench/hits/',
    format: 'parquet',
    compression: 'zstd',
    partitioning: {
      columns: ['EventDate'],
      format: 'year={year}/month={month}/hits_{partition}.parquet',
    },
    uploadInstructions: [
      '# Partition data by date for efficient querying',
      'duckdb -c "',
      "  COPY (",
      "    SELECT *,",
      "    YEAR(EventDate) as year,",
      "    MONTH(EventDate) as month",
      "    FROM read_parquet('hits.parquet')",
      "  ) TO 'hits_partitioned'",
      "  (FORMAT PARQUET, PARTITION_BY (year, month), COMPRESSION 'zstd');",
      '"',
      '',
      '# Upload to R2',
      'wrangler r2 object put bench-datasets/clickbench/hits/ --file=hits_partitioned/ --recursive',
    ],
    duckdbInstructions: [
      "-- Query directly from R2 (via S3-compatible API)",
      "CREATE SECRET r2_secret (TYPE S3, KEY_ID 'xxx', SECRET 'xxx', ENDPOINT 'xxx.r2.cloudflarestorage.com');",
      "SELECT COUNT(*) FROM read_parquet('s3://bench-datasets/clickbench/hits/**/*.parquet');",
      '',
      '-- With partition pruning',
      "SELECT * FROM read_parquet('s3://bench-datasets/clickbench/hits/year=2013/month=7/*.parquet');",
    ],
  },
}

export default clickbench
