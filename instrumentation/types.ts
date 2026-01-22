/**
 * JSONL Benchmark Output Format Types
 *
 * Standardized schema for benchmark results that can be:
 * - Written to JSONL files for storage and analysis
 * - Compared across runs, databases, and environments
 * - Ingested into dashboards and alerting systems
 */

/**
 * Environment where the benchmark ran.
 */
export type BenchmarkEnvironment = 'worker' | 'do' | 'container' | 'local'

/**
 * Container size for container environments (Cloudflare Containers).
 */
export type ContainerSize = 'standard-1' | 'standard-2' | 'standard-4' | 'standard-8' | 'gpu-1'

/**
 * Primary benchmark result schema.
 *
 * Each benchmark run produces one BenchmarkResult per test case.
 * Results are written as newline-delimited JSON (JSONL).
 */
export interface BenchmarkResult {
  // ============================================================
  // Identification
  // ============================================================

  /**
   * Benchmark name/path.
   * Format: "{category}/{test-name}"
   * Examples: "oltp/point-lookup", "analytics/clickbench-q1", "cold-start/worker"
   */
  benchmark: string

  /**
   * Database being tested.
   * Examples: "db4", "postgres", "sqlite", "duckdb", "evodb"
   */
  database: string

  /**
   * Dataset used for the benchmark.
   * Examples: "ecommerce-1gb", "clickbench", "synthetic-100k"
   */
  dataset: string

  // ============================================================
  // Timing Metrics (in milliseconds)
  // ============================================================

  /**
   * 50th percentile (median) latency in milliseconds.
   */
  p50_ms: number

  /**
   * 99th percentile latency in milliseconds.
   */
  p99_ms: number

  /**
   * Minimum latency in milliseconds.
   */
  min_ms: number

  /**
   * Maximum latency in milliseconds.
   */
  max_ms: number

  /**
   * Mean (average) latency in milliseconds.
   */
  mean_ms: number

  /**
   * Standard deviation of latencies in milliseconds.
   * Optional - not all runners compute this.
   */
  stddev_ms?: number

  // ============================================================
  // Throughput
  // ============================================================

  /**
   * Operations per second achieved.
   */
  ops_per_sec: number

  /**
   * Total number of iterations run.
   */
  iterations: number

  /**
   * Total benchmark duration in milliseconds.
   */
  total_duration_ms?: number

  // ============================================================
  // VFS/Storage Cost Metrics
  // ============================================================

  /**
   * Number of VFS read operations (blob reads).
   */
  vfs_reads: number

  /**
   * Number of VFS write operations (blob writes).
   */
  vfs_writes: number

  /**
   * Total bytes read through VFS.
   */
  vfs_bytes_read: number

  /**
   * Total bytes written through VFS.
   */
  vfs_bytes_written: number

  /**
   * SQL rows read (if applicable).
   */
  sql_rows_read?: number

  /**
   * SQL rows written (if applicable).
   */
  sql_rows_written?: number

  /**
   * Write amplification factor (bytes written / logical bytes).
   */
  write_amplification?: number

  /**
   * Read amplification factor (bytes read / logical bytes).
   */
  read_amplification?: number

  /**
   * Estimated cost in USD for this operation at scale.
   */
  estimated_cost_usd?: number

  // ============================================================
  // Environment
  // ============================================================

  /**
   * ISO 8601 timestamp when the benchmark started.
   */
  timestamp: string

  /**
   * Execution environment.
   */
  environment: BenchmarkEnvironment

  /**
   * Container size if environment is 'container'.
   */
  container_size?: ContainerSize

  /**
   * WASM bundle size in bytes (for WASM-based databases).
   */
  wasm_size_bytes?: number

  /**
   * Cold start time in milliseconds.
   * Time from zero state to first query result.
   */
  cold_start_ms?: number

  /**
   * Memory usage in bytes at end of benchmark.
   */
  memory_bytes?: number

  /**
   * CPU time consumed in milliseconds.
   */
  cpu_time_ms?: number

  /**
   * Cloudflare colo code (e.g., "SJC", "AMS").
   */
  colo?: string

  // ============================================================
  // Metadata
  // ============================================================

  /**
   * Git commit SHA for the benchmark code.
   */
  git_sha?: string

  /**
   * Git branch name.
   */
  git_branch?: string

  /**
   * Unique identifier for this benchmark run.
   * Format: UUID or "{timestamp}-{random}"
   */
  run_id: string

  /**
   * Vitest task name (full path from describe/bench).
   */
  vitest_task?: string

  /**
   * Additional tags for filtering/grouping.
   */
  tags?: Record<string, string>

  /**
   * Free-form notes about this run.
   */
  notes?: string
}

/**
 * Partial benchmark result for incremental updates.
 * Only benchmark and run_id are required; all else optional.
 */
export type PartialBenchmarkResult = Pick<BenchmarkResult, 'benchmark' | 'run_id'> &
  Partial<Omit<BenchmarkResult, 'benchmark' | 'run_id'>>

/**
 * Filter options for querying benchmark results.
 */
export interface BenchmarkFilter {
  /**
   * Filter by benchmark name (exact match or regex).
   */
  benchmark?: string | RegExp

  /**
   * Filter by database.
   */
  database?: string | string[]

  /**
   * Filter by dataset.
   */
  dataset?: string | string[]

  /**
   * Filter by environment.
   */
  environment?: BenchmarkEnvironment | BenchmarkEnvironment[]

  /**
   * Filter by run ID.
   */
  run_id?: string

  /**
   * Filter by git SHA.
   */
  git_sha?: string

  /**
   * Filter by timestamp range (ISO 8601).
   */
  timestamp_after?: string

  /**
   * Filter by timestamp range (ISO 8601).
   */
  timestamp_before?: string

  /**
   * Filter by tag key-value pairs.
   */
  tags?: Record<string, string>

  /**
   * Custom filter function.
   */
  predicate?: (result: BenchmarkResult) => boolean
}

/**
 * Aggregation options for summarizing results.
 */
export interface AggregationOptions {
  /**
   * Group by these fields.
   */
  groupBy: Array<keyof BenchmarkResult>

  /**
   * Calculate these statistics for each group.
   */
  stats?: Array<'min' | 'max' | 'mean' | 'median' | 'stddev' | 'p50' | 'p99'>
}

/**
 * Aggregated benchmark summary.
 */
export interface BenchmarkSummary {
  /**
   * Group key values.
   */
  group: Record<string, string | number>

  /**
   * Number of results in this group.
   */
  count: number

  /**
   * Aggregated timing metrics.
   */
  timing: {
    p50_ms: { min: number; max: number; mean: number; median: number }
    p99_ms: { min: number; max: number; mean: number; median: number }
    mean_ms: { min: number; max: number; mean: number; median: number }
  }

  /**
   * Aggregated throughput.
   */
  throughput: {
    ops_per_sec: { min: number; max: number; mean: number; median: number }
  }

  /**
   * Aggregated VFS metrics.
   */
  vfs: {
    reads: { total: number; mean: number }
    writes: { total: number; mean: number }
    bytes_read: { total: number; mean: number }
    bytes_written: { total: number; mean: number }
  }
}

/**
 * Options for JSONL writer.
 */
export interface WriterOptions {
  /**
   * Output file path.
   */
  outputPath: string

  /**
   * Whether to append to existing file or overwrite.
   * Default: true (append)
   */
  append?: boolean

  /**
   * Buffer size before flushing to disk.
   * Default: 100
   */
  bufferSize?: number

  /**
   * Whether to pretty-print JSON (useful for debugging, not recommended for production).
   * Default: false
   */
  prettyPrint?: boolean

  /**
   * Auto-flush interval in milliseconds.
   * Default: 5000 (5 seconds)
   */
  flushInterval?: number
}

/**
 * Options for JSONL reader.
 */
export interface ReaderOptions {
  /**
   * Input file path or paths.
   */
  inputPath: string | string[]

  /**
   * Whether to stream results or load all into memory.
   * Default: false (load all)
   */
  streaming?: boolean

  /**
   * Maximum results to return.
   */
  limit?: number

  /**
   * Number of results to skip.
   */
  offset?: number

  /**
   * Sort by field.
   */
  sortBy?: keyof BenchmarkResult

  /**
   * Sort direction.
   */
  sortDirection?: 'asc' | 'desc'
}

/**
 * Vitest benchmark task result (from vitest internal API).
 * This matches the structure provided by vitest bench.
 */
export interface VitestBenchmarkTask {
  /**
   * Task ID.
   */
  id: string

  /**
   * Task name.
   */
  name: string

  /**
   * Suite name(s) this task belongs to.
   */
  suite?: {
    name: string
    file?: {
      name: string
    }
  }

  /**
   * Benchmark result (if available).
   */
  result?: {
    /**
     * State of the task.
     */
    state: 'pass' | 'fail' | 'skip' | 'run'

    /**
     * Duration in milliseconds.
     */
    duration?: number

    /**
     * Benchmark-specific metrics.
     */
    benchmark?: {
      /**
       * Benchmark name.
       */
      name: string

      /**
       * Rank among siblings.
       */
      rank: number

      /**
       * Whether this is the fastest in its group.
       */
      fastest?: boolean

      /**
       * Samples (timing values in nanoseconds).
       */
      samples: number[]

      /**
       * Min time in nanoseconds.
       */
      min: number

      /**
       * Max time in nanoseconds.
       */
      max: number

      /**
       * Mean time in nanoseconds.
       */
      mean: number

      /**
       * Median time in nanoseconds.
       */
      median?: number

      /**
       * Standard deviation in nanoseconds.
       */
      sd: number

      /**
       * P75 time in nanoseconds.
       */
      p75: number

      /**
       * P99 time in nanoseconds.
       */
      p99: number

      /**
       * P995 time in nanoseconds.
       */
      p995: number

      /**
       * P999 time in nanoseconds.
       */
      p999: number

      /**
       * Relative Margin of Error.
       */
      rme: number

      /**
       * Samples count.
       */
      sampleCount?: number

      /**
       * Hertz (operations per second).
       */
      hz: number

      /**
       * Period in nanoseconds.
       */
      period: number
    }

    /**
     * Error if task failed.
     */
    error?: {
      message: string
      stack?: string
    }
  }

  /**
   * File path.
   */
  file?: {
    name: string
    filepath: string
  }

  /**
   * Benchmark options.
   */
  options?: {
    iterations?: number
    warmupIterations?: number
    time?: number
    warmupTime?: number
  }
}

/**
 * Reporter options for the JSONL reporter.
 */
export interface BenchReporterOptions {
  /**
   * Output file path for JSONL results.
   * Default: './benchmark-results.jsonl'
   */
  outputPath?: string

  /**
   * Whether to append to existing file.
   * Default: true
   */
  append?: boolean

  /**
   * Include VFS metrics from instrumentation.
   * Default: true
   */
  includeVfsMetrics?: boolean

  /**
   * Default database name (can be overridden per-test).
   * Default: 'unknown'
   */
  defaultDatabase?: string

  /**
   * Default dataset name.
   * Default: 'default'
   */
  defaultDataset?: string

  /**
   * Default environment.
   * Default: 'local'
   */
  defaultEnvironment?: BenchmarkEnvironment

  /**
   * Additional tags to include on all results.
   */
  tags?: Record<string, string>

  /**
   * Whether to print summary to console.
   * Default: true
   */
  printSummary?: boolean

  /**
   * Custom function to extract database name from task.
   */
  extractDatabase?: (task: VitestBenchmarkTask) => string

  /**
   * Custom function to extract dataset from task.
   */
  extractDataset?: (task: VitestBenchmarkTask) => string

  /**
   * Git info to include.
   */
  git?: {
    sha?: string
    branch?: string
  }
}

/**
 * Comparison result between two benchmark runs.
 */
export interface BenchmarkComparison {
  /**
   * Benchmark identifier.
   */
  benchmark: string

  /**
   * Database being compared.
   */
  database: string

  /**
   * Baseline result.
   */
  baseline: BenchmarkResult

  /**
   * Current result.
   */
  current: BenchmarkResult

  /**
   * Percentage change in p50 latency (positive = slower).
   */
  p50_change_pct: number

  /**
   * Percentage change in p99 latency.
   */
  p99_change_pct: number

  /**
   * Percentage change in ops/sec (positive = faster).
   */
  ops_per_sec_change_pct: number

  /**
   * Whether the change is statistically significant.
   */
  significant: boolean

  /**
   * Whether this represents a regression.
   */
  regression: boolean
}
