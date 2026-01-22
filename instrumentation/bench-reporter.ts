/**
 * Vitest Benchmark Reporter for JSONL Output
 *
 * A custom Vitest reporter that captures benchmark results and writes
 * them to JSONL format for analysis and comparison.
 *
 * Features:
 * - Automatic capture of vitest bench results
 * - Conversion from vitest format to BenchmarkResult schema
 * - Integration with VFS instrumentation metrics
 * - Git metadata inclusion
 * - Console summary output
 *
 * Usage in vitest.config.ts:
 * ```typescript
 * import { defineConfig } from 'vitest/config'
 * import { BenchReporter } from './instrumentation/bench-reporter'
 *
 * export default defineConfig({
 *   test: {
 *     benchmark: {
 *       reporters: [
 *         'default',
 *         new BenchReporter({
 *           outputPath: './benchmark-results.jsonl',
 *         }),
 *       ],
 *     },
 *   },
 * })
 * ```
 */

import type { Reporter, File, Task, TaskResultPack } from 'vitest'
import type {
  BenchmarkResult,
  BenchReporterOptions,
  VitestBenchmarkTask,
  BenchmarkEnvironment,
} from './types'
import { JSONLWriter, generateRunId, getGitInfo } from './writer'

/**
 * Global registry for VFS metrics.
 * Benchmarks can register their metrics here for the reporter to pick up.
 */
export const vfsMetricsRegistry = new Map<string, {
  vfs_reads: number
  vfs_writes: number
  vfs_bytes_read: number
  vfs_bytes_written: number
  sql_rows_read?: number
  sql_rows_written?: number
  write_amplification?: number
  read_amplification?: number
  estimated_cost_usd?: number
}>()

/**
 * Register VFS metrics for a benchmark.
 * Call this from within your benchmark to attach VFS metrics to the result.
 *
 * @example
 * ```typescript
 * import { registerVfsMetrics } from './instrumentation/bench-reporter'
 *
 * bench('my benchmark', async () => {
 *   instrument.reset()
 *   // ... do work ...
 *   const metrics = instrument.getMetrics()
 *   registerVfsMetrics('my benchmark', {
 *     vfs_reads: metrics.blobsRead,
 *     vfs_writes: metrics.blobsWritten,
 *     vfs_bytes_read: metrics.bytesRead,
 *     vfs_bytes_written: metrics.bytesWritten,
 *   })
 * })
 * ```
 */
export function registerVfsMetrics(
  benchmarkName: string,
  metrics: {
    vfs_reads: number
    vfs_writes: number
    vfs_bytes_read: number
    vfs_bytes_written: number
    sql_rows_read?: number
    sql_rows_written?: number
    write_amplification?: number
    read_amplification?: number
    estimated_cost_usd?: number
  }
): void {
  vfsMetricsRegistry.set(benchmarkName, metrics)
}

/**
 * Clear VFS metrics registry.
 * Call this at the start of a benchmark suite to ensure fresh metrics.
 */
export function clearVfsMetrics(): void {
  vfsMetricsRegistry.clear()
}

/**
 * Default reporter options.
 */
const DEFAULT_OPTIONS: Required<Omit<BenchReporterOptions, 'extractDatabase' | 'extractDataset' | 'git' | 'tags'>> = {
  outputPath: './benchmark-results.jsonl',
  append: true,
  includeVfsMetrics: true,
  defaultDatabase: 'unknown',
  defaultDataset: 'default',
  defaultEnvironment: 'local',
  printSummary: true,
}

/**
 * Vitest Benchmark Reporter that outputs JSONL.
 */
export class BenchReporter implements Reporter {
  private options: BenchReporterOptions
  private writer: JSONLWriter | null = null
  private runId: string
  private gitInfo: { sha: string; branch: string } | null = null
  private results: BenchmarkResult[] = []
  private startTime: number = 0

  constructor(options: Partial<BenchReporterOptions> = {}) {
    this.options = {
      ...DEFAULT_OPTIONS,
      ...options,
    }
    this.runId = generateRunId()
  }

  /**
   * Called when vitest starts.
   */
  async onInit(): Promise<void> {
    this.startTime = Date.now()

    // Initialize writer
    this.writer = new JSONLWriter({
      outputPath: this.options.outputPath ?? DEFAULT_OPTIONS.outputPath,
      append: this.options.append ?? DEFAULT_OPTIONS.append,
    })

    // Get git info
    if (this.options.git) {
      this.gitInfo = {
        sha: this.options.git.sha ?? '',
        branch: this.options.git.branch ?? '',
      }
    } else {
      this.gitInfo = await getGitInfo()
    }
  }

  /**
   * Called when a task (benchmark) is updated.
   * This is where we capture benchmark results.
   */
  onTaskUpdate(packs: TaskResultPack[]): void {
    for (const pack of packs) {
      const [taskId, result, meta] = pack

      // Skip non-benchmark results
      if (!result?.benchmark) continue

      // Convert to our format
      const benchResult = this.convertResult(taskId, result, meta)
      if (benchResult) {
        this.results.push(benchResult)
        this.writer?.write(benchResult)
      }
    }
  }

  /**
   * Called when all tests/benchmarks are finished.
   */
  async onFinished(files?: File[]): Promise<void> {
    // Process any remaining results from files
    if (files) {
      for (const file of files) {
        this.processFile(file)
      }
    }

    // Flush and close writer
    if (this.writer) {
      await this.writer.close()
    }

    // Print summary if enabled
    if (this.options.printSummary) {
      this.printSummary()
    }
  }

  /**
   * Process a completed file and extract benchmark results.
   */
  private processFile(file: File): void {
    if (!file.tasks) return

    for (const task of file.tasks) {
      this.processTask(task, file)
    }
  }

  /**
   * Process a single task (may be a suite or a benchmark).
   */
  private processTask(task: Task, file: File): void {
    // Handle suites (describe blocks)
    if (task.type === 'suite' && 'tasks' in task && task.tasks) {
      for (const subTask of task.tasks) {
        this.processTask(subTask, file)
      }
      return
    }

    // Handle individual benchmarks (custom type in vitest bench)
    const taskAny = task as { type: string; result?: { benchmark?: unknown } }
    if (taskAny.type === 'benchmark' && taskAny.result?.benchmark) {
      const result = this.convertTask(task as unknown as VitestBenchmarkTask, file)
      if (result && !this.results.some(r => r.vitest_task === result.vitest_task)) {
        this.results.push(result)
        this.writer?.write(result)
      }
    }
  }

  /**
   * Convert a vitest task result pack to BenchmarkResult.
   */
  private convertResult(
    _taskId: string,
    result: TaskResultPack[1],
    _meta: TaskResultPack[2]
  ): BenchmarkResult | null {
    if (!result?.benchmark) return null

    const bench = result.benchmark

    // Nanoseconds to milliseconds
    const nsToMs = (ns: number) => ns / 1_000_000

    const benchmarkResult: BenchmarkResult = {
      benchmark: bench.name,
      database: this.extractDatabase(bench.name),
      dataset: this.extractDataset(bench.name),

      // Timing - vitest reports in nanoseconds
      p50_ms: nsToMs(bench.median ?? bench.mean),
      p99_ms: nsToMs(bench.p99),
      min_ms: nsToMs(bench.min),
      max_ms: nsToMs(bench.max),
      mean_ms: nsToMs(bench.mean),
      stddev_ms: nsToMs(bench.sd),

      // Throughput
      ops_per_sec: bench.hz,
      iterations: bench.sampleCount ?? bench.samples?.length ?? 0,
      total_duration_ms: result.duration,

      // VFS metrics (from registry or defaults)
      vfs_reads: 0,
      vfs_writes: 0,
      vfs_bytes_read: 0,
      vfs_bytes_written: 0,

      // Environment
      timestamp: new Date().toISOString(),
      environment: this.detectEnvironment(),
      run_id: this.runId,
      vitest_task: bench.name,

      // Git info
      git_sha: this.gitInfo?.sha,
      git_branch: this.gitInfo?.branch,

      // Tags
      tags: this.options.tags,
    }

    // Merge VFS metrics if available
    if (this.options.includeVfsMetrics) {
      const vfsMetrics = vfsMetricsRegistry.get(bench.name)
      if (vfsMetrics) {
        Object.assign(benchmarkResult, vfsMetrics)
      }
    }

    return benchmarkResult
  }

  /**
   * Convert a VitestBenchmarkTask to BenchmarkResult.
   */
  private convertTask(
    task: VitestBenchmarkTask,
    file: File
  ): BenchmarkResult | null {
    if (!task.result?.benchmark) return null

    const bench = task.result.benchmark
    const suiteName = task.suite?.name ?? ''
    const fullName = suiteName ? `${suiteName} > ${task.name}` : task.name

    // Nanoseconds to milliseconds
    const nsToMs = (ns: number) => ns / 1_000_000

    const benchmarkResult: BenchmarkResult = {
      benchmark: this.normalizeBenchmarkName(fullName, file.name),
      database: this.extractDatabase(fullName),
      dataset: this.extractDataset(fullName),

      // Timing
      p50_ms: nsToMs(bench.median ?? bench.mean),
      p99_ms: nsToMs(bench.p99),
      min_ms: nsToMs(bench.min),
      max_ms: nsToMs(bench.max),
      mean_ms: nsToMs(bench.mean),
      stddev_ms: nsToMs(bench.sd),

      // Throughput
      ops_per_sec: bench.hz,
      iterations: bench.sampleCount ?? bench.samples?.length ?? 0,
      total_duration_ms: task.result.duration,

      // VFS metrics
      vfs_reads: 0,
      vfs_writes: 0,
      vfs_bytes_read: 0,
      vfs_bytes_written: 0,

      // Environment
      timestamp: new Date().toISOString(),
      environment: this.detectEnvironment(),
      run_id: this.runId,
      vitest_task: fullName,

      // Git info
      git_sha: this.gitInfo?.sha,
      git_branch: this.gitInfo?.branch,

      // Tags
      tags: this.options.tags,
    }

    // Merge VFS metrics
    if (this.options.includeVfsMetrics) {
      const vfsMetrics = vfsMetricsRegistry.get(task.name) ?? vfsMetricsRegistry.get(fullName)
      if (vfsMetrics) {
        Object.assign(benchmarkResult, vfsMetrics)
      }
    }

    return benchmarkResult
  }

  /**
   * Normalize benchmark name to a consistent format.
   * e.g., "VFS Cost - Write Amplification > db4 insert 100 rows" -> "vfs-cost/write-amplification/db4-insert-100-rows"
   */
  private normalizeBenchmarkName(fullName: string, fileName: string): string {
    // Extract category from file name
    const category = fileName
      .replace(/\.bench\.ts$/, '')
      .replace(/.*\//, '')

    // Normalize the test name
    const testName = fullName
      .toLowerCase()
      .replace(/\s+>\s+/g, '/')
      .replace(/\s+-\s+/g, '/')
      .replace(/\s+/g, '-')
      .replace(/[^a-z0-9/-]/g, '')

    return `${category}/${testName}`
  }

  /**
   * Extract database name from benchmark name.
   */
  private extractDatabase(name: string): string {
    if (this.options.extractDatabase) {
      return this.options.extractDatabase({ name } as VitestBenchmarkTask)
    }

    // Common patterns
    const lower = name.toLowerCase()

    const databases = ['db4', 'evodb', 'postgres', 'sqlite', 'duckdb', 'mongo', 'tigerbeetle']
    for (const db of databases) {
      if (lower.includes(db)) return db
    }

    // Check for PGLite
    if (lower.includes('pglite')) return 'postgres'

    // Check for libsql
    if (lower.includes('libsql')) return 'sqlite'

    return this.options.defaultDatabase ?? DEFAULT_OPTIONS.defaultDatabase
  }

  /**
   * Extract dataset name from benchmark name.
   */
  private extractDataset(name: string): string {
    if (this.options.extractDataset) {
      return this.options.extractDataset({ name } as VitestBenchmarkTask)
    }

    // Try to find dataset patterns
    const lower = name.toLowerCase()

    // Look for row counts
    const rowMatch = lower.match(/(\d+)\s*(rows?|records?|items?)/i)
    if (rowMatch) {
      return `synthetic-${rowMatch[1]}`
    }

    // Look for size patterns
    const sizeMatch = lower.match(/(\d+(?:\.\d+)?)\s*(gb|mb|kb)/i)
    if (sizeMatch) {
      return `${sizeMatch[1]}${sizeMatch[2].toLowerCase()}`
    }

    // Look for known datasets
    if (lower.includes('clickbench')) return 'clickbench'
    if (lower.includes('tpc-h')) return 'tpc-h'
    if (lower.includes('ecommerce')) return 'ecommerce'

    return this.options.defaultDataset ?? DEFAULT_OPTIONS.defaultDataset
  }

  /**
   * Detect the execution environment.
   */
  private detectEnvironment(): BenchmarkEnvironment {
    // Check for Cloudflare Workers environment
    if (typeof globalThis !== 'undefined') {
      const global = globalThis as Record<string, unknown>

      // Check for DO environment
      if (global.DurableObjectState) {
        return 'do'
      }

      // Check for Worker environment
      if (global.caches && typeof global.fetch === 'function' && !global.window) {
        return 'worker'
      }
    }

    // Check for container environment
    // Use globalThis to access process in a way that works across environments
    const globalAny = globalThis as { process?: { env?: Record<string, string | undefined> } }
    const env = globalAny.process?.env ?? {}
    if (env.CONTAINER_RUNTIME || env.KUBERNETES_SERVICE_HOST) {
      return 'container'
    }

    return this.options.defaultEnvironment ?? DEFAULT_OPTIONS.defaultEnvironment
  }

  /**
   * Print a summary of the benchmark results.
   */
  private printSummary(): void {
    if (this.results.length === 0) {
      console.log('\nNo benchmark results collected.')
      return
    }

    const totalTime = Date.now() - this.startTime
    const byDatabase = new Map<string, BenchmarkResult[]>()

    for (const result of this.results) {
      const existing = byDatabase.get(result.database) ?? []
      existing.push(result)
      byDatabase.set(result.database, existing)
    }

    console.log('\n' + '='.repeat(60))
    console.log('BENCHMARK RESULTS SUMMARY')
    console.log('='.repeat(60))
    console.log(`Run ID: ${this.runId}`)
    console.log(`Total benchmarks: ${this.results.length}`)
    console.log(`Total time: ${(totalTime / 1000).toFixed(2)}s`)
    console.log(`Output: ${this.options.outputPath}`)

    if (this.gitInfo) {
      console.log(`Git: ${this.gitInfo.branch} @ ${this.gitInfo.sha.slice(0, 7)}`)
    }

    console.log('')

    for (const [database, results] of byDatabase) {
      console.log(`${database}:`)

      for (const result of results.slice(0, 5)) {
        const shortName = result.benchmark.split('/').slice(-2).join('/')
        console.log(
          `  ${shortName.padEnd(40)} ` +
          `p50: ${result.p50_ms.toFixed(2)}ms, ` +
          `p99: ${result.p99_ms.toFixed(2)}ms, ` +
          `ops/s: ${result.ops_per_sec.toFixed(0)}`
        )
      }

      if (results.length > 5) {
        console.log(`  ... and ${results.length - 5} more`)
      }

      console.log('')
    }

    console.log('='.repeat(60))
  }
}

/**
 * Create a BenchReporter instance.
 * Convenience function for vitest config.
 */
export function createBenchReporter(
  options: Partial<BenchReporterOptions> = {}
): BenchReporter {
  return new BenchReporter(options)
}

/**
 * Helper to create a benchmark result manually.
 * Useful for custom benchmark runners or tests.
 */
export function createBenchmarkResult(
  partial: Partial<BenchmarkResult> & Pick<BenchmarkResult, 'benchmark' | 'database'>
): BenchmarkResult {
  return {
    dataset: 'default',
    p50_ms: 0,
    p99_ms: 0,
    min_ms: 0,
    max_ms: 0,
    mean_ms: 0,
    ops_per_sec: 0,
    iterations: 0,
    vfs_reads: 0,
    vfs_writes: 0,
    vfs_bytes_read: 0,
    vfs_bytes_written: 0,
    timestamp: new Date().toISOString(),
    environment: 'local',
    run_id: generateRunId(),
    ...partial,
  }
}

/**
 * Calculate statistics from an array of samples (timing values).
 * Useful when implementing custom benchmarks.
 */
export function calculateStats(samples: number[]): {
  min: number
  max: number
  mean: number
  median: number
  p50: number
  p99: number
  stddev: number
} {
  if (samples.length === 0) {
    return { min: 0, max: 0, mean: 0, median: 0, p50: 0, p99: 0, stddev: 0 }
  }

  const sorted = [...samples].sort((a, b) => a - b)
  const n = sorted.length

  const min = sorted[0]
  const max = sorted[n - 1]
  const mean = samples.reduce((a, b) => a + b, 0) / n
  const median = sorted[Math.floor(n / 2)]
  const p50 = sorted[Math.floor(n * 0.5)]
  const p99 = sorted[Math.floor(n * 0.99)]

  const variance = samples.reduce((sum, v) => sum + Math.pow(v - mean, 2), 0) / n
  const stddev = Math.sqrt(variance)

  return { min, max, mean, median, p50, p99, stddev }
}

/**
 * Measure execution time of an async function.
 * Returns timing in milliseconds.
 */
export async function measure<T>(
  fn: () => Promise<T>,
  iterations: number = 1
): Promise<{ result: T; samples: number[]; stats: ReturnType<typeof calculateStats> }> {
  const samples: number[] = []
  let result: T

  for (let i = 0; i < iterations; i++) {
    const start = performance.now()
    result = await fn()
    const end = performance.now()
    samples.push(end - start)
  }

  return {
    result: result!,
    samples,
    stats: calculateStats(samples),
  }
}
