/**
 * JSONL Reader for Benchmark Results
 *
 * Provides efficient reading, filtering, and aggregation of
 * benchmark results from JSONL files.
 *
 * Features:
 * - Streaming and batch reading modes
 * - Powerful filtering with predicates
 * - Sorting and pagination
 * - Aggregation and grouping
 * - Multi-file support
 */

import { readFile, stat } from 'node:fs/promises'
import { createReadStream } from 'node:fs'
import { createInterface } from 'node:readline'
import type {
  BenchmarkResult,
  BenchmarkFilter,
  BenchmarkSummary,
  AggregationOptions,
  ReaderOptions,
  BenchmarkComparison,
} from './types'

/**
 * Default reader options.
 */
const DEFAULT_OPTIONS: Partial<ReaderOptions> = {
  streaming: false,
  sortDirection: 'desc',
}

/**
 * JSONL Reader for benchmark results.
 *
 * @example
 * ```typescript
 * const reader = new JSONLReader({ inputPath: './results.jsonl' })
 *
 * // Read all results
 * const all = await reader.read()
 *
 * // Read with filtering
 * const filtered = await reader.read({
 *   database: 'db4',
 *   environment: 'worker',
 * })
 *
 * // Aggregate results
 * const summary = await reader.aggregate({
 *   groupBy: ['database', 'benchmark'],
 * })
 * ```
 */
export class JSONLReader {
  private options: ReaderOptions

  constructor(options: ReaderOptions) {
    this.options = {
      ...DEFAULT_OPTIONS,
      ...options,
    }
  }

  /**
   * Read all results, optionally filtered.
   */
  async read(filter?: BenchmarkFilter): Promise<BenchmarkResult[]> {
    const paths = Array.isArray(this.options.inputPath)
      ? this.options.inputPath
      : [this.options.inputPath]

    let results: BenchmarkResult[] = []

    for (const path of paths) {
      const fileResults = await this.readFile(path)
      results = results.concat(fileResults)
    }

    // Apply filter
    if (filter) {
      results = this.applyFilter(results, filter)
    }

    // Apply sorting
    if (this.options.sortBy) {
      results = this.sortResults(results, this.options.sortBy, this.options.sortDirection)
    }

    // Apply offset
    if (this.options.offset && this.options.offset > 0) {
      results = results.slice(this.options.offset)
    }

    // Apply limit
    if (this.options.limit && this.options.limit > 0) {
      results = results.slice(0, this.options.limit)
    }

    return results
  }

  /**
   * Stream results for memory-efficient processing.
   * Yields results one at a time.
   */
  async *stream(filter?: BenchmarkFilter): AsyncGenerator<BenchmarkResult> {
    const paths = Array.isArray(this.options.inputPath)
      ? this.options.inputPath
      : [this.options.inputPath]

    let count = 0
    let skipped = 0
    const offset = this.options.offset ?? 0
    const limit = this.options.limit ?? Infinity

    for (const path of paths) {
      const stream = createReadStream(path, { encoding: 'utf-8' })
      const rl = createInterface({
        input: stream,
        crlfDelay: Infinity,
      })

      for await (const line of rl) {
        if (!line.trim()) continue

        try {
          const result = JSON.parse(line) as BenchmarkResult

          // Apply filter
          if (filter && !this.matchesFilter(result, filter)) {
            continue
          }

          // Handle offset
          if (skipped < offset) {
            skipped++
            continue
          }

          // Handle limit
          if (count >= limit) {
            rl.close()
            return
          }

          yield result
          count++
        } catch (error) {
          console.warn('JSONLReader: Failed to parse line:', error)
        }
      }
    }
  }

  /**
   * Get the first matching result.
   */
  async first(filter?: BenchmarkFilter): Promise<BenchmarkResult | null> {
    for await (const result of this.stream(filter)) {
      return result
    }
    return null
  }

  /**
   * Get the last matching result.
   */
  async last(filter?: BenchmarkFilter): Promise<BenchmarkResult | null> {
    const results = await this.read(filter)
    return results[results.length - 1] ?? null
  }

  /**
   * Count results matching filter.
   */
  async count(filter?: BenchmarkFilter): Promise<number> {
    let count = 0
    for await (const _ of this.stream(filter)) {
      count++
    }
    return count
  }

  /**
   * Check if any results match filter.
   */
  async exists(filter?: BenchmarkFilter): Promise<boolean> {
    for await (const _ of this.stream(filter)) {
      return true
    }
    return false
  }

  /**
   * Get unique values for a field.
   */
  async distinct<K extends keyof BenchmarkResult>(
    field: K,
    filter?: BenchmarkFilter
  ): Promise<Array<BenchmarkResult[K]>> {
    const values = new Set<BenchmarkResult[K]>()

    for await (const result of this.stream(filter)) {
      values.add(result[field])
    }

    return Array.from(values)
  }

  /**
   * Aggregate results by groups.
   */
  async aggregate(
    options: AggregationOptions,
    filter?: BenchmarkFilter
  ): Promise<BenchmarkSummary[]> {
    const groups = new Map<string, BenchmarkResult[]>()

    for await (const result of this.stream(filter)) {
      // Build group key
      const keyParts = options.groupBy.map(field => String(result[field]))
      const key = keyParts.join('|')

      const group = groups.get(key) ?? []
      group.push(result)
      groups.set(key, group)
    }

    // Calculate summaries
    const summaries: BenchmarkSummary[] = []

    for (const [key, results] of groups) {
      const keyParts = key.split('|')
      const group: Record<string, string | number> = {}

      options.groupBy.forEach((field, i) => {
        group[field] = keyParts[i]
      })

      summaries.push(this.summarizeGroup(group, results))
    }

    return summaries
  }

  /**
   * Compare two runs.
   */
  async compare(
    baselineRunId: string,
    currentRunId: string
  ): Promise<BenchmarkComparison[]> {
    const baselineResults = await this.read({ run_id: baselineRunId })
    const currentResults = await this.read({ run_id: currentRunId })

    // Group by benchmark + database
    const baselineByKey = new Map<string, BenchmarkResult>()
    for (const r of baselineResults) {
      baselineByKey.set(`${r.benchmark}|${r.database}`, r)
    }

    const comparisons: BenchmarkComparison[] = []

    for (const current of currentResults) {
      const key = `${current.benchmark}|${current.database}`
      const baseline = baselineByKey.get(key)

      if (!baseline) continue

      const p50Change = ((current.p50_ms - baseline.p50_ms) / baseline.p50_ms) * 100
      const p99Change = ((current.p99_ms - baseline.p99_ms) / baseline.p99_ms) * 100
      const opsChange = ((current.ops_per_sec - baseline.ops_per_sec) / baseline.ops_per_sec) * 100

      // Consider significant if > 5% change
      const significant = Math.abs(p50Change) > 5 || Math.abs(p99Change) > 5

      // Regression if slower by > 5%
      const regression = p50Change > 5 || p99Change > 5

      comparisons.push({
        benchmark: current.benchmark,
        database: current.database,
        baseline,
        current,
        p50_change_pct: p50Change,
        p99_change_pct: p99Change,
        ops_per_sec_change_pct: opsChange,
        significant,
        regression,
      })
    }

    return comparisons
  }

  /**
   * Get statistics for a field.
   */
  async stats(
    field: 'p50_ms' | 'p99_ms' | 'mean_ms' | 'ops_per_sec',
    filter?: BenchmarkFilter
  ): Promise<{
    min: number
    max: number
    mean: number
    median: number
    stddev: number
    count: number
  }> {
    const values: number[] = []

    for await (const result of this.stream(filter)) {
      values.push(result[field])
    }

    if (values.length === 0) {
      return { min: 0, max: 0, mean: 0, median: 0, stddev: 0, count: 0 }
    }

    values.sort((a, b) => a - b)

    const min = values[0]
    const max = values[values.length - 1]
    const mean = values.reduce((a, b) => a + b, 0) / values.length
    const median = values[Math.floor(values.length / 2)]
    const variance = values.reduce((sum, v) => sum + Math.pow(v - mean, 2), 0) / values.length
    const stddev = Math.sqrt(variance)

    return { min, max, mean, median, stddev, count: values.length }
  }

  // Private helpers

  private async readFile(path: string): Promise<BenchmarkResult[]> {
    // Check file exists
    try {
      await stat(path)
    } catch {
      return []
    }

    const content = await readFile(path, 'utf-8')
    const lines = content.split('\n').filter((line: string) => line.trim())
    const results: BenchmarkResult[] = []

    for (const line of lines) {
      try {
        results.push(JSON.parse(line) as BenchmarkResult)
      } catch (error) {
        console.warn('JSONLReader: Failed to parse line:', error)
      }
    }

    return results
  }

  private applyFilter(results: BenchmarkResult[], filter: BenchmarkFilter): BenchmarkResult[] {
    return results.filter(r => this.matchesFilter(r, filter))
  }

  private matchesFilter(result: BenchmarkResult, filter: BenchmarkFilter): boolean {
    // Benchmark filter (string or regex)
    if (filter.benchmark) {
      if (filter.benchmark instanceof RegExp) {
        if (!filter.benchmark.test(result.benchmark)) return false
      } else {
        if (result.benchmark !== filter.benchmark) return false
      }
    }

    // Database filter
    if (filter.database) {
      const dbs = Array.isArray(filter.database) ? filter.database : [filter.database]
      if (!dbs.includes(result.database)) return false
    }

    // Dataset filter
    if (filter.dataset) {
      const datasets = Array.isArray(filter.dataset) ? filter.dataset : [filter.dataset]
      if (!datasets.includes(result.dataset)) return false
    }

    // Environment filter
    if (filter.environment) {
      const envs = Array.isArray(filter.environment) ? filter.environment : [filter.environment]
      if (!envs.includes(result.environment)) return false
    }

    // Run ID filter
    if (filter.run_id && result.run_id !== filter.run_id) {
      return false
    }

    // Git SHA filter
    if (filter.git_sha && result.git_sha !== filter.git_sha) {
      return false
    }

    // Timestamp filters
    if (filter.timestamp_after) {
      if (result.timestamp < filter.timestamp_after) return false
    }
    if (filter.timestamp_before) {
      if (result.timestamp > filter.timestamp_before) return false
    }

    // Tags filter
    if (filter.tags) {
      if (!result.tags) return false
      for (const [key, value] of Object.entries(filter.tags)) {
        if (result.tags[key] !== value) return false
      }
    }

    // Custom predicate
    if (filter.predicate && !filter.predicate(result)) {
      return false
    }

    return true
  }

  private sortResults(
    results: BenchmarkResult[],
    sortBy: keyof BenchmarkResult,
    direction: 'asc' | 'desc' = 'desc'
  ): BenchmarkResult[] {
    return results.sort((a, b) => {
      const aVal = a[sortBy] as string | number | undefined
      const bVal = b[sortBy] as string | number | undefined

      if (aVal === undefined && bVal === undefined) return 0
      if (aVal === undefined) return 1
      if (bVal === undefined) return -1

      let cmp = 0
      if (typeof aVal === 'string' && typeof bVal === 'string') {
        cmp = aVal.localeCompare(bVal)
      } else if (typeof aVal === 'number' && typeof bVal === 'number') {
        cmp = aVal - bVal
      }

      return direction === 'desc' ? -cmp : cmp
    })
  }

  private summarizeGroup(
    group: Record<string, string | number>,
    results: BenchmarkResult[]
  ): BenchmarkSummary {
    const calcStats = (values: number[]): { min: number; max: number; mean: number; median: number } => {
      if (values.length === 0) return { min: 0, max: 0, mean: 0, median: 0 }
      const sorted = [...values].sort((a, b) => a - b)
      return {
        min: sorted[0],
        max: sorted[sorted.length - 1],
        mean: sorted.reduce((a, b) => a + b, 0) / sorted.length,
        median: sorted[Math.floor(sorted.length / 2)],
      }
    }

    return {
      group,
      count: results.length,
      timing: {
        p50_ms: calcStats(results.map(r => r.p50_ms)),
        p99_ms: calcStats(results.map(r => r.p99_ms)),
        mean_ms: calcStats(results.map(r => r.mean_ms)),
      },
      throughput: {
        ops_per_sec: calcStats(results.map(r => r.ops_per_sec)),
      },
      vfs: {
        reads: {
          total: results.reduce((sum, r) => sum + r.vfs_reads, 0),
          mean: results.reduce((sum, r) => sum + r.vfs_reads, 0) / results.length,
        },
        writes: {
          total: results.reduce((sum, r) => sum + r.vfs_writes, 0),
          mean: results.reduce((sum, r) => sum + r.vfs_writes, 0) / results.length,
        },
        bytes_read: {
          total: results.reduce((sum, r) => sum + r.vfs_bytes_read, 0),
          mean: results.reduce((sum, r) => sum + r.vfs_bytes_read, 0) / results.length,
        },
        bytes_written: {
          total: results.reduce((sum, r) => sum + r.vfs_bytes_written, 0),
          mean: results.reduce((sum, r) => sum + r.vfs_bytes_written, 0) / results.length,
        },
      },
    }
  }
}

/**
 * Create a JSONL reader with sensible defaults.
 */
export function createReader(
  inputPath: string | string[],
  options: Partial<Omit<ReaderOptions, 'inputPath'>> = {}
): JSONLReader {
  return new JSONLReader({
    inputPath,
    ...options,
  })
}

/**
 * Read all results from a file (one-shot).
 */
export async function readResults(
  inputPath: string | string[],
  filter?: BenchmarkFilter
): Promise<BenchmarkResult[]> {
  const reader = createReader(inputPath)
  return reader.read(filter)
}

/**
 * Read the most recent N results.
 */
export async function readRecentResults(
  inputPath: string,
  count: number,
  filter?: BenchmarkFilter
): Promise<BenchmarkResult[]> {
  const reader = createReader(inputPath, {
    sortBy: 'timestamp',
    sortDirection: 'desc',
    limit: count,
  })
  return reader.read(filter)
}

/**
 * Find the fastest result for a benchmark.
 */
export async function findFastest(
  inputPath: string,
  benchmark: string
): Promise<BenchmarkResult | null> {
  const reader = createReader(inputPath, {
    sortBy: 'p50_ms',
    sortDirection: 'asc',
    limit: 1,
  })
  return reader.first({ benchmark })
}

/**
 * Compare latest results against a baseline run.
 */
export async function compareToBaseline(
  inputPath: string,
  baselineRunId: string
): Promise<{
  regressions: BenchmarkComparison[]
  improvements: BenchmarkComparison[]
  unchanged: BenchmarkComparison[]
}> {
  const reader = createReader(inputPath)

  // Find the most recent run that isn't the baseline
  const recentResults = await reader.read()
  const runIds = [...new Set(recentResults.map(r => r.run_id))]
  const currentRunId = runIds.find(id => id !== baselineRunId)

  if (!currentRunId) {
    return { regressions: [], improvements: [], unchanged: [] }
  }

  const comparisons = await reader.compare(baselineRunId, currentRunId)

  return {
    regressions: comparisons.filter(c => c.regression),
    improvements: comparisons.filter(c => c.ops_per_sec_change_pct > 5),
    unchanged: comparisons.filter(c => !c.significant),
  }
}

/**
 * Format a comparison result for display.
 */
export function formatComparison(comparison: BenchmarkComparison): string {
  const sign = (n: number) => (n >= 0 ? '+' : '')
  const pct = (n: number) => `${sign(n)}${n.toFixed(1)}%`

  const status = comparison.regression
    ? '[REGRESSION]'
    : comparison.ops_per_sec_change_pct > 5
      ? '[IMPROVED]'
      : '[OK]'

  return [
    `${status} ${comparison.benchmark} (${comparison.database})`,
    `  p50:  ${comparison.baseline.p50_ms.toFixed(2)}ms -> ${comparison.current.p50_ms.toFixed(2)}ms (${pct(comparison.p50_change_pct)})`,
    `  p99:  ${comparison.baseline.p99_ms.toFixed(2)}ms -> ${comparison.current.p99_ms.toFixed(2)}ms (${pct(comparison.p99_change_pct)})`,
    `  ops/s: ${comparison.baseline.ops_per_sec.toFixed(0)} -> ${comparison.current.ops_per_sec.toFixed(0)} (${pct(comparison.ops_per_sec_change_pct)})`,
  ].join('\n')
}

/**
 * Format a summary for display.
 */
export function formatSummary(summary: BenchmarkSummary): string {
  const groupStr = Object.entries(summary.group)
    .map(([k, v]) => `${k}=${v}`)
    .join(', ')

  return [
    `Group: ${groupStr} (${summary.count} results)`,
    `  p50:  min=${summary.timing.p50_ms.min.toFixed(2)}ms, max=${summary.timing.p50_ms.max.toFixed(2)}ms, mean=${summary.timing.p50_ms.mean.toFixed(2)}ms`,
    `  p99:  min=${summary.timing.p99_ms.min.toFixed(2)}ms, max=${summary.timing.p99_ms.max.toFixed(2)}ms, mean=${summary.timing.p99_ms.mean.toFixed(2)}ms`,
    `  ops/s: min=${summary.throughput.ops_per_sec.min.toFixed(0)}, max=${summary.throughput.ops_per_sec.max.toFixed(0)}, mean=${summary.throughput.ops_per_sec.mean.toFixed(0)}`,
    `  VFS: ${summary.vfs.reads.total} reads, ${summary.vfs.writes.total} writes`,
  ].join('\n')
}
