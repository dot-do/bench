/**
 * JSONL Writer for Benchmark Results
 *
 * Provides buffered, append-mode writing of benchmark results
 * to newline-delimited JSON files.
 *
 * Features:
 * - Buffered writes for performance
 * - Auto-flush on interval
 * - Append mode support
 * - Atomic file operations
 * - Memory-efficient streaming
 */

import { writeFile, appendFile, mkdir, stat } from 'node:fs/promises'
import { dirname } from 'node:path'
import type {
  BenchmarkResult,
  PartialBenchmarkResult,
  WriterOptions,
} from './types'

/**
 * Default writer options.
 */
const DEFAULT_OPTIONS: Required<Omit<WriterOptions, 'outputPath'>> = {
  append: true,
  bufferSize: 100,
  prettyPrint: false,
  flushInterval: 5000,
}

/**
 * JSONL Writer for benchmark results.
 *
 * Buffers results in memory and periodically flushes to disk.
 * Supports both append and overwrite modes.
 *
 * @example
 * ```typescript
 * const writer = new JSONLWriter({ outputPath: './results.jsonl' })
 *
 * // Write individual results
 * writer.write(result)
 *
 * // Write multiple results
 * writer.writeMany(results)
 *
 * // Ensure all data is written
 * await writer.flush()
 *
 * // Clean up
 * await writer.close()
 * ```
 */
export class JSONLWriter {
  private options: Required<WriterOptions>
  private buffer: BenchmarkResult[] = []
  private flushTimer: ReturnType<typeof setInterval> | null = null
  private initialized = false
  private writePromise: Promise<void> | null = null
  private totalWritten = 0

  constructor(options: WriterOptions) {
    this.options = {
      ...DEFAULT_OPTIONS,
      ...options,
    }
  }

  /**
   * Initialize the writer.
   * Creates the output directory if needed and sets up auto-flush.
   */
  async init(): Promise<void> {
    if (this.initialized) return

    // Ensure output directory exists
    const dir = dirname(this.options.outputPath)
    await mkdir(dir, { recursive: true }).catch(() => {
      // Directory may already exist
    })

    // If not appending, create/truncate the file
    if (!this.options.append) {
      await writeFile(this.options.outputPath, '', 'utf-8')
    }

    // Set up auto-flush interval
    if (this.options.flushInterval > 0) {
      this.flushTimer = setInterval(() => {
        void this.flush()
      }, this.options.flushInterval)

      // Don't prevent process exit (in Node.js environments)
      // Cast to unknown first to handle both browser and Node.js timer types
      const timer = this.flushTimer as unknown as { unref?: () => void }
      if (typeof timer.unref === 'function') {
        timer.unref()
      }
    }

    this.initialized = true
  }

  /**
   * Write a single benchmark result.
   * Result is buffered and written when buffer is full or on flush.
   */
  write(result: BenchmarkResult): void {
    this.buffer.push(result)

    // Flush if buffer is full
    if (this.buffer.length >= this.options.bufferSize) {
      void this.flush()
    }
  }

  /**
   * Write multiple benchmark results.
   */
  writeMany(results: BenchmarkResult[]): void {
    for (const result of results) {
      this.write(result)
    }
  }

  /**
   * Write a partial result (for incremental updates).
   * Requires a complete BenchmarkResult to be constructed later.
   */
  writePartial(partial: PartialBenchmarkResult): void {
    // Store partial for later completion
    // For now, we require full results
    console.warn('writePartial: Partial writes not yet implemented, result will be incomplete')

    const result: BenchmarkResult = {
      // Required fields first
      benchmark: partial.benchmark,
      run_id: partial.run_id,
      // Default values for required fields
      database: partial.database ?? 'unknown',
      dataset: partial.dataset ?? 'default',
      p50_ms: partial.p50_ms ?? 0,
      p99_ms: partial.p99_ms ?? 0,
      min_ms: partial.min_ms ?? 0,
      max_ms: partial.max_ms ?? 0,
      mean_ms: partial.mean_ms ?? 0,
      ops_per_sec: partial.ops_per_sec ?? 0,
      iterations: partial.iterations ?? 0,
      vfs_reads: partial.vfs_reads ?? 0,
      vfs_writes: partial.vfs_writes ?? 0,
      vfs_bytes_read: partial.vfs_bytes_read ?? 0,
      vfs_bytes_written: partial.vfs_bytes_written ?? 0,
      timestamp: partial.timestamp ?? new Date().toISOString(),
      environment: partial.environment ?? 'local',
      // Optional fields
      stddev_ms: partial.stddev_ms,
      total_duration_ms: partial.total_duration_ms,
      sql_rows_read: partial.sql_rows_read,
      sql_rows_written: partial.sql_rows_written,
      write_amplification: partial.write_amplification,
      read_amplification: partial.read_amplification,
      estimated_cost_usd: partial.estimated_cost_usd,
      container_size: partial.container_size,
      wasm_size_bytes: partial.wasm_size_bytes,
      cold_start_ms: partial.cold_start_ms,
      memory_bytes: partial.memory_bytes,
      cpu_time_ms: partial.cpu_time_ms,
      colo: partial.colo,
      git_sha: partial.git_sha,
      git_branch: partial.git_branch,
      vitest_task: partial.vitest_task,
      tags: partial.tags,
      notes: partial.notes,
    }

    this.write(result)
  }

  /**
   * Flush buffered results to disk.
   * Returns a promise that resolves when flush is complete.
   */
  async flush(): Promise<void> {
    // If already flushing, wait for that to complete
    if (this.writePromise) {
      await this.writePromise
    }

    // Nothing to flush
    if (this.buffer.length === 0) return

    // Take buffer contents
    const toWrite = this.buffer
    this.buffer = []

    // Convert to JSONL
    const lines = toWrite.map(result => {
      if (this.options.prettyPrint) {
        return JSON.stringify(result, null, 2)
      }
      return JSON.stringify(result)
    })

    const content = lines.join('\n') + '\n'

    // Write to file
    this.writePromise = this.doWrite(content)
    await this.writePromise
    this.writePromise = null

    this.totalWritten += toWrite.length
  }

  /**
   * Internal write operation.
   */
  private async doWrite(content: string): Promise<void> {
    await this.init()

    try {
      await appendFile(this.options.outputPath, content, 'utf-8')
    } catch (error) {
      console.error('JSONLWriter: Failed to write results:', error)
      throw error
    }
  }

  /**
   * Get the number of results written so far.
   */
  getWrittenCount(): number {
    return this.totalWritten
  }

  /**
   * Get the number of results currently in buffer.
   */
  getBufferSize(): number {
    return this.buffer.length
  }

  /**
   * Close the writer.
   * Flushes any remaining results and cleans up resources.
   */
  async close(): Promise<void> {
    // Stop auto-flush
    if (this.flushTimer) {
      clearInterval(this.flushTimer)
      this.flushTimer = null
    }

    // Final flush
    await this.flush()
  }

  /**
   * Get the output path.
   */
  getOutputPath(): string {
    return this.options.outputPath
  }
}

/**
 * Create a JSONL writer with sensible defaults.
 *
 * @param outputPath - Path to output file
 * @param options - Additional options
 */
export function createWriter(
  outputPath: string,
  options: Partial<Omit<WriterOptions, 'outputPath'>> = {}
): JSONLWriter {
  return new JSONLWriter({
    outputPath,
    ...options,
  })
}

/**
 * Write a single result to a file (one-shot).
 * Useful for simple scripts that don't need buffering.
 */
export async function writeResult(
  result: BenchmarkResult,
  outputPath: string,
  options: { append?: boolean; prettyPrint?: boolean } = {}
): Promise<void> {
  const { append = true, prettyPrint = false } = options

  const content = prettyPrint
    ? JSON.stringify(result, null, 2) + '\n'
    : JSON.stringify(result) + '\n'

  // Ensure directory exists
  const dir = dirname(outputPath)
  await mkdir(dir, { recursive: true }).catch(() => {})

  if (append) {
    await appendFile(outputPath, content, 'utf-8')
  } else {
    await writeFile(outputPath, content, 'utf-8')
  }
}

/**
 * Write multiple results to a file (one-shot).
 */
export async function writeResults(
  results: BenchmarkResult[],
  outputPath: string,
  options: { append?: boolean; prettyPrint?: boolean } = {}
): Promise<void> {
  const { append = true, prettyPrint = false } = options

  const lines = results.map(r =>
    prettyPrint ? JSON.stringify(r, null, 2) : JSON.stringify(r)
  )
  const content = lines.join('\n') + '\n'

  const dir = dirname(outputPath)
  await mkdir(dir, { recursive: true }).catch(() => {})

  if (append) {
    await appendFile(outputPath, content, 'utf-8')
  } else {
    await writeFile(outputPath, content, 'utf-8')
  }
}

/**
 * Generate a unique run ID.
 * Format: {timestamp}-{random}
 */
export function generateRunId(): string {
  const timestamp = Date.now().toString(36)
  const random = Math.random().toString(36).substring(2, 8)
  return `${timestamp}-${random}`
}

/**
 * Get git information for metadata.
 * Returns null if not in a git repo or git is unavailable.
 */
export async function getGitInfo(): Promise<{ sha: string; branch: string } | null> {
  try {
    // Dynamic import to avoid bundling issues
    const { execSync } = await import('node:child_process')

    const sha = execSync('git rev-parse HEAD', { encoding: 'utf-8' }).trim()
    const branch = execSync('git rev-parse --abbrev-ref HEAD', { encoding: 'utf-8' }).trim()

    return { sha, branch }
  } catch {
    return null
  }
}

/**
 * Get file size in bytes.
 * Returns 0 if file doesn't exist.
 */
export async function getFileSize(path: string): Promise<number> {
  try {
    const stats = await stat(path)
    return stats.size
  } catch {
    return 0
  }
}

/**
 * Rotate output file if it exceeds size limit.
 * Renames existing file with timestamp suffix.
 */
export async function rotateIfNeeded(
  outputPath: string,
  maxSizeBytes: number = 100 * 1024 * 1024 // 100MB default
): Promise<boolean> {
  const size = await getFileSize(outputPath)

  if (size > maxSizeBytes) {
    const { rename } = await import('node:fs/promises')
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-')
    const rotatedPath = outputPath.replace(/\.jsonl$/, `-${timestamp}.jsonl`)

    await rename(outputPath, rotatedPath)
    return true
  }

  return false
}
