/**
 * VFS Instrumentation Module
 *
 * Provides tools for tracking and analyzing Cloudflare Durable Object
 * storage costs and performance characteristics.
 *
 * Key components:
 * - VFSInstrument: Wraps DO storage/sql to track blob operations
 * - CloudflareAnalytics: Pull metrics from wrangler tail and Analytics Engine
 * - StorageMetricsAggregator: Aggregate metrics for batch analysis
 * - JSONL Writer/Reader: Standardized benchmark output format
 * - BenchReporter: Vitest reporter plugin for automatic result capture
 *
 * Usage:
 * ```typescript
 * import { VFSInstrument } from './instrumentation'
 *
 * const instrument = new VFSInstrument({ trackOperations: true })
 * const wrappedStorage = instrument.wrapStorage(state.storage)
 * const wrappedSql = instrument.wrapSql(state.storage.sql)
 *
 * // ... perform operations ...
 *
 * const metrics = instrument.getMetrics()
 * console.log(metrics.blobsWritten, metrics.writeAmplification)
 * ```
 *
 * JSONL Output Usage:
 * ```typescript
 * import { JSONLWriter, JSONLReader, BenchReporter } from './instrumentation'
 *
 * // Write benchmark results
 * const writer = new JSONLWriter({ outputPath: './results.jsonl' })
 * writer.write(result)
 * await writer.close()
 *
 * // Read and filter results
 * const reader = new JSONLReader({ inputPath: './results.jsonl' })
 * const filtered = await reader.read({ database: 'db4' })
 *
 * // Use with vitest (in vitest.config.ts)
 * import { BenchReporter } from './instrumentation'
 * export default defineConfig({
 *   test: {
 *     benchmark: {
 *       reporters: ['default', new BenchReporter()],
 *     },
 *   },
 * })
 * ```
 */

// VFS Metrics
export {
  VFSInstrument,
  createMockStorage,
  createMockSqlStorage,
  type VFSMetrics,
  type VFSOperation,
  type VFSInstrumentOptions,
} from './vfs-metrics'

// Cloudflare Analytics
export {
  captureFromTail,
  parseTailEvent,
  queryAnalyticsEngine,
  extractExecutionMetrics,
  createMetricsCollector,
  StorageMetricsAggregator,
  checkTailSqlMetricsSupport,
  type CloudflareMetrics,
  type TailEvent,
  type AnalyticsEngineData,
} from './cloudflare-analytics'

// JSONL Benchmark Types
export {
  type BenchmarkResult,
  type PartialBenchmarkResult,
  type BenchmarkFilter,
  type BenchmarkSummary,
  type BenchmarkComparison,
  type AggregationOptions,
  type BenchmarkEnvironment,
  type ContainerSize,
  type WriterOptions,
  type ReaderOptions,
  type VitestBenchmarkTask,
  type BenchReporterOptions,
} from './types'

// JSONL Writer
export {
  JSONLWriter,
  createWriter,
  writeResult,
  writeResults,
  generateRunId,
  getGitInfo,
  getFileSize,
  rotateIfNeeded,
} from './writer'

// JSONL Reader
export {
  JSONLReader,
  createReader,
  readResults,
  readRecentResults,
  findFastest,
  compareToBaseline,
  formatComparison,
  formatSummary,
} from './reader'

// Vitest Benchmark Reporter
export {
  BenchReporter,
  createBenchReporter,
  registerVfsMetrics,
  clearVfsMetrics,
  vfsMetricsRegistry,
  createBenchmarkResult,
  calculateStats,
  measure,
} from './bench-reporter'
