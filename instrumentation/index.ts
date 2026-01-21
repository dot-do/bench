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
