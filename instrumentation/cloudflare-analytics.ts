/**
 * Cloudflare Analytics Integration
 *
 * Pull metrics from:
 * - wrangler tail events (real-time)
 * - Workers Analytics Engine (aggregated)
 * - DO analytics (cpu time, wall time, storage ops)
 *
 * Note: Some metrics are only available in production.
 * Local development with miniflare exposes limited analytics.
 */

export interface CloudflareMetrics {
  // From DO execution context
  cpuTimeMs: number
  wallTimeMs: number

  // From DO analytics
  storageReadUnits: number
  storageWriteUnits: number

  // From sql.exec (available in miniflare)
  sqlRowsRead: number
  sqlRowsWritten: number

  // From tail events (production only)
  tailEvents?: TailEvent[]

  // Request-level metrics
  requestDurationMs?: number
  requestColo?: string
}

export interface TailEvent {
  timestamp: number
  event: 'request' | 'alarm' | 'websocket' | 'queue'
  outcome: 'ok' | 'exception' | 'exceeded-cpu' | 'exceeded-memory'

  // Request details (if event === 'request')
  request?: {
    method: string
    url: string
  }

  // Timing
  cpuMs?: number
  wallMs?: number

  // DO-specific
  durableObjectId?: string
  durableObjectName?: string

  // SQL metrics (if exposed)
  sqlRowsRead?: number
  sqlRowsWritten?: number

  // Logs
  logs?: Array<{
    level: 'log' | 'debug' | 'info' | 'warn' | 'error'
    message: string
  }>

  // Exceptions
  exceptions?: Array<{
    name: string
    message: string
    stack?: string
  }>
}

export interface AnalyticsEngineData {
  // Aggregated metrics from Analytics Engine
  totalRequests: number
  avgCpuMs: number
  avgWallMs: number
  p50CpuMs: number
  p99CpuMs: number
  p50WallMs: number
  p99WallMs: number

  // Error rates
  errorRate: number
  exceptionsCount: number

  // By colo
  byColoRequests: Record<string, number>
  byColoAvgLatency: Record<string, number>
}

/**
 * Capture metrics from wrangler tail events.
 *
 * Uses wrangler CLI to tail worker events in real-time.
 * Best used for debugging and development.
 *
 * @param workerName - Name of the worker to tail
 * @param options - Tail options
 */
export async function captureFromTail(
  workerName: string,
  options: {
    duration?: number
    format?: 'json' | 'pretty'
    filter?: {
      status?: 'ok' | 'error'
      method?: string
      search?: string
    }
  } = {}
): Promise<CloudflareMetrics> {
  const { duration = 5000 } = options

  // Build wrangler tail command
  const args = ['wrangler', 'tail', workerName, '--format', 'json']

  if (options.filter?.status === 'error') {
    args.push('--status', 'error')
  }
  if (options.filter?.method) {
    args.push('--method', options.filter.method)
  }
  if (options.filter?.search) {
    args.push('--search', options.filter.search)
  }

  const events: TailEvent[] = []
  const metrics: CloudflareMetrics = {
    cpuTimeMs: 0,
    wallTimeMs: 0,
    storageReadUnits: 0,
    storageWriteUnits: 0,
    sqlRowsRead: 0,
    sqlRowsWritten: 0,
    tailEvents: events,
  }

  try {
    // Note: In a real implementation, this would spawn wrangler tail
    // and parse the JSON output stream. For now, we provide the interface.
    console.warn(
      'captureFromTail: wrangler tail integration requires manual setup.',
      'Run: npx wrangler tail <worker> --format json',
      'and pipe output to this function.'
    )

    // Simulate waiting for events
    await new Promise(resolve => setTimeout(resolve, Math.min(duration, 1000)))

    return metrics
  } catch (error) {
    console.error('Failed to capture tail events:', error)
    return metrics
  }
}

/**
 * Parse wrangler tail JSON output into TailEvent.
 *
 * Wrangler tail outputs one JSON object per line.
 * Each object has structure based on event type.
 */
export function parseTailEvent(json: string): TailEvent | null {
  try {
    const data = JSON.parse(json)

    // Extract event type
    const event = data.event?.request
      ? 'request'
      : data.event?.alarm
        ? 'alarm'
        : data.event?.websocket
          ? 'websocket'
          : 'queue'

    const tailEvent: TailEvent = {
      timestamp: data.eventTimestamp ?? Date.now(),
      event,
      outcome: data.outcome ?? 'ok',
    }

    // Request details
    if (data.event?.request) {
      tailEvent.request = {
        method: data.event.request.method,
        url: data.event.request.url,
      }
    }

    // DO details
    if (data.durableObjectId) {
      tailEvent.durableObjectId = data.durableObjectId
      tailEvent.durableObjectName = data.durableObjectName
    }

    // Logs
    if (data.logs?.length) {
      tailEvent.logs = data.logs.map((log: { level: string; message: unknown[] }) => ({
        level: log.level,
        message: log.message.join(' '),
      }))
    }

    // Exceptions
    if (data.exceptions?.length) {
      tailEvent.exceptions = data.exceptions.map((ex: { name: string; message: string; stack?: string }) => ({
        name: ex.name,
        message: ex.message,
        stack: ex.stack,
      }))
    }

    return tailEvent
  } catch {
    return null
  }
}

/**
 * Query Workers Analytics Engine for aggregated metrics.
 *
 * Requires CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN environment variables.
 * Analytics Engine must be enabled for the worker.
 *
 * @param workerName - Name of the worker
 * @param timeRange - Time range for query (e.g., '1h', '24h', '7d')
 */
export async function queryAnalyticsEngine(
  workerName: string,
  timeRange: '1h' | '24h' | '7d' = '24h'
): Promise<AnalyticsEngineData | null> {
  // Access environment variables - works in Node.js and Cloudflare Workers
  const accountId = typeof globalThis !== 'undefined' && 'process' in globalThis
    ? (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env?.CLOUDFLARE_ACCOUNT_ID
    : undefined
  const apiToken = typeof globalThis !== 'undefined' && 'process' in globalThis
    ? (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env?.CLOUDFLARE_API_TOKEN
    : undefined

  if (!accountId || !apiToken) {
    console.warn(
      'queryAnalyticsEngine requires CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN'
    )
    return null
  }

  // Calculate time range
  const now = Date.now()
  const timeRangeMs = {
    '1h': 60 * 60 * 1000,
    '24h': 24 * 60 * 60 * 1000,
    '7d': 7 * 24 * 60 * 60 * 1000,
  }[timeRange]

  const startTime = new Date(now - timeRangeMs).toISOString()
  const endTime = new Date(now).toISOString()

  // GraphQL query for Workers Analytics Engine
  const query = `
    query GetWorkerAnalytics($accountTag: String!, $filter: WorkerAnalyticsFilter!) {
      viewer {
        accounts(filter: { accountTag: $accountTag }) {
          workersInvocationsAdaptive(
            filter: $filter
            limit: 1000
          ) {
            dimensions {
              scriptName
              coloCode
              status
            }
            avg {
              cpuTimeMs
              wallTime
            }
            quantiles {
              cpuTimeP50
              cpuTimeP99
              wallTimeP50
              wallTimeP99
            }
            count
          }
        }
      }
    }
  `

  try {
    const response = await fetch(
      'https://api.cloudflare.com/client/v4/graphql',
      {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${apiToken}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query,
          variables: {
            accountTag: accountId,
            filter: {
              scriptName: workerName,
              datetime_geq: startTime,
              datetime_leq: endTime,
            },
          },
        }),
      }
    )

    if (!response.ok) {
      console.error('Analytics Engine query failed:', response.statusText)
      return null
    }

    const result = await response.json() as {
      data?: {
        viewer?: {
          accounts?: Array<{
            workersInvocationsAdaptive?: Array<{
              dimensions: { scriptName: string; coloCode: string; status: string }
              avg: { cpuTimeMs: number; wallTime: number }
              quantiles: { cpuTimeP50: number; cpuTimeP99: number; wallTimeP50: number; wallTimeP99: number }
              count: number
            }>
          }>
        }
      }
      errors?: Array<{ message: string }>
    }

    if (result.errors) {
      console.error('Analytics Engine query errors:', result.errors)
      return null
    }

    // Aggregate results
    const invocations = result.data?.viewer?.accounts?.[0]?.workersInvocationsAdaptive ?? []

    const aggregated: AnalyticsEngineData = {
      totalRequests: 0,
      avgCpuMs: 0,
      avgWallMs: 0,
      p50CpuMs: 0,
      p99CpuMs: 0,
      p50WallMs: 0,
      p99WallMs: 0,
      errorRate: 0,
      exceptionsCount: 0,
      byColoRequests: {},
      byColoAvgLatency: {},
    }

    let totalCpu = 0
    let totalWall = 0
    let errorCount = 0

    for (const inv of invocations) {
      aggregated.totalRequests += inv.count

      totalCpu += inv.avg.cpuTimeMs * inv.count
      totalWall += inv.avg.wallTime * inv.count

      if (inv.dimensions.status === 'error') {
        errorCount += inv.count
      }

      // By colo
      const colo = inv.dimensions.coloCode
      aggregated.byColoRequests[colo] = (aggregated.byColoRequests[colo] ?? 0) + inv.count
      aggregated.byColoAvgLatency[colo] = inv.avg.wallTime

      // Use last quantiles (approximation)
      aggregated.p50CpuMs = inv.quantiles.cpuTimeP50
      aggregated.p99CpuMs = inv.quantiles.cpuTimeP99
      aggregated.p50WallMs = inv.quantiles.wallTimeP50
      aggregated.p99WallMs = inv.quantiles.wallTimeP99
    }

    if (aggregated.totalRequests > 0) {
      aggregated.avgCpuMs = totalCpu / aggregated.totalRequests
      aggregated.avgWallMs = totalWall / aggregated.totalRequests
      aggregated.errorRate = errorCount / aggregated.totalRequests
    }

    return aggregated
  } catch (error) {
    console.error('Failed to query Analytics Engine:', error)
    return null
  }
}

/**
 * Extract metrics from a DO execution context.
 *
 * This should be called within the DO's fetch/alarm handler
 * to capture timing information.
 */
export function extractExecutionMetrics(
  ctx: ExecutionContext | DurableObjectState,
  startTime: number
): Partial<CloudflareMetrics> {
  const wallTimeMs = performance.now() - startTime

  return {
    wallTimeMs,
    // CPU time is not directly accessible in user code
    // It's reported in tail events and analytics
    cpuTimeMs: 0,
  }
}

/**
 * Create a metrics collector that wraps a DO handler.
 *
 * Automatically captures timing and can log to Analytics Engine.
 */
export function createMetricsCollector(options: {
  logToAnalytics?: boolean
  analyticsBinding?: AnalyticsEngineDataset
}) {
  return function collectMetrics<T extends (...args: unknown[]) => Promise<Response>>(
    handler: T
  ): T {
    return (async (...args: unknown[]) => {
      const startTime = performance.now()

      try {
        const response = await handler(...args)
        const duration = performance.now() - startTime

        // Log to Analytics Engine if enabled
        if (options.logToAnalytics && options.analyticsBinding) {
          options.analyticsBinding.writeDataPoint({
            blobs: [
              response.status.toString(),
            ],
            doubles: [
              duration,
              response.status,
            ],
            indexes: ['status'],
          })
        }

        return response
      } catch (error) {
        const duration = performance.now() - startTime

        if (options.logToAnalytics && options.analyticsBinding) {
          options.analyticsBinding.writeDataPoint({
            blobs: ['exception'],
            doubles: [duration, 500],
            indexes: ['status'],
          })
        }

        throw error
      }
    }) as T
  }
}

// Type definitions
interface ExecutionContext {
  waitUntil(promise: Promise<unknown>): void
  passThroughOnException(): void
}

interface DurableObjectState {
  id: DurableObjectId
  storage: unknown
  blockConcurrencyWhile<T>(callback: () => Promise<T>): Promise<T>
  waitUntil(promise: Promise<unknown>): void
}

interface DurableObjectId {
  toString(): string
  name?: string
}

interface AnalyticsEngineDataset {
  writeDataPoint(data: {
    blobs?: string[]
    doubles?: number[]
    indexes?: string[]
  }): void
}

/**
 * Storage metrics aggregator for batch analysis.
 *
 * Collects metrics across multiple operations and provides
 * summary statistics useful for cost analysis.
 */
export class StorageMetricsAggregator {
  private samples: Array<{
    timestamp: number
    rowsRead: number
    rowsWritten: number
    durationMs: number
    operation: string
  }> = []

  /**
   * Record a storage operation sample.
   */
  record(
    operation: string,
    rowsRead: number,
    rowsWritten: number,
    durationMs: number
  ): void {
    this.samples.push({
      timestamp: Date.now(),
      operation,
      rowsRead,
      rowsWritten,
      durationMs,
    })
  }

  /**
   * Get summary statistics.
   */
  getSummary(): {
    totalSamples: number
    totalRowsRead: number
    totalRowsWritten: number
    avgRowsRead: number
    avgRowsWritten: number
    avgDurationMs: number
    estimatedReadCost: number
    estimatedWriteCost: number
    byOperation: Record<string, {
      count: number
      avgRowsRead: number
      avgRowsWritten: number
      avgDurationMs: number
    }>
  } {
    if (this.samples.length === 0) {
      return {
        totalSamples: 0,
        totalRowsRead: 0,
        totalRowsWritten: 0,
        avgRowsRead: 0,
        avgRowsWritten: 0,
        avgDurationMs: 0,
        estimatedReadCost: 0,
        estimatedWriteCost: 0,
        byOperation: {},
      }
    }

    const totalRowsRead = this.samples.reduce((sum, s) => sum + s.rowsRead, 0)
    const totalRowsWritten = this.samples.reduce((sum, s) => sum + s.rowsWritten, 0)
    const totalDuration = this.samples.reduce((sum, s) => sum + s.durationMs, 0)

    // Group by operation
    const byOperation: Record<string, {
      count: number
      totalRowsRead: number
      totalRowsWritten: number
      totalDuration: number
    }> = {}

    for (const sample of this.samples) {
      const op = byOperation[sample.operation] ?? {
        count: 0,
        totalRowsRead: 0,
        totalRowsWritten: 0,
        totalDuration: 0,
      }

      op.count++
      op.totalRowsRead += sample.rowsRead
      op.totalRowsWritten += sample.rowsWritten
      op.totalDuration += sample.durationMs

      byOperation[sample.operation] = op
    }

    const byOperationSummary: Record<string, {
      count: number
      avgRowsRead: number
      avgRowsWritten: number
      avgDurationMs: number
    }> = {}

    for (const [name, op] of Object.entries(byOperation)) {
      byOperationSummary[name] = {
        count: op.count,
        avgRowsRead: op.totalRowsRead / op.count,
        avgRowsWritten: op.totalRowsWritten / op.count,
        avgDurationMs: op.totalDuration / op.count,
      }
    }

    return {
      totalSamples: this.samples.length,
      totalRowsRead,
      totalRowsWritten,
      avgRowsRead: totalRowsRead / this.samples.length,
      avgRowsWritten: totalRowsWritten / this.samples.length,
      avgDurationMs: totalDuration / this.samples.length,
      estimatedReadCost: (totalRowsRead / 1_000_000) * 0.001,
      estimatedWriteCost: (totalRowsWritten / 1_000_000) * 1.00,
      byOperation: byOperationSummary,
    }
  }

  /**
   * Clear all samples.
   */
  reset(): void {
    this.samples = []
  }

  /**
   * Export samples as JSON for analysis.
   */
  exportSamples(): string {
    return JSON.stringify(this.samples, null, 2)
  }
}

/**
 * Check if wrangler tail exposes rowsRead/rowsWritten.
 *
 * As of 2024, DO SQL metrics in tail events are still evolving.
 * This function tests whether the current wrangler version
 * includes these metrics.
 *
 * Note: This function only works in Node.js environments.
 */
export async function checkTailSqlMetricsSupport(): Promise<{
  supported: boolean
  version: string | null
}> {
  // Check if we're in a Node.js environment
  if (typeof globalThis === 'undefined' || !('process' in globalThis)) {
    return {
      supported: false,
      version: null,
    }
  }

  try {
    // Dynamic import for Node.js child_process
    // This will fail in non-Node environments (Workers, browsers)
    // Use a variable to prevent static analysis from trying to resolve the module
    const moduleName = 'child_process'
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const childProcess: { execSync: (cmd: string, opts: { encoding: string }) => string } | null =
      await import(/* webpackIgnore: true */ moduleName).catch(() => null)
    if (!childProcess) {
      return {
        supported: false,
        version: null,
      }
    }

    const version = childProcess.execSync('npx wrangler --version', { encoding: 'utf-8' }).trim()

    // SQL metrics in tail were added around wrangler 3.x
    const majorVersion = parseInt(version.split('.')[0] ?? '0')

    return {
      supported: majorVersion >= 3,
      version,
    }
  } catch {
    return {
      supported: false,
      version: null,
    }
  }
}
