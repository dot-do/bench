/**
 * Financial Benchmark Worker - TigerBeetle
 *
 * Cloudflare Worker that runs TigerBeetle financial benchmarks.
 * Uses the TigerBeetle WASM adapter with Durable Object state persistence.
 *
 * Endpoint: POST /benchmark/financial
 *
 * Benchmark operations:
 * - Account creation (single and batch)
 * - Transfer processing throughput
 * - Balance lookups
 * - Two-phase transfers
 */

import { DurableObject } from 'cloudflare:workers'
import {
  LedgerState,
  Uint128,
  AccountFlags,
  TransferFlags,
  type CreateAccountInput,
  type CreateTransferInput,
  type Account,
} from '@dotdo/poc-tigerbeetle-do'

// ============================================================================
// Inlined Types (from instrumentation/types.ts)
// Inlined to avoid bundling issues in Worker context
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

// ============================================================================
// Configuration
// ============================================================================

const BENCHMARK_CONFIG = {
  iterations: {
    single: 100,
    batch: 10,
    throughput: 5,
  },
  batchSizes: {
    small: 100,
    medium: 1000,
    large: 8190, // TigerBeetle max batch size
  },
  // Initial account seed count
  seedAccounts: 1000,
  // Initial balance per account (in cents)
  initialBalance: 1000000, // $10,000.00
} as const

// ============================================================================
// Types
// ============================================================================

interface Env {
  TIGERBEETLE_DO: DurableObjectNamespace<TigerBeetleBenchDO>
  RESULTS: R2Bucket
}

interface BenchmarkRequest {
  operations?: string[]
  iterations?: number
  batchSize?: number
  runId?: string
}

interface BenchmarkTiming {
  name: string
  iterations: number
  totalMs: number
  minMs: number
  maxMs: number
  meanMs: number
  p50Ms: number
  p99Ms: number
  opsPerSec: number
}

interface FinancialBenchmarkResults {
  runId: string
  timestamp: string
  environment: BenchmarkEnvironment
  colo?: string
  benchmarks: BenchmarkTiming[]
  summary: {
    totalDurationMs: number
    totalOperations: number
    overallOpsPerSec: number
  }
}

// ============================================================================
// Utility Functions
// ============================================================================

function generateRunId(): string {
  const timestamp = Date.now().toString(36)
  const random = Math.random().toString(36).substring(2, 8)
  return `financial-${timestamp}-${random}`
}

function generateUint128Id(): Uint128 {
  const buffer = new Uint8Array(16)
  crypto.getRandomValues(buffer)
  return Uint128.fromBytes(buffer)
}

function calculatePercentile(sortedValues: number[], percentile: number): number {
  if (sortedValues.length === 0) return 0
  const index = Math.ceil((percentile / 100) * sortedValues.length) - 1
  return sortedValues[Math.max(0, index)]
}

function calculateStats(times: number[]): Omit<BenchmarkTiming, 'name' | 'iterations' | 'opsPerSec'> {
  if (times.length === 0) {
    return { totalMs: 0, minMs: 0, maxMs: 0, meanMs: 0, p50Ms: 0, p99Ms: 0 }
  }

  const sorted = [...times].sort((a, b) => a - b)
  const totalMs = times.reduce((a, b) => a + b, 0)

  return {
    totalMs,
    minMs: sorted[0],
    maxMs: sorted[sorted.length - 1],
    meanMs: totalMs / times.length,
    p50Ms: calculatePercentile(sorted, 50),
    p99Ms: calculatePercentile(sorted, 99),
  }
}

// ============================================================================
// TigerBeetle Benchmark Durable Object
// ============================================================================

export class TigerBeetleBenchDO extends DurableObject<Env> {
  private ledger: LedgerState
  private accountIds: Uint128[] = []
  private initialized = false

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    this.ledger = new LedgerState({
      maxBatchSize: BENCHMARK_CONFIG.batchSizes.large,
    })
  }

  /**
   * Initialize the ledger with seed data
   */
  private async initialize(): Promise<void> {
    if (this.initialized) return

    // Try to restore state from storage
    const savedState = await this.ctx.storage.get<string>('ledger_state')
    if (savedState) {
      try {
        const state = JSON.parse(savedState)
        this.ledger.importState(state)
        // Safely extract account IDs, handling case where accounts may be undefined
        if (state.accounts && Array.isArray(state.accounts)) {
          this.accountIds = state.accounts.map((a: Account) => a.id)
        }
        this.initialized = true
        return
      } catch {
        // State corrupted, reinitialize
      }
    }

    // Create seed accounts
    const bankAccountId = generateUint128Id()
    const accounts: CreateAccountInput[] = [
      // Bank account (can have unlimited credits)
      {
        id: bankAccountId,
        ledger: 1,
        code: 1001,
        flags: AccountFlags.NONE,
      },
    ]

    // Create user accounts
    for (let i = 0; i < BENCHMARK_CONFIG.seedAccounts; i++) {
      const id = generateUint128Id()
      this.accountIds.push(id)
      accounts.push({
        id,
        ledger: 1,
        code: 1001,
        flags: AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS,
      })
    }

    // Batch create accounts
    this.ledger.createAccounts(accounts)

    // Fund accounts with initial balance
    const fundingTransfers: CreateTransferInput[] = this.accountIds.map((accountId) => ({
      id: generateUint128Id(),
      debit_account_id: bankAccountId,
      credit_account_id: accountId,
      amount: Uint128.fromNumber(BENCHMARK_CONFIG.initialBalance),
      ledger: 1,
      code: 1,
    }))

    this.ledger.createTransfers(fundingTransfers)

    this.initialized = true

    // Persist state
    await this.saveState()
  }

  /**
   * BigInt-safe JSON serializer
   */
  private bigIntReplacer = (_key: string, value: unknown) =>
    typeof value === 'bigint' ? value.toString() : value

  /**
   * Save ledger state to durable storage
   */
  private async saveState(): Promise<void> {
    const state = this.ledger.exportState()
    await this.ctx.storage.put('ledger_state', JSON.stringify(state, this.bigIntReplacer))
  }

  /**
   * Run all financial benchmarks
   */
  async runBenchmarks(request: BenchmarkRequest): Promise<FinancialBenchmarkResults> {
    await this.initialize()

    const runId = request.runId ?? generateRunId()
    const iterations = request.iterations ?? BENCHMARK_CONFIG.iterations.single
    const batchSize = request.batchSize ?? BENCHMARK_CONFIG.batchSizes.medium
    const operations = request.operations ?? [
      'account_create_single',
      'account_create_batch',
      'balance_lookup_single',
      'balance_lookup_batch',
      'transfer_single',
      'transfer_batch',
      'two_phase_transfer',
      'throughput_1000',
    ]

    const benchmarks: BenchmarkTiming[] = []
    const startTime = performance.now()

    for (const op of operations) {
      const timing = await this.runOperation(op, iterations, batchSize)
      if (timing) {
        benchmarks.push(timing)
      }
    }

    const totalDurationMs = performance.now() - startTime
    const totalOperations = benchmarks.reduce((sum, b) => sum + b.iterations, 0)

    return {
      runId,
      timestamp: new Date().toISOString(),
      environment: 'do',
      benchmarks,
      summary: {
        totalDurationMs,
        totalOperations,
        overallOpsPerSec: totalOperations / (totalDurationMs / 1000),
      },
    }
  }

  /**
   * Run a single benchmark operation
   */
  private async runOperation(
    operation: string,
    iterations: number,
    batchSize: number
  ): Promise<BenchmarkTiming | null> {
    const times: number[] = []

    switch (operation) {
      case 'account_create_single':
        for (let i = 0; i < iterations; i++) {
          const start = performance.now()
          this.ledger.createAccounts([
            {
              id: generateUint128Id(),
              ledger: 1,
              code: 1001,
            },
          ])
          times.push(performance.now() - start)
        }
        break

      case 'account_create_batch':
        for (let i = 0; i < Math.min(iterations, BENCHMARK_CONFIG.iterations.batch); i++) {
          const accounts: CreateAccountInput[] = Array.from({ length: batchSize }, () => ({
            id: generateUint128Id(),
            ledger: 1,
            code: 1001,
          }))
          const start = performance.now()
          this.ledger.createAccounts(accounts)
          times.push(performance.now() - start)
        }
        break

      case 'balance_lookup_single':
        for (let i = 0; i < iterations; i++) {
          const accountId = this.accountIds[Math.floor(Math.random() * this.accountIds.length)]
          const start = performance.now()
          this.ledger.lookupAccounts([accountId])
          times.push(performance.now() - start)
        }
        break

      case 'balance_lookup_batch':
        for (let i = 0; i < iterations; i++) {
          const ids = this.accountIds.slice(0, 100)
          const start = performance.now()
          this.ledger.lookupAccounts(ids)
          times.push(performance.now() - start)
        }
        break

      case 'transfer_single':
        for (let i = 0; i < iterations; i++) {
          const fromIdx = Math.floor(Math.random() * this.accountIds.length)
          let toIdx = Math.floor(Math.random() * this.accountIds.length)
          while (toIdx === fromIdx) toIdx = Math.floor(Math.random() * this.accountIds.length)

          const start = performance.now()
          this.ledger.createTransfers([
            {
              id: generateUint128Id(),
              debit_account_id: this.accountIds[fromIdx],
              credit_account_id: this.accountIds[toIdx],
              amount: Uint128.fromNumber(100), // $1.00
              ledger: 1,
              code: 1,
            },
          ])
          times.push(performance.now() - start)
        }
        break

      case 'transfer_batch':
        for (let i = 0; i < Math.min(iterations, BENCHMARK_CONFIG.iterations.batch); i++) {
          const transfers: CreateTransferInput[] = Array.from({ length: batchSize }, () => {
            const fromIdx = Math.floor(Math.random() * this.accountIds.length)
            let toIdx = Math.floor(Math.random() * this.accountIds.length)
            while (toIdx === fromIdx) toIdx = Math.floor(Math.random() * this.accountIds.length)

            return {
              id: generateUint128Id(),
              debit_account_id: this.accountIds[fromIdx],
              credit_account_id: this.accountIds[toIdx],
              amount: Uint128.fromNumber(100),
              ledger: 1,
              code: 1,
            }
          })

          const start = performance.now()
          this.ledger.createTransfers(transfers)
          times.push(performance.now() - start)
        }
        break

      case 'two_phase_transfer':
        for (let i = 0; i < iterations; i++) {
          const fromIdx = Math.floor(Math.random() * this.accountIds.length)
          let toIdx = Math.floor(Math.random() * this.accountIds.length)
          while (toIdx === fromIdx) toIdx = Math.floor(Math.random() * this.accountIds.length)

          const pendingId = generateUint128Id()
          const start = performance.now()

          // Phase 1: Create pending transfer
          this.ledger.createTransfers([
            {
              id: pendingId,
              debit_account_id: this.accountIds[fromIdx],
              credit_account_id: this.accountIds[toIdx],
              amount: Uint128.fromNumber(100),
              ledger: 1,
              code: 1,
              flags: TransferFlags.PENDING,
              timeout: 3600, // 1 hour
            },
          ])

          // Phase 2: Post the pending transfer
          this.ledger.createTransfers([
            {
              id: generateUint128Id(),
              debit_account_id: this.accountIds[fromIdx],
              credit_account_id: this.accountIds[toIdx],
              amount: Uint128.fromNumber(100),
              pending_id: pendingId,
              ledger: 1,
              code: 1,
              flags: TransferFlags.POST_PENDING_TRANSFER,
            },
          ])

          times.push(performance.now() - start)
        }
        break

      case 'throughput_1000':
        for (let i = 0; i < BENCHMARK_CONFIG.iterations.throughput; i++) {
          const transfers: CreateTransferInput[] = Array.from({ length: 1000 }, () => {
            const fromIdx = Math.floor(Math.random() * this.accountIds.length)
            let toIdx = Math.floor(Math.random() * this.accountIds.length)
            while (toIdx === fromIdx) toIdx = Math.floor(Math.random() * this.accountIds.length)

            return {
              id: generateUint128Id(),
              debit_account_id: this.accountIds[fromIdx],
              credit_account_id: this.accountIds[toIdx],
              amount: Uint128.fromNumber(Math.floor(Math.random() * 1000) + 1),
              ledger: 1,
              code: 1,
            }
          })

          const start = performance.now()
          this.ledger.createTransfers(transfers)
          times.push(performance.now() - start)
        }
        break

      case 'throughput_10000':
        for (let i = 0; i < BENCHMARK_CONFIG.iterations.throughput; i++) {
          const transfers: CreateTransferInput[] = Array.from({ length: 10000 }, () => {
            const fromIdx = Math.floor(Math.random() * this.accountIds.length)
            let toIdx = Math.floor(Math.random() * this.accountIds.length)
            while (toIdx === fromIdx) toIdx = Math.floor(Math.random() * this.accountIds.length)

            return {
              id: generateUint128Id(),
              debit_account_id: this.accountIds[fromIdx],
              credit_account_id: this.accountIds[toIdx],
              amount: Uint128.fromNumber(Math.floor(Math.random() * 1000) + 1),
              ledger: 1,
              code: 1,
            }
          })

          const start = performance.now()
          this.ledger.createTransfers(transfers)
          times.push(performance.now() - start)
        }
        break

      default:
        return null
    }

    const stats = calculateStats(times)
    const effectiveIterations = times.length

    return {
      name: operation,
      iterations: effectiveIterations,
      ...stats,
      opsPerSec: effectiveIterations / (stats.totalMs / 1000),
    }
  }

  /**
   * Reset the benchmark state
   */
  async reset(): Promise<void> {
    this.ledger.clear()
    this.accountIds = []
    this.initialized = false
    await this.ctx.storage.delete('ledger_state')
  }

  /**
   * Get current ledger stats
   */
  getStats(): { accounts: number; transfers: number; pendingTransfers: number } {
    return {
      accounts: this.ledger.accountCount,
      transfers: this.ledger.transferCount,
      pendingTransfers: this.ledger.pendingTransferCount,
    }
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)

    // BigInt-safe JSON serializer
    const bigIntReplacer = (_key: string, value: unknown) =>
      typeof value === 'bigint' ? value.toString() : value

    if (request.method === 'POST' && url.pathname === '/run') {
      try {
        const body = (await request.json()) as BenchmarkRequest
        const results = await this.runBenchmarks(body)
        return new Response(JSON.stringify(results, bigIntReplacer, 2), {
          headers: { 'Content-Type': 'application/json' },
        })
      } catch (error) {
        return new Response(
          JSON.stringify({ error: error instanceof Error ? error.message : 'Unknown error' }, bigIntReplacer),
          { status: 500, headers: { 'Content-Type': 'application/json' } }
        )
      }
    }

    if (request.method === 'POST' && url.pathname === '/reset') {
      await this.reset()
      return new Response(JSON.stringify({ status: 'reset' }), {
        headers: { 'Content-Type': 'application/json' },
      })
    }

    if (request.method === 'GET' && url.pathname === '/stats') {
      await this.initialize()
      return new Response(JSON.stringify(this.getStats(), bigIntReplacer), {
        headers: { 'Content-Type': 'application/json' },
      })
    }

    return new Response('Not found', { status: 404 })
  }
}

// ============================================================================
// Worker Entry Point
// ============================================================================

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url)

    // Health check
    if (url.pathname === '/health') {
      return new Response(JSON.stringify({ status: 'ok', timestamp: new Date().toISOString() }), {
        headers: { 'Content-Type': 'application/json' },
      })
    }

    // Main benchmark endpoint
    if (request.method === 'POST' && url.pathname === '/benchmark/financial') {
      try {
        // Get or create benchmark DO instance
        const doId = env.TIGERBEETLE_DO.idFromName('benchmark')
        const benchDO = env.TIGERBEETLE_DO.get(doId)

        // Parse request body
        let body: BenchmarkRequest = {}
        try {
          body = (await request.json()) as BenchmarkRequest
        } catch {
          // Empty body is fine, use defaults
        }

        // Run benchmarks
        const doRequest = new Request('http://internal/run', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        })

        const response = await benchDO.fetch(doRequest)

        // Check if DO returned an error response
        if (!response.ok) {
          const errorBody = await response.json() as { error?: string }
          throw new Error(errorBody.error ?? `DO request failed with status ${response.status}`)
        }

        const results = (await response.json()) as FinancialBenchmarkResults

        // Validate that benchmarks array exists
        if (!results.benchmarks || !Array.isArray(results.benchmarks)) {
          throw new Error('Invalid benchmark results: missing benchmarks array')
        }

        // Add colo information
        const colo = request.cf?.colo as string | undefined
        if (colo) {
          results.colo = colo
        }

        // Convert to JSONL format for R2 storage
        const jsonlResults = results.benchmarks.map((b) => {
          const result: BenchmarkResult = {
            benchmark: `financial/${b.name}`,
            database: 'tigerbeetle',
            dataset: 'financial-accounts',
            p50_ms: b.p50Ms,
            p99_ms: b.p99Ms,
            min_ms: b.minMs,
            max_ms: b.maxMs,
            mean_ms: b.meanMs,
            ops_per_sec: b.opsPerSec,
            iterations: b.iterations,
            vfs_reads: 0,
            vfs_writes: 0,
            vfs_bytes_read: 0,
            vfs_bytes_written: 0,
            timestamp: results.timestamp,
            environment: results.environment,
            run_id: results.runId,
            colo: results.colo,
            total_duration_ms: b.totalMs,
          }
          return JSON.stringify(result)
        })

        // Store results in R2
        const resultsKey = `financial/${results.runId}.jsonl`
        await env.RESULTS.put(resultsKey, jsonlResults.join('\n'))

        // BigInt-safe JSON serializer
        const bigIntReplacer = (_key: string, value: unknown) =>
          typeof value === 'bigint' ? value.toString() : value

        return new Response(JSON.stringify(results, bigIntReplacer, 2), {
          headers: {
            'Content-Type': 'application/json',
            'X-Results-Key': resultsKey,
            'X-Run-Id': results.runId,
          },
        })
      } catch (error) {
        // BigInt-safe JSON serializer for error responses
        const bigIntReplacer = (_key: string, value: unknown) =>
          typeof value === 'bigint' ? value.toString() : value

        return new Response(
          JSON.stringify({
            error: error instanceof Error ? error.message : 'Unknown error',
            stack: error instanceof Error ? error.stack : undefined,
          }, bigIntReplacer),
          { status: 500, headers: { 'Content-Type': 'application/json' } }
        )
      }
    }

    // Reset benchmark state
    if (request.method === 'POST' && url.pathname === '/benchmark/financial/reset') {
      const doId = env.TIGERBEETLE_DO.idFromName('benchmark')
      const benchDO = env.TIGERBEETLE_DO.get(doId)

      const doRequest = new Request('http://internal/reset', { method: 'POST' })
      await benchDO.fetch(doRequest)

      return new Response(JSON.stringify({ status: 'reset' }), {
        headers: { 'Content-Type': 'application/json' },
      })
    }

    // Get benchmark stats
    if (request.method === 'GET' && url.pathname === '/benchmark/financial/stats') {
      const doId = env.TIGERBEETLE_DO.idFromName('benchmark')
      const benchDO = env.TIGERBEETLE_DO.get(doId)

      const doRequest = new Request('http://internal/stats', { method: 'GET' })
      const response = await benchDO.fetch(doRequest)

      return new Response(await response.text(), {
        headers: { 'Content-Type': 'application/json' },
      })
    }

    // List stored results
    if (request.method === 'GET' && url.pathname === '/benchmark/financial/results') {
      const list = await env.RESULTS.list({ prefix: 'financial/' })
      const results = list.objects.map((obj: R2Object) => ({
        key: obj.key,
        size: obj.size,
        uploaded: obj.uploaded.toISOString(),
      }))

      return new Response(JSON.stringify(results, null, 2), {
        headers: { 'Content-Type': 'application/json' },
      })
    }

    // Get specific result
    if (request.method === 'GET' && url.pathname.startsWith('/benchmark/financial/results/')) {
      const runId = url.pathname.replace('/benchmark/financial/results/', '')
      const key = `financial/${runId}.jsonl`
      const object = await env.RESULTS.get(key)

      if (!object) {
        return new Response(JSON.stringify({ error: 'Result not found' }), {
          status: 404,
          headers: { 'Content-Type': 'application/json' },
        })
      }

      return new Response(await object.text(), {
        headers: { 'Content-Type': 'application/x-ndjson' },
      })
    }

    // API documentation
    if (request.method === 'GET' && url.pathname === '/') {
      return new Response(
        JSON.stringify(
          {
            name: 'Financial Benchmark Worker',
            description: 'TigerBeetle financial benchmark runner on Cloudflare Workers',
            endpoints: {
              'POST /benchmark/financial': 'Run financial benchmarks',
              'POST /benchmark/financial/reset': 'Reset benchmark state',
              'GET /benchmark/financial/stats': 'Get current ledger statistics',
              'GET /benchmark/financial/results': 'List stored benchmark results',
              'GET /benchmark/financial/results/:runId': 'Get specific benchmark result',
              'GET /health': 'Health check',
            },
            requestBody: {
              operations: [
                'account_create_single',
                'account_create_batch',
                'balance_lookup_single',
                'balance_lookup_batch',
                'transfer_single',
                'transfer_batch',
                'two_phase_transfer',
                'throughput_1000',
                'throughput_10000',
              ],
              iterations: 'Number of iterations per operation (default: 100)',
              batchSize: 'Batch size for batch operations (default: 1000)',
              runId: 'Custom run ID (optional)',
            },
          },
          null,
          2
        ),
        { headers: { 'Content-Type': 'application/json' } }
      )
    }

    return new Response('Not Found', { status: 404 })
  },
}
