/**
 * VFS Metrics Instrumentation
 *
 * Tracks actual DO SQLite blob operations to verify 2MB optimization effectiveness.
 * Different databases may have different write amplification patterns.
 *
 * Key insight: Cloudflare charges per-row, not per-byte.
 * - Read:  $0.001 per 1M rows
 * - Write: $1.00 per 1M rows
 *
 * A 1KB row costs the same as a 2MB row, so packing data into
 * 2MB blobs is 2000x more efficient for bulk operations.
 */

export interface VFSMetrics {
  // Blob operations (DO storage layer)
  blobsRead: number
  blobsWritten: number
  bytesRead: number
  bytesWritten: number

  // Row operations (sql.exec layer)
  rowsRead: number
  rowsWritten: number

  // Write amplification = bytes written / logical bytes changed
  // e.g., updating 100 bytes causes 2MB blob rewrite = 20,000x amplification
  writeAmplification: number

  // Read amplification = bytes read / logical bytes needed
  // e.g., reading 100 bytes requires 2MB blob load = 20,000x amplification
  readAmplification: number

  // Cost calculation (DO pricing per 1M operations)
  estimatedReadCost: number   // $0.001 per 1M rows
  estimatedWriteCost: number  // $1.00 per 1M rows
  estimatedTotalCost: number

  // Timing
  storageReadMs: number
  storageWriteMs: number
  sqlExecMs: number
}

export interface VFSOperation {
  type: 'get' | 'put' | 'delete' | 'list' | 'sql'
  key?: string
  bytes: number
  rows?: number
  durationMs: number
  timestamp: number
}

export interface VFSInstrumentOptions {
  /** Track individual operations for debugging */
  trackOperations?: boolean
  /** Max operations to keep in history */
  maxOperations?: number
  /** Logical bytes written (for write amplification calculation) */
  logicalBytesWritten?: number
  /** Logical bytes read (for read amplification calculation) */
  logicalBytesRead?: number
}

const PRICING = {
  rowsReadPer1M: 0.001,   // $0.001 per 1M rows read
  rowsWrittenPer1M: 1.00, // $1.00 per 1M rows written
}

const DEFAULT_BLOB_SIZE = 2 * 1024 * 1024 // 2MB optimal blob size

/**
 * VFS Instrumentation for tracking DO storage operations.
 *
 * Wraps DurableObjectStorage and SqlStorage to intercept
 * and measure all blob/row operations.
 */
export class VFSInstrument {
  private metrics: VFSMetrics
  private operations: VFSOperation[] = []
  private options: VFSInstrumentOptions
  private logicalBytesWritten = 0
  private logicalBytesRead = 0

  constructor(options: VFSInstrumentOptions = {}) {
    this.options = {
      trackOperations: false,
      maxOperations: 1000,
      ...options,
    }
    this.metrics = this.createEmptyMetrics()
  }

  private createEmptyMetrics(): VFSMetrics {
    return {
      blobsRead: 0,
      blobsWritten: 0,
      bytesRead: 0,
      bytesWritten: 0,
      rowsRead: 0,
      rowsWritten: 0,
      writeAmplification: 0,
      readAmplification: 0,
      estimatedReadCost: 0,
      estimatedWriteCost: 0,
      estimatedTotalCost: 0,
      storageReadMs: 0,
      storageWriteMs: 0,
      sqlExecMs: 0,
    }
  }

  /**
   * Wrap DurableObjectStorage to track get/put/delete/list operations.
   * Returns a proxy that intercepts all method calls.
   */
  wrapStorage<T extends DurableObjectStorage>(storage: T): T {
    const self = this

    return new Proxy(storage, {
      get(target, prop, receiver) {
        const original = Reflect.get(target, prop, receiver)

        if (typeof original !== 'function') {
          return original
        }

        // Wrap storage methods
        switch (prop) {
          case 'get':
            return async function (key: string, options?: DurableObjectGetOptions) {
              const start = performance.now()
              const result = await original.call(target, key, options)
              const duration = performance.now() - start

              const bytes = result ? self.estimateBytes(result) : 0
              self.recordRead(bytes, duration, key)

              return result
            }

          case 'getAlarm':
            return original.bind(target)

          case 'put':
            return async function (key: string, value: unknown, options?: DurableObjectPutOptions) {
              const start = performance.now()
              await original.call(target, key, value, options)
              const duration = performance.now() - start

              const bytes = self.estimateBytes(value)
              self.recordWrite(bytes, duration, key)
            }

          case 'delete':
            return async function (key: string | string[]) {
              const start = performance.now()
              const result = await original.call(target, key)
              const duration = performance.now() - start

              // Delete is a write operation
              const count = Array.isArray(key) ? key.length : 1
              self.recordWrite(0, duration) // Deletes still count as writes
              self.metrics.blobsWritten += count - 1 // Adjust for batch deletes

              return result
            }

          case 'list':
            return async function (options?: DurableObjectListOptions) {
              const start = performance.now()
              const result = await original.call(target, options)
              const duration = performance.now() - start

              let bytes = 0
              for (const value of result.values()) {
                bytes += self.estimateBytes(value)
              }

              self.metrics.blobsRead += result.size
              self.metrics.bytesRead += bytes
              self.metrics.storageReadMs += duration

              if (self.options.trackOperations) {
                self.addOperation({
                  type: 'list',
                  bytes,
                  durationMs: duration,
                  timestamp: Date.now(),
                })
              }

              return result
            }

          case 'transaction':
            return async function <R>(closure: (txn: DurableObjectTransaction) => Promise<R>): Promise<R> {
              // Wrap the transaction's storage operations too
              return original.call(target, async (txn: DurableObjectTransaction) => {
                const wrappedTxn = self.wrapStorage(txn as unknown as DurableObjectStorage)
                return closure(wrappedTxn as unknown as DurableObjectTransaction)
              })
            }

          default:
            return original.bind(target)
        }
      },
    })
  }

  /**
   * Wrap SqlStorage to track sql.exec operations.
   * Tracks rowsRead/rowsWritten from query results.
   */
  wrapSql<T extends SqlStorage>(sql: T): T {
    const self = this

    return new Proxy(sql, {
      get(target, prop, receiver) {
        const original = Reflect.get(target, prop, receiver)

        if (typeof original !== 'function') {
          return original
        }

        if (prop === 'exec') {
          return function (query: string, ...bindings: unknown[]) {
            const start = performance.now()
            const cursor = original.call(target, query, ...bindings)
            const duration = performance.now() - start

            // Analyze query type
            const queryUpper = query.trim().toUpperCase()
            const isRead = queryUpper.startsWith('SELECT')
            const isWrite = queryUpper.startsWith('INSERT') ||
                            queryUpper.startsWith('UPDATE') ||
                            queryUpper.startsWith('DELETE') ||
                            queryUpper.startsWith('CREATE') ||
                            queryUpper.startsWith('DROP') ||
                            queryUpper.startsWith('ALTER')

            // Track SQL execution time
            self.metrics.sqlExecMs += duration

            // Create instrumented cursor that tracks row counts
            return self.wrapCursor(cursor, isRead, isWrite)
          }
        }

        return original.bind(target)
      },
    })
  }

  /**
   * Wrap SQL cursor to track rows as they're consumed.
   */
  private wrapCursor(cursor: SqlStorageCursor, isRead: boolean, isWrite: boolean): SqlStorageCursor {
    const self = this
    let rowCount = 0

    // The cursor is an iterator, wrap its iteration
    const originalIterator = cursor[Symbol.iterator].bind(cursor)

    const wrappedCursor = {
      ...cursor,

      get rowsRead() {
        return cursor.rowsRead
      },

      get rowsWritten() {
        return cursor.rowsWritten
      },

      get columnNames() {
        return cursor.columnNames
      },

      toArray() {
        const results = cursor.toArray()

        if (isRead) {
          self.metrics.rowsRead += results.length
        }
        if (isWrite) {
          self.metrics.rowsWritten += cursor.rowsWritten ?? 0
        }

        if (self.options.trackOperations) {
          self.addOperation({
            type: 'sql',
            bytes: 0, // SQL layer doesn't expose bytes
            rows: results.length,
            durationMs: 0,
            timestamp: Date.now(),
          })
        }

        return results
      },

      one() {
        const result = cursor.one()

        if (isRead && result) {
          self.metrics.rowsRead += 1
        }

        return result
      },

      [Symbol.iterator]() {
        const iterator = originalIterator()

        return {
          next() {
            const result = iterator.next()

            if (!result.done && isRead) {
              self.metrics.rowsRead += 1
              rowCount++
            }

            return result
          },

          return(value?: unknown) {
            return iterator.return?.(value) ?? { done: true, value: undefined }
          },

          throw(e?: unknown) {
            return iterator.throw?.(e) ?? { done: true, value: undefined }
          },
        }
      },

      raw() {
        // Raw iteration - same instrumentation
        return cursor.raw()
      },
    } as SqlStorageCursor

    return wrappedCursor
  }

  /**
   * Record a logical write for amplification calculation.
   * Call this with the actual user-intended bytes written.
   */
  recordLogicalWrite(bytes: number): void {
    this.logicalBytesWritten += bytes
  }

  /**
   * Record a logical read for amplification calculation.
   * Call this with the actual user-intended bytes read.
   */
  recordLogicalRead(bytes: number): void {
    this.logicalBytesRead += bytes
  }

  /**
   * Get current metrics snapshot.
   */
  getMetrics(): VFSMetrics {
    // Calculate write amplification
    if (this.logicalBytesWritten > 0) {
      this.metrics.writeAmplification = this.metrics.bytesWritten / this.logicalBytesWritten
    } else if (this.metrics.bytesWritten > 0) {
      // Default: assume logical = physical for no-tracking scenarios
      this.metrics.writeAmplification = 1
    }

    // Calculate read amplification
    if (this.logicalBytesRead > 0) {
      this.metrics.readAmplification = this.metrics.bytesRead / this.logicalBytesRead
    } else if (this.metrics.bytesRead > 0) {
      this.metrics.readAmplification = 1
    }

    // Calculate costs (per 1M operations scaled to actual operations)
    // Cost = (rows / 1M) * price_per_1M
    this.metrics.estimatedReadCost =
      ((this.metrics.rowsRead + this.metrics.blobsRead) / 1_000_000) * PRICING.rowsReadPer1M

    this.metrics.estimatedWriteCost =
      ((this.metrics.rowsWritten + this.metrics.blobsWritten) / 1_000_000) * PRICING.rowsWrittenPer1M

    this.metrics.estimatedTotalCost =
      this.metrics.estimatedReadCost + this.metrics.estimatedWriteCost

    return { ...this.metrics }
  }

  /**
   * Get operation history (if trackOperations enabled).
   */
  getOperations(): VFSOperation[] {
    return [...this.operations]
  }

  /**
   * Reset all metrics for next benchmark.
   */
  reset(): void {
    this.metrics = this.createEmptyMetrics()
    this.operations = []
    this.logicalBytesWritten = 0
    this.logicalBytesRead = 0
  }

  /**
   * Format metrics as a summary string.
   */
  formatSummary(): string {
    const m = this.getMetrics()

    return [
      `VFS Metrics Summary`,
      `-------------------`,
      `Blobs:   ${m.blobsRead} read, ${m.blobsWritten} written`,
      `Bytes:   ${formatBytes(m.bytesRead)} read, ${formatBytes(m.bytesWritten)} written`,
      `Rows:    ${m.rowsRead} read, ${m.rowsWritten} written`,
      ``,
      `Amplification:`,
      `  Write: ${m.writeAmplification.toFixed(2)}x`,
      `  Read:  ${m.readAmplification.toFixed(2)}x`,
      ``,
      `Timing:`,
      `  Storage read:  ${m.storageReadMs.toFixed(2)}ms`,
      `  Storage write: ${m.storageWriteMs.toFixed(2)}ms`,
      `  SQL exec:      ${m.sqlExecMs.toFixed(2)}ms`,
      ``,
      `Estimated Cost (at scale):`,
      `  Read:  $${m.estimatedReadCost.toFixed(6)}`,
      `  Write: $${m.estimatedWriteCost.toFixed(6)}`,
      `  Total: $${m.estimatedTotalCost.toFixed(6)}`,
    ].join('\n')
  }

  // Private helpers

  private recordRead(bytes: number, durationMs: number, key?: string): void {
    this.metrics.blobsRead += 1
    this.metrics.bytesRead += bytes
    this.metrics.storageReadMs += durationMs

    if (this.options.trackOperations) {
      this.addOperation({
        type: 'get',
        key,
        bytes,
        durationMs,
        timestamp: Date.now(),
      })
    }
  }

  private recordWrite(bytes: number, durationMs: number, key?: string): void {
    this.metrics.blobsWritten += 1
    this.metrics.bytesWritten += bytes
    this.metrics.storageWriteMs += durationMs

    if (this.options.trackOperations) {
      this.addOperation({
        type: 'put',
        key,
        bytes,
        durationMs,
        timestamp: Date.now(),
      })
    }
  }

  private addOperation(op: VFSOperation): void {
    this.operations.push(op)

    // Trim if exceeds max
    if (this.operations.length > (this.options.maxOperations ?? 1000)) {
      this.operations = this.operations.slice(-500)
    }
  }

  private estimateBytes(value: unknown): number {
    if (value === null || value === undefined) {
      return 0
    }

    if (value instanceof ArrayBuffer) {
      return value.byteLength
    }

    if (ArrayBuffer.isView(value)) {
      return value.byteLength
    }

    if (typeof value === 'string') {
      return new TextEncoder().encode(value).length
    }

    // For objects, estimate via JSON serialization
    try {
      return new TextEncoder().encode(JSON.stringify(value)).length
    } catch {
      return 0
    }
  }
}

// Type definitions for Cloudflare DO storage (minimal)
interface DurableObjectStorage {
  get(key: string, options?: DurableObjectGetOptions): Promise<unknown>
  get(keys: string[], options?: DurableObjectGetOptions): Promise<Map<string, unknown>>
  put(key: string, value: unknown, options?: DurableObjectPutOptions): Promise<void>
  put(entries: Record<string, unknown>, options?: DurableObjectPutOptions): Promise<void>
  delete(key: string): Promise<boolean>
  delete(keys: string[]): Promise<number>
  list(options?: DurableObjectListOptions): Promise<Map<string, unknown>>
  transaction<T>(closure: (txn: DurableObjectTransaction) => Promise<T>): Promise<T>
  getAlarm(): Promise<number | null>
  setAlarm(scheduledTime: number | Date): Promise<void>
  deleteAlarm(): Promise<void>
}

interface DurableObjectTransaction {
  get(key: string, options?: DurableObjectGetOptions): Promise<unknown>
  put(key: string, value: unknown, options?: DurableObjectPutOptions): Promise<void>
  delete(key: string): Promise<boolean>
  rollback(): void
}

interface DurableObjectGetOptions {
  allowConcurrency?: boolean
  noCache?: boolean
}

interface DurableObjectPutOptions {
  allowConcurrency?: boolean
  allowUnconfirmed?: boolean
  noCache?: boolean
}

interface DurableObjectListOptions {
  start?: string
  startAfter?: string
  end?: string
  prefix?: string
  reverse?: boolean
  limit?: number
  allowConcurrency?: boolean
  noCache?: boolean
}

interface SqlStorage {
  exec<T = Record<string, unknown>>(query: string, ...bindings: unknown[]): SqlStorageCursor<T>
  databaseSize: number
}

interface SqlStorageCursor<T = Record<string, unknown>> extends Iterable<T> {
  readonly columnNames: string[]
  readonly rowsRead: number
  readonly rowsWritten: number
  toArray(): T[]
  one(): T | null
  raw<R extends unknown[] = unknown[]>(): Iterable<R>
}

// Utility functions
function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return `${(bytes / Math.pow(k, i)).toFixed(2)} ${sizes[i]}`
}

/**
 * Create a mock storage for testing instrumentation.
 * Uses in-memory Map but exposes same interface as DurableObjectStorage.
 */
export function createMockStorage(): DurableObjectStorage {
  const data = new Map<string, unknown>()

  async function get(key: string, options?: DurableObjectGetOptions): Promise<unknown>
  async function get(keys: string[], options?: DurableObjectGetOptions): Promise<Map<string, unknown>>
  async function get(key: string | string[], _options?: DurableObjectGetOptions): Promise<unknown | Map<string, unknown>> {
    if (Array.isArray(key)) {
      const result = new Map<string, unknown>()
      for (const k of key) {
        const v = data.get(k)
        if (v !== undefined) result.set(k, v)
      }
      return result
    }
    return data.get(key)
  }

  async function put(key: string, value: unknown, options?: DurableObjectPutOptions): Promise<void>
  async function put(entries: Record<string, unknown>, options?: DurableObjectPutOptions): Promise<void>
  async function put(keyOrEntries: string | Record<string, unknown>, valueOrOptions?: unknown, _options?: DurableObjectPutOptions): Promise<void> {
    if (typeof keyOrEntries === 'string') {
      data.set(keyOrEntries, valueOrOptions)
    } else {
      for (const [k, v] of Object.entries(keyOrEntries)) {
        data.set(k, v)
      }
    }
  }

  async function del(key: string): Promise<boolean>
  async function del(keys: string[]): Promise<number>
  async function del(key: string | string[]): Promise<boolean | number> {
    if (Array.isArray(key)) {
      let count = 0
      for (const k of key) {
        if (data.delete(k)) count++
      }
      return count
    }
    return data.delete(key)
  }

  async function list(options?: DurableObjectListOptions): Promise<Map<string, unknown>> {
    const result = new Map<string, unknown>()
    let count = 0

    for (const [key, value] of data) {
      if (options?.prefix && !key.startsWith(options.prefix)) continue
      if (options?.start && key < options.start) continue
      if (options?.end && key >= options.end) continue
      if (options?.limit && count >= options.limit) break

      result.set(key, value)
      count++
    }

    return result
  }

  async function transaction<T>(closure: (txn: DurableObjectTransaction) => Promise<T>): Promise<T> {
    // Simple mock - no real transaction semantics
    const txn: DurableObjectTransaction = {
      get: get as DurableObjectTransaction['get'],
      put: put as DurableObjectTransaction['put'],
      delete: del as DurableObjectTransaction['delete'],
      rollback: () => { throw new Error('Rollback called') },
    }
    return closure(txn)
  }

  return {
    get,
    put,
    delete: del,
    list,
    transaction,
    getAlarm: async () => null,
    setAlarm: async () => {},
    deleteAlarm: async () => {},
  } as DurableObjectStorage
}

/**
 * Create a mock SqlStorage for testing instrumentation.
 */
export function createMockSqlStorage(): SqlStorage {
  return {
    databaseSize: 0,

    exec<T = Record<string, unknown>>(query: string, ..._bindings: unknown[]): SqlStorageCursor<T> {
      const rows: T[] = []
      let rowsWritten = 0

      // Very basic SQL parsing for mocking
      const queryUpper = query.trim().toUpperCase()

      if (queryUpper.startsWith('SELECT')) {
        // Return empty results for mock
      } else if (queryUpper.startsWith('INSERT')) {
        rowsWritten = 1
      } else if (queryUpper.startsWith('UPDATE')) {
        rowsWritten = 1
      } else if (queryUpper.startsWith('DELETE')) {
        rowsWritten = 1
      }

      const cursor: SqlStorageCursor<T> = {
        columnNames: [],
        rowsRead: rows.length,
        rowsWritten,

        toArray() {
          return rows
        },

        one() {
          return rows[0] ?? null
        },

        raw<R extends unknown[] = unknown[]>(): Iterable<R> {
          const rawRows = rows.map(r => Object.values(r as object) as R)
          return rawRows
        },

        [Symbol.iterator]() {
          return rows[Symbol.iterator]()
        },
      }

      return cursor
    },
  }
}
