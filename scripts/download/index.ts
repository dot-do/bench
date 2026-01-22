/**
 * Download/Generate Scripts Module
 *
 * Provides utilities for downloading and generating benchmark datasets and
 * reference data needed for performance testing.
 *
 * Key components:
 * - downloadDatasets: Download benchmark datasets from remote sources
 * - generateFixtures: Generate test fixtures locally
 * - downloadReferences: Download reference data for comparison
 *
 * Usage:
 * ```typescript
 * import { downloadAll } from './scripts/download'
 *
 * const result = await downloadAll('./data', {
 *   verbose: true,
 *   parallel: true,
 *   timeout: 30000
 * })
 *
 * console.log(`Downloaded ${result.successful} datasets`)
 * if (result.errors.length > 0) {
 *   console.error('Some downloads failed:', result.errors)
 * }
 * ```
 */

import * as fs from 'fs/promises'
import * as path from 'path'

// Individual download/generate functions
// These will be imported from their respective modules as they are created
// export { downloadDatasets } from './datasets'
// export { generateFixtures } from './fixtures'
// export { downloadReferences } from './references'

/**
 * Options for downloadAll function
 */
export interface DownloadAllOptions {
  /** Enable verbose logging */
  verbose?: boolean
  /** Run downloads in parallel */
  parallel?: boolean
  /** Timeout per download in milliseconds */
  timeout?: number
  /** Skip files that already exist */
  skipExisting?: boolean
}

/**
 * Result of downloadAll operation
 */
export interface DownloadAllResult {
  /** Number of successful downloads */
  successful: number
  /** Number of failed downloads */
  failed: number
  /** List of errors that occurred */
  errors: Array<{
    type: string
    message: string
    error?: Error
  }>
  /** Total time in milliseconds */
  duration: number
}

/**
 * Logger utility for consistent output
 */
class DownloadLogger {
  constructor(private verbose: boolean = false) {}

  info(message: string): void {
    console.log(`[DOWNLOAD] ${message}`)
  }

  debug(message: string): void {
    if (this.verbose) {
      console.log(`[DOWNLOAD:DEBUG] ${message}`)
    }
  }

  error(message: string, error?: Error): void {
    console.error(`[DOWNLOAD:ERROR] ${message}`)
    if (error && this.verbose) {
      console.error(error)
    }
  }

  warn(message: string): void {
    console.warn(`[DOWNLOAD:WARN] ${message}`)
  }
}

/**
 * Download all datasets and fixtures to the specified output directory
 *
 * This is a convenience function that orchestrates all individual download
 * functions with error handling and logging.
 *
 * @param outputDir Directory to download files to
 * @param options Configuration options for downloads
 * @returns Promise resolving to download result summary
 */
export async function downloadAll(
  outputDir: string,
  options: DownloadAllOptions = {}
): Promise<DownloadAllResult> {
  const startTime = Date.now()
  const logger = new DownloadLogger(options.verbose ?? false)
  const result: DownloadAllResult = {
    successful: 0,
    failed: 0,
    errors: [],
    duration: 0,
  }

  try {
    // Ensure output directory exists
    logger.info(`Creating output directory: ${outputDir}`)
    await fs.mkdir(outputDir, { recursive: true })
    logger.debug(`Output directory ready: ${outputDir}`)

    // List of download functions to execute
    // These will be populated as individual download modules are created
    const downloadFunctions: Array<() => Promise<void>> = [
      // async () => downloadDatasets(outputDir, options),
      // async () => generateFixtures(outputDir, options),
      // async () => downloadReferences(outputDir, options),
    ]

    logger.info(`Starting downloads with ${downloadFunctions.length} functions`)

    if (downloadFunctions.length === 0) {
      logger.warn('No download functions configured')
      return {
        ...result,
        duration: Date.now() - startTime,
      }
    }

    // Execute downloads
    if (options.parallel) {
      logger.debug('Running downloads in parallel')
      const promises = downloadFunctions.map((fn) =>
        executeWithErrorHandling(fn, logger, options.timeout)
      )
      const outcomes = await Promise.all(promises)

      for (const outcome of outcomes) {
        if (outcome.success) {
          result.successful++
        } else {
          result.failed++
          result.errors.push(outcome.error)
        }
      }
    } else {
      logger.debug('Running downloads sequentially')
      for (const fn of downloadFunctions) {
        const outcome = await executeWithErrorHandling(fn, logger, options.timeout)
        if (outcome.success) {
          result.successful++
        } else {
          result.failed++
          result.errors.push(outcome.error)
        }
      }
    }

    result.duration = Date.now() - startTime

    // Log summary
    logger.info(
      `Downloads complete: ${result.successful} succeeded, ${result.failed} failed (${result.duration}ms)`
    )

    if (result.errors.length > 0) {
      logger.warn(`${result.errors.length} error(s) occurred during downloads:`)
      for (const err of result.errors) {
        logger.error(`  ${err.type}: ${err.message}`)
      }
    }

    return result
  } catch (error) {
    logger.error('Fatal error during downloads', error instanceof Error ? error : undefined)
    result.errors.push({
      type: 'FatalError',
      message: error instanceof Error ? error.message : String(error),
      error: error instanceof Error ? error : undefined,
    })
    result.failed++
    result.duration = Date.now() - startTime
    return result
  }
}

/**
 * Execute a download function with error handling and timeout
 */
async function executeWithErrorHandling(
  fn: () => Promise<void>,
  logger: DownloadLogger,
  timeout?: number
): Promise<{ success: boolean; error: { type: string; message: string; error?: Error } }> {
  try {
    if (timeout) {
      await executeWithTimeout(fn(), timeout)
    } else {
      await fn()
    }
    return { success: true, error: {} as any }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    const errorType =
      error instanceof Error ? error.constructor.name : typeof error

    logger.error(`Download failed: ${message}`)

    return {
      success: false,
      error: {
        type: errorType,
        message,
        error: error instanceof Error ? error : undefined,
      },
    }
  }
}

/**
 * Execute a promise with a timeout
 */
function executeWithTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  return Promise.race([
    promise,
    new Promise<T>((_, reject) =>
      setTimeout(() => reject(new Error(`Operation timed out after ${timeoutMs}ms`)), timeoutMs)
    ),
  ])
}
