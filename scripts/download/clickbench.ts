/**
 * Download script for ClickBench hits.parquet dataset
 * Source: https://datasets.clickhouse.com/hits_compatible/
 *
 * The ClickBench dataset is a standard benchmark for analytical databases.
 * hits.parquet contains ~100M rows of anonymized web analytics data.
 */

import * as fs from 'fs'
import * as fsp from 'fs/promises'
import * as path from 'path'
import * as crypto from 'crypto'
import { Readable } from 'stream'
import { pipeline } from 'stream/promises'

const HITS_PARQUET_URL = 'https://datasets.clickhouse.com/hits_compatible/hits.parquet'
const EXPECTED_SIZE = 14_779_976_446 // ~14.8 GB (approximate)

interface DownloadProgress {
  downloaded: number
  total: number | null
  percentage: string
  speed: string
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
}

function formatSpeed(bytesPerSecond: number): string {
  return formatBytes(bytesPerSecond) + '/s'
}

function createProgressLogger(filename: string): (progress: DownloadProgress) => void {
  let lastLog = Date.now()
  return (progress: DownloadProgress) => {
    const now = Date.now()
    if (now - lastLog < 1000) return // Log at most once per second
    lastLog = now

    const downloaded = formatBytes(progress.downloaded)
    const total = progress.total ? formatBytes(progress.total) : 'unknown'
    console.log(`[clickbench] Downloading ${filename}: ${downloaded} / ${total} (${progress.percentage}) - ${progress.speed}`)
  }
}

async function downloadWithProgress(
  url: string,
  destPath: string,
  onProgress: (progress: DownloadProgress) => void
): Promise<string> {
  const response = await fetch(url)

  if (!response.ok) {
    throw new Error(`Failed to download ${url}: ${response.status} ${response.statusText}`)
  }

  const contentLength = response.headers.get('content-length')
  const total = contentLength ? parseInt(contentLength, 10) : null

  let downloaded = 0
  let lastTime = Date.now()
  let lastDownloaded = 0

  const hash = crypto.createHash('sha256')

  const reader = response.body?.getReader()
  if (!reader) {
    throw new Error('Response body is not readable')
  }

  const writeStream = fs.createWriteStream(destPath)

  const processStream = async function* (): AsyncGenerator<Uint8Array> {
    while (true) {
      const { done, value } = await reader.read()
      if (done) break

      downloaded += value.length
      hash.update(value)

      const now = Date.now()
      const elapsed = (now - lastTime) / 1000
      if (elapsed >= 1) {
        const bytesPerSecond = (downloaded - lastDownloaded) / elapsed
        lastDownloaded = downloaded
        lastTime = now

        onProgress({
          downloaded,
          total,
          percentage: total ? ((downloaded / total) * 100).toFixed(1) + '%' : 'N/A',
          speed: formatSpeed(bytesPerSecond)
        })
      }

      yield value
    }
  }

  const readable = Readable.from(processStream())
  await pipeline(readable, writeStream)

  return hash.digest('hex')
}

async function verifyFile(filePath: string): Promise<{ valid: boolean; size: number }> {
  if (!fs.existsSync(filePath)) {
    return { valid: false, size: 0 }
  }

  const stats = fs.statSync(filePath)
  // We consider the file valid if it's reasonably close to expected size
  // Since we don't have an official checksum, size validation is our best option
  const sizeDiff = Math.abs(stats.size - EXPECTED_SIZE)
  const valid = sizeDiff < EXPECTED_SIZE * 0.01 // Within 1% of expected size

  return { valid, size: stats.size }
}

export async function download(outputDir: string): Promise<void> {
  const filename = 'hits.parquet'
  const destPath = path.join(outputDir, filename)

  // Ensure output directory exists
  await fsp.mkdir(outputDir, { recursive: true })

  // Check if file already exists and is valid
  const existing = await verifyFile(destPath)
  if (existing.valid) {
    console.log(`[clickbench] ${filename} already exists (${formatBytes(existing.size)}), skipping download`)
    return
  }

  if (existing.size > 0) {
    console.log(`[clickbench] Existing file appears incomplete or corrupted (${formatBytes(existing.size)}), re-downloading...`)
  }

  console.log(`[clickbench] Starting download of ${filename} from ClickBench dataset`)
  console.log(`[clickbench] URL: ${HITS_PARQUET_URL}`)
  console.log(`[clickbench] Expected size: ~${formatBytes(EXPECTED_SIZE)}`)
  console.log(`[clickbench] Destination: ${destPath}`)

  const progressLogger = createProgressLogger(filename)
  const startTime = Date.now()

  try {
    const sha256 = await downloadWithProgress(HITS_PARQUET_URL, destPath, progressLogger)

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1)
    const finalStats = fs.statSync(destPath)

    console.log(`[clickbench] Download complete!`)
    console.log(`[clickbench] File size: ${formatBytes(finalStats.size)}`)
    console.log(`[clickbench] SHA256: ${sha256}`)
    console.log(`[clickbench] Time elapsed: ${elapsed}s`)
    console.log(`[clickbench] Average speed: ${formatSpeed(finalStats.size / parseFloat(elapsed))}`)
  } catch (error) {
    console.error(`[clickbench] Download failed:`, error)
    throw error
  }
}

// CLI support
if (typeof require !== 'undefined' && require.main === module) {
  const args = process.argv.slice(2)
  const outputDir = args[0] || './data/clickbench'
  download(outputDir).catch(console.error)
}
