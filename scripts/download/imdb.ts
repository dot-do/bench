/**
 * Download script for IMDb datasets
 * Source: https://datasets.imdbws.com/
 *
 * Downloads:
 * - title.basics.tsv.gz - Basic title information (movies, TV shows, etc.)
 * - name.basics.tsv.gz - Basic name/person information (actors, directors, etc.)
 * - title.ratings.tsv.gz - IMDb ratings and vote counts
 *
 * Note: IMDb data is for non-commercial use only.
 */

import * as fs from 'fs'
import * as fsp from 'fs/promises'
import * as path from 'path'
import * as crypto from 'crypto'
import * as zlib from 'zlib'
import { Readable } from 'stream'
import { pipeline } from 'stream/promises'

const IMDB_BASE_URL = 'https://datasets.imdbws.com'

const DATASETS = [
  { name: 'title.basics.tsv.gz', description: 'Basic title information' },
  { name: 'name.basics.tsv.gz', description: 'Basic name/person information' },
  { name: 'title.ratings.tsv.gz', description: 'IMDb ratings and votes' },
] as const

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
    console.log(`[imdb] Downloading ${filename}: ${downloaded} / ${total} (${progress.percentage}) - ${progress.speed}`)
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

async function computeFileHash(filePath: string): Promise<string> {
  const hash = crypto.createHash('sha256')
  const stream = fs.createReadStream(filePath)

  for await (const chunk of stream) {
    hash.update(chunk)
  }

  return hash.digest('hex')
}

async function decompressGzip(gzPath: string, outputPath: string): Promise<void> {
  console.log(`[imdb] Decompressing ${gzPath} to ${outputPath}`)

  const gunzip = zlib.createGunzip()
  const source = fs.createReadStream(gzPath)
  const destination = fs.createWriteStream(outputPath)

  await pipeline(source, gunzip, destination)

  const stats = fs.statSync(outputPath)
  console.log(`[imdb] Decompressed size: ${formatBytes(stats.size)}`)
}

interface FileMetadata {
  sha256: string
  size: number
  downloadedAt: string
}

function getMetadataPath(destPath: string): string {
  return destPath + '.meta.json'
}

async function saveMetadata(destPath: string, metadata: FileMetadata): Promise<void> {
  const metaPath = getMetadataPath(destPath)
  await fsp.writeFile(metaPath, JSON.stringify(metadata, null, 2))
}

async function loadMetadata(destPath: string): Promise<FileMetadata | null> {
  const metaPath = getMetadataPath(destPath)
  if (!fs.existsSync(metaPath)) return null

  try {
    const content = await fsp.readFile(metaPath, 'utf-8')
    return JSON.parse(content)
  } catch {
    return null
  }
}

async function isFileValid(destPath: string): Promise<boolean> {
  if (!fs.existsSync(destPath)) return false

  const stats = fs.statSync(destPath)
  if (stats.size === 0) return false

  const metadata = await loadMetadata(destPath)
  if (!metadata) return false

  // Verify size matches
  if (metadata.size !== stats.size) return false

  // Optional: verify hash (expensive for large files)
  // const currentHash = await computeFileHash(destPath)
  // return currentHash === metadata.sha256

  return true
}

async function downloadDataset(
  datasetName: string,
  description: string,
  outputDir: string,
  decompress: boolean = true
): Promise<void> {
  const url = `${IMDB_BASE_URL}/${datasetName}`
  const gzPath = path.join(outputDir, datasetName)
  const tsvName = datasetName.replace('.gz', '')
  const tsvPath = path.join(outputDir, tsvName)

  // Check if already downloaded
  const targetPath = decompress ? tsvPath : gzPath
  if (await isFileValid(targetPath)) {
    const stats = fs.statSync(targetPath)
    console.log(`[imdb] ${decompress ? tsvName : datasetName} already exists (${formatBytes(stats.size)}), skipping`)
    return
  }

  console.log(`[imdb] Downloading ${datasetName} (${description})`)
  console.log(`[imdb] URL: ${url}`)

  const progressLogger = createProgressLogger(datasetName)
  const startTime = Date.now()

  const sha256 = await downloadWithProgress(url, gzPath, progressLogger)

  const gzStats = fs.statSync(gzPath)
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1)

  console.log(`[imdb] Downloaded ${datasetName}: ${formatBytes(gzStats.size)} in ${elapsed}s`)
  console.log(`[imdb] SHA256 (compressed): ${sha256}`)

  // Save metadata for the compressed file
  await saveMetadata(gzPath, {
    sha256,
    size: gzStats.size,
    downloadedAt: new Date().toISOString()
  })

  if (decompress) {
    await decompressGzip(gzPath, tsvPath)

    // Compute and save hash for decompressed file
    const tsvHash = await computeFileHash(tsvPath)
    const tsvStats = fs.statSync(tsvPath)

    await saveMetadata(tsvPath, {
      sha256: tsvHash,
      size: tsvStats.size,
      downloadedAt: new Date().toISOString()
    })

    console.log(`[imdb] SHA256 (decompressed): ${tsvHash}`)

    // Optionally remove compressed file to save space
    // await unlink(gzPath)
    // console.log(`[imdb] Removed compressed file ${datasetName}`)
  }
}

export async function download(outputDir: string): Promise<void> {
  // Ensure output directory exists
  await fsp.mkdir(outputDir, { recursive: true })

  console.log(`[imdb] Starting IMDb dataset download`)
  console.log(`[imdb] Destination directory: ${outputDir}`)
  console.log(`[imdb] Note: IMDb data is for non-commercial use only`)
  console.log('')

  const startTime = Date.now()

  for (const dataset of DATASETS) {
    try {
      await downloadDataset(dataset.name, dataset.description, outputDir, true)
      console.log('')
    } catch (error) {
      console.error(`[imdb] Failed to download ${dataset.name}:`, error)
      throw error
    }
  }

  const totalElapsed = ((Date.now() - startTime) / 1000).toFixed(1)
  console.log(`[imdb] All downloads complete in ${totalElapsed}s`)
}

// CLI support
if (typeof require !== 'undefined' && require.main === module) {
  const args = process.argv.slice(2)
  const outputDir = args[0] || './data/imdb'
  download(outputDir).catch(console.error)
}
