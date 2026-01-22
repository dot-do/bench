/**
 * Download script for standards.org.ai TSV datasets
 * Source: https://github.com/dot-org-ai/standards.org.ai (.data/ directory)
 *
 * Downloads over 200 TSV files containing reference data for industry standards:
 * - ONET (occupations, skills, knowledge areas)
 * - NAICS/NAPCS (industry and product classifications)
 * - ISO (countries, currencies, languages)
 * - W3C (web standards, ARIA, CSS, HTML)
 * - Healthcare (ICD, LOINC, SNOMED, FHIR)
 * - Finance (ISO 20022, SWIFT, LEI)
 * - And many more...
 */

import * as fs from 'fs'
import * as fsp from 'fs/promises'
import * as path from 'path'
import * as crypto from 'crypto'
import { Readable } from 'stream'
import { pipeline } from 'stream/promises'

const GITHUB_RAW_BASE = 'https://raw.githubusercontent.com/dot-org-ai/standards.org.ai/main/.data'
const GITHUB_API_CONTENTS = 'https://api.github.com/repos/dot-org-ai/standards.org.ai/contents/.data'

interface GitHubFile {
  name: string
  path: string
  sha: string
  size: number
  download_url: string
  type: 'file' | 'dir'
}

interface DownloadProgress {
  downloaded: number
  total: number | null
  percentage: string
}

interface FileMetadata {
  sha256: string
  gitSha: string
  size: number
  downloadedAt: string
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
}

async function fetchDirectoryListing(): Promise<GitHubFile[]> {
  console.log(`[standards] Fetching file list from GitHub API...`)

  const response = await fetch(GITHUB_API_CONTENTS, {
    headers: {
      'Accept': 'application/vnd.github.v3+json',
      'User-Agent': 'dotdo-bench-downloader'
    }
  })

  if (!response.ok) {
    throw new Error(`Failed to fetch directory listing: ${response.status} ${response.statusText}`)
  }

  const files: GitHubFile[] = await response.json()

  // Filter to only TSV files
  const tsvFiles = files.filter(f => f.type === 'file' && f.name.endsWith('.tsv'))

  console.log(`[standards] Found ${tsvFiles.length} TSV files`)
  return tsvFiles
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

async function computeFileHash(filePath: string): Promise<string> {
  const hash = crypto.createHash('sha256')
  const stream = fs.createReadStream(filePath)

  for await (const chunk of stream) {
    hash.update(chunk)
  }

  return hash.digest('hex')
}

async function downloadFile(
  file: GitHubFile,
  outputDir: string,
  index: number,
  total: number
): Promise<{ downloaded: boolean; skipped: boolean }> {
  const destPath = path.join(outputDir, file.name)
  const prefix = `[standards] [${index}/${total}]`

  // Check if file already exists with matching git SHA
  const existingMetadata = await loadMetadata(destPath)
  if (existingMetadata && existingMetadata.gitSha === file.sha && fs.existsSync(destPath)) {
    const stats = fs.statSync(destPath)
    if (stats.size === file.size) {
      console.log(`${prefix} ${file.name} - up to date (${formatBytes(file.size)})`)
      return { downloaded: false, skipped: true }
    }
  }

  console.log(`${prefix} Downloading ${file.name} (${formatBytes(file.size)})...`)

  const response = await fetch(file.download_url)

  if (!response.ok) {
    throw new Error(`Failed to download ${file.name}: ${response.status} ${response.statusText}`)
  }

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
      hash.update(value)
      yield value
    }
  }

  const readable = Readable.from(processStream())
  await pipeline(readable, writeStream)

  const sha256 = hash.digest('hex')

  // Save metadata
  await saveMetadata(destPath, {
    sha256,
    gitSha: file.sha,
    size: file.size,
    downloadedAt: new Date().toISOString()
  })

  return { downloaded: true, skipped: false }
}

async function downloadWithConcurrency(
  files: GitHubFile[],
  outputDir: string,
  concurrency: number = 5
): Promise<{ downloaded: number; skipped: number; failed: number }> {
  let downloaded = 0
  let skipped = 0
  let failed = 0
  let currentIndex = 0

  const total = files.length

  async function worker(): Promise<void> {
    while (currentIndex < files.length) {
      const index = ++currentIndex
      const file = files[index - 1]

      try {
        const result = await downloadFile(file, outputDir, index, total)
        if (result.downloaded) downloaded++
        if (result.skipped) skipped++
      } catch (error) {
        console.error(`[standards] Failed to download ${file.name}:`, error)
        failed++
      }
    }
  }

  // Start workers
  const workers = Array(Math.min(concurrency, files.length))
    .fill(null)
    .map(() => worker())

  await Promise.all(workers)

  return { downloaded, skipped, failed }
}

export async function download(outputDir: string): Promise<void> {
  // Ensure output directory exists
  await fsp.mkdir(outputDir, { recursive: true })

  console.log(`[standards] Starting standards.org.ai dataset download`)
  console.log(`[standards] Source: ${GITHUB_RAW_BASE}`)
  console.log(`[standards] Destination: ${outputDir}`)
  console.log('')

  const startTime = Date.now()

  // Fetch list of files
  const files = await fetchDirectoryListing()

  if (files.length === 0) {
    console.log(`[standards] No TSV files found`)
    return
  }

  // Sort files alphabetically for consistent ordering
  files.sort((a, b) => a.name.localeCompare(b.name))

  // Calculate total size
  const totalSize = files.reduce((sum, f) => sum + f.size, 0)
  console.log(`[standards] Total download size: ${formatBytes(totalSize)}`)
  console.log('')

  // Download files with concurrency
  const results = await downloadWithConcurrency(files, outputDir, 5)

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1)

  console.log('')
  console.log(`[standards] Download complete!`)
  console.log(`[standards] Downloaded: ${results.downloaded} files`)
  console.log(`[standards] Skipped (up to date): ${results.skipped} files`)
  if (results.failed > 0) {
    console.log(`[standards] Failed: ${results.failed} files`)
  }
  console.log(`[standards] Time elapsed: ${elapsed}s`)
}

// Export list of categories for reference
export const STANDARD_CATEGORIES = [
  'AdvanceCTE',
  'APQC',
  'BLS',
  'Census',
  'CPT',
  'Ecommerce',
  'EDI',
  'Education',
  'FHIR',
  'Finance',
  'Graph',
  'GS1',
  'GSA',
  'HCPCS',
  'IANA',
  'ICD',
  'ISO',
  'LOINC',
  'NAICS',
  'NAPCS',
  'NDC',
  'NPI',
  'ONET',
  'RxNorm',
  'SBA',
  'SEC',
  'SNOMED',
  'Superset',
  'UN',
  'UNSPSC',
  'USITC',
  'USPTO',
  'W3C',
] as const

// CLI support
if (typeof require !== 'undefined' && require.main === module) {
  const args = process.argv.slice(2)
  const outputDir = args[0] || './data/standards'
  download(outputDir).catch(console.error)
}
