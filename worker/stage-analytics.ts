/**
 * Analytics Dataset Staging Worker
 *
 * A Cloudflare Worker that uses Sandbox SDK to download and stage large analytics
 * datasets like ClickBench and IMDB. Spins up sandbox containers with network access
 * to download from external sources and streams data directly to R2.
 *
 * Supported Datasets:
 * - clickbench: ClickBench hits.parquet (~14GB) - Web analytics benchmark data
 * - imdb: IMDB datasets (title.basics, name.basics, title.ratings) - Movie/TV data
 *
 * Endpoint: POST /stage/{dataset}
 * - POST /stage/clickbench - Downloads ~14GB parquet file
 * - POST /stage/imdb - Downloads multiple TSV files (~2GB total uncompressed)
 *
 * @see scripts/download/clickbench.ts - ClickBench download logic
 * @see scripts/download/imdb.ts - IMDB download logic
 * @see https://datasets.clickhouse.com/hits_compatible/
 * @see https://datasets.imdbws.com/
 */

import { Hono } from 'hono'

// Environment bindings
interface Env {
  // R2 bucket for storing downloaded datasets
  ANALYTICS_BUCKET: R2Bucket
  // Sandbox API token for authentication
  DO_TOKEN: string
  // Sandbox API base URL (optional, defaults to api.do)
  SANDBOX_API_URL?: string
}

// Valid dataset types
type DatasetType = 'clickbench' | 'imdb'

// Dataset configuration
interface DatasetConfig {
  name: string
  description: string
  files: Array<{
    name: string
    url: string
    compressed?: boolean
    expectedSize?: number // in bytes, approximate
  }>
  totalSize: string // Human-readable total size
  downloadTimeout: number // milliseconds
  memoryMb: number // Memory required for sandbox
}

const DATASET_CONFIGS: Record<DatasetType, DatasetConfig> = {
  clickbench: {
    name: 'ClickBench',
    description: 'Web analytics benchmark dataset (~100M rows of anonymized web analytics)',
    files: [
      {
        name: 'hits.parquet',
        url: 'https://datasets.clickhouse.com/hits_compatible/hits.parquet',
        expectedSize: 14_779_976_446, // ~14.8 GB
      },
    ],
    totalSize: '~14 GB',
    downloadTimeout: 1800000, // 30 minutes
    memoryMb: 4096, // 4GB for large file streaming
  },
  imdb: {
    name: 'IMDB',
    description: 'IMDB movie/TV datasets (titles, names, ratings)',
    files: [
      {
        name: 'title.basics.tsv.gz',
        url: 'https://datasets.imdbws.com/title.basics.tsv.gz',
        compressed: true,
        expectedSize: 150_000_000, // ~150 MB compressed, ~700 MB uncompressed
      },
      {
        name: 'name.basics.tsv.gz',
        url: 'https://datasets.imdbws.com/name.basics.tsv.gz',
        compressed: true,
        expectedSize: 250_000_000, // ~250 MB compressed, ~800 MB uncompressed
      },
      {
        name: 'title.ratings.tsv.gz',
        url: 'https://datasets.imdbws.com/title.ratings.tsv.gz',
        compressed: true,
        expectedSize: 7_000_000, // ~7 MB compressed, ~25 MB uncompressed
      },
    ],
    totalSize: '~400 MB compressed (~1.5 GB uncompressed)',
    downloadTimeout: 600000, // 10 minutes
    memoryMb: 2048, // 2GB for decompression
  },
}

// Sandbox API client
class SandboxClient {
  private baseUrl: string
  private token: string

  constructor(token: string, baseUrl = 'https://api.do') {
    this.token = token
    this.baseUrl = baseUrl
  }

  private async request<T>(method: string, path: string, body?: unknown): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      method,
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${this.token}`,
      },
      body: body ? JSON.stringify(body) : undefined,
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`Sandbox API error: ${response.status} ${error}`)
    }

    return response.json()
  }

  /**
   * Create a new sandbox environment with network access
   */
  async create(options: {
    name: string
    runtime: 'node'
    memory?: number
    timeout?: number
    networkAccess: boolean
  }): Promise<{ id: string; name: string; status: string }> {
    return this.request('POST', '/sandboxs', {
      name: options.name,
      type: 'vm',
      runtime: options.runtime,
      memory: options.memory ?? 2048,
      timeout: options.timeout ?? 600000, // 10 minutes default
      networkAccess: options.networkAccess, // Required for downloading
      fileSystem: true,
    })
  }

  /**
   * Execute code in a sandbox
   */
  async execute(
    sandboxId: string,
    options: {
      code: string
      language?: 'typescript' | 'javascript'
    }
  ): Promise<{ output: string; exitCode: number; duration: number }> {
    return this.request('POST', `/sandboxs/${sandboxId}/execute`, {
      code: options.code,
      language: options.language ?? 'javascript',
    })
  }

  /**
   * Run a shell command in a sandbox
   */
  async run(
    sandboxId: string,
    command: string
  ): Promise<{ stdout: string; stderr: string; exitCode: number }> {
    return this.request('POST', `/sandboxs/${sandboxId}/run`, {
      command,
    })
  }

  /**
   * Read a file from the sandbox as a stream
   * Returns file info and can be used to stream content to R2
   */
  async readFileStream(
    sandboxId: string,
    path: string
  ): Promise<Response> {
    const response = await fetch(`${this.baseUrl}/sandboxs/${sandboxId}/files${path}`, {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${this.token}`,
      },
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`Failed to read file: ${response.status} ${error}`)
    }

    return response
  }

  /**
   * Get file info from the sandbox
   */
  async statFile(
    sandboxId: string,
    path: string
  ): Promise<{ size: number; exists: boolean }> {
    return this.request('POST', `/sandboxs/${sandboxId}/stat`, {
      path,
    })
  }

  /**
   * Delete a sandbox
   */
  async delete(sandboxId: string): Promise<void> {
    await this.request('DELETE', `/sandboxs/${sandboxId}`)
  }
}

/**
 * Generate the download script to run in the sandbox
 * This script downloads files and stores them in /output directory
 */
function getDownloadScript(dataset: DatasetType): string {
  const config = DATASET_CONFIGS[dataset]

  const baseCode = `
const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const zlib = require('zlib');
const { pipeline } = require('stream/promises');

const OUTPUT_DIR = '/output';
fs.mkdirSync(OUTPUT_DIR, { recursive: true });

function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function downloadFile(url, destPath, decompress = false) {
  return new Promise((resolve, reject) => {
    console.log('Downloading: ' + url);
    console.log('Destination: ' + destPath);

    const protocol = url.startsWith('https') ? https : http;

    const request = protocol.get(url, (response) => {
      // Handle redirects
      if (response.statusCode >= 300 && response.statusCode < 400 && response.headers.location) {
        console.log('Redirecting to: ' + response.headers.location);
        return downloadFile(response.headers.location, destPath, decompress).then(resolve).catch(reject);
      }

      if (response.statusCode !== 200) {
        return reject(new Error('Download failed with status: ' + response.statusCode));
      }

      const contentLength = response.headers['content-length'];
      const totalSize = contentLength ? parseInt(contentLength, 10) : null;
      let downloaded = 0;
      let lastLog = Date.now();

      console.log('Content-Length: ' + (totalSize ? formatBytes(totalSize) : 'unknown'));

      // Create write stream, optionally with decompression
      const writeStream = fs.createWriteStream(destPath);
      let sourceStream = response;

      if (decompress) {
        console.log('Decompressing gzip stream...');
        const gunzip = zlib.createGunzip();
        sourceStream = response.pipe(gunzip);
      }

      response.on('data', (chunk) => {
        downloaded += chunk.length;
        const now = Date.now();
        if (now - lastLog >= 5000) { // Log every 5 seconds
          const progress = totalSize ? ((downloaded / totalSize) * 100).toFixed(1) + '%' : formatBytes(downloaded);
          console.log('Progress: ' + formatBytes(downloaded) + ' / ' + (totalSize ? formatBytes(totalSize) : 'unknown') + ' (' + progress + ')');
          lastLog = now;
        }
      });

      sourceStream.pipe(writeStream);

      writeStream.on('finish', () => {
        const stats = fs.statSync(destPath);
        console.log('Download complete: ' + destPath);
        console.log('Final size: ' + formatBytes(stats.size));
        resolve({ path: destPath, size: stats.size });
      });

      writeStream.on('error', reject);
      sourceStream.on('error', reject);
    });

    request.on('error', reject);
    request.setTimeout(1800000, () => { // 30 minute timeout
      request.destroy();
      reject(new Error('Download timeout'));
    });
  });
}

async function main() {
  const results = [];
  const errors = [];
`

  // Generate download calls for each file
  let downloadCalls = ''
  for (const file of config.files) {
    const destName = file.compressed ? file.name.replace('.gz', '') : file.name
    const destPath = `/output/${destName}`
    downloadCalls += `
  try {
    console.log('\\n=== Downloading ${file.name} ===');
    const result = await downloadFile(
      '${file.url}',
      '${destPath}',
      ${file.compressed ? 'true' : 'false'}
    );
    results.push({
      name: '${destName}',
      originalName: '${file.name}',
      size: result.size,
      path: result.path,
    });
  } catch (error) {
    console.error('Failed to download ${file.name}:', error.message);
    errors.push({
      name: '${file.name}',
      error: error.message,
    });
  }
`
  }

  const endCode = `
  console.log('\\n=== Download Summary ===');
  console.log('Successful: ' + results.length);
  console.log('Failed: ' + errors.length);

  // Output JSON result for parsing
  console.log('\\n__RESULT_JSON__');
  console.log(JSON.stringify({ success: errors.length === 0, results, errors }));
}

main().catch((error) => {
  console.error('Fatal error:', error);
  console.log('\\n__RESULT_JSON__');
  console.log(JSON.stringify({ success: false, results: [], errors: [{ name: 'fatal', error: error.message }] }));
  process.exit(1);
});
`

  return baseCode + downloadCalls + endCode
}

/**
 * Parse the result JSON from sandbox output
 */
function parseDownloadResult(output: string): {
  success: boolean
  results: Array<{ name: string; originalName: string; size: number; path: string }>
  errors: Array<{ name: string; error: string }>
} {
  const marker = '__RESULT_JSON__'
  const markerIndex = output.indexOf(marker)

  if (markerIndex === -1) {
    throw new Error('Could not find result marker in sandbox output')
  }

  const jsonStr = output.slice(markerIndex + marker.length).trim().split('\n')[0]
  return JSON.parse(jsonStr)
}

// Response types
interface StageResult {
  success: boolean
  dataset: DatasetType
  duration: number
  files: Array<{
    name: string
    size: number
    key: string
  }>
  totalSize: number
  error?: string
  sandboxLogs?: string
}

// Create Hono app
const app = new Hono<{ Bindings: Env }>()

// Health check
app.get('/health', (c) => {
  return c.json({ status: 'ok', service: 'stage-analytics' })
})

// List available datasets
app.get('/datasets', (c) => {
  return c.json({
    datasets: Object.entries(DATASET_CONFIGS).map(([key, config]) => ({
      id: key,
      name: config.name,
      description: config.description,
      totalSize: config.totalSize,
      files: config.files.map((f) => ({
        name: f.name,
        expectedSize: f.expectedSize,
        compressed: f.compressed ?? false,
      })),
    })),
  })
})

// Stage a dataset
app.post('/stage/:dataset', async (c) => {
  const dataset = c.req.param('dataset') as DatasetType
  const startTime = Date.now()

  // Validate dataset
  if (!DATASET_CONFIGS[dataset]) {
    return c.json(
      {
        success: false,
        error: `Invalid dataset: ${dataset}. Valid options: ${Object.keys(DATASET_CONFIGS).join(', ')}`,
      },
      400
    )
  }

  const config = DATASET_CONFIGS[dataset]

  // Check if dataset already exists in R2
  const existingPrefix = `analytics/${dataset}/`
  const existingFiles = await c.env.ANALYTICS_BUCKET.list({ prefix: existingPrefix })

  if (existingFiles.objects.length >= config.files.length) {
    // Return existing dataset info
    const files = existingFiles.objects.map((obj) => ({
      name: obj.key.split('/').pop()!,
      size: obj.size,
      key: obj.key,
    }))
    const totalSize = files.reduce((sum, f) => sum + f.size, 0)

    return c.json({
      success: true,
      cached: true,
      dataset,
      duration: Date.now() - startTime,
      files,
      totalSize,
      message: 'Dataset already staged in R2',
    })
  }

  // Create sandbox client
  const sandbox = new SandboxClient(
    c.env.DO_TOKEN,
    c.env.SANDBOX_API_URL || 'https://api.do'
  )

  let sandboxId: string | null = null
  let sandboxLogs = ''

  try {
    // Create sandbox with network access
    console.log(`Creating sandbox for ${dataset} with network access...`)
    const created = await sandbox.create({
      name: `analytics-${dataset}-${Date.now()}`,
      runtime: 'node',
      memory: config.memoryMb,
      timeout: config.downloadTimeout,
      networkAccess: true, // Required for external downloads
    })
    sandboxId = created.id
    console.log(`Sandbox created: ${sandboxId}`)

    // Execute download script
    console.log(`Executing download script for ${dataset}...`)
    const script = getDownloadScript(dataset)
    const result = await sandbox.execute(sandboxId, {
      code: script,
      language: 'javascript',
    })

    sandboxLogs = result.output
    console.log(`Script execution complete. Exit code: ${result.exitCode}, Duration: ${result.duration}ms`)

    // Parse result
    const downloadResult = parseDownloadResult(result.output)

    if (!downloadResult.success || downloadResult.errors.length > 0) {
      const errorMessages = downloadResult.errors.map((e) => `${e.name}: ${e.error}`).join('; ')
      throw new Error(`Download failed: ${errorMessages}`)
    }

    // Stream files from sandbox to R2
    const uploadedFiles: Array<{ name: string; size: number; key: string }> = []

    for (const file of downloadResult.results) {
      console.log(`Streaming ${file.name} to R2...`)

      // Stream file from sandbox to R2
      const fileResponse = await sandbox.readFileStream(sandboxId, file.path)

      if (!fileResponse.body) {
        throw new Error(`Failed to get stream for ${file.name}`)
      }

      // Determine content type
      const contentType = file.name.endsWith('.parquet')
        ? 'application/octet-stream'
        : file.name.endsWith('.tsv')
        ? 'text/tab-separated-values'
        : 'application/octet-stream'

      // Upload to R2
      const r2Key = `analytics/${dataset}/${file.name}`
      await c.env.ANALYTICS_BUCKET.put(r2Key, fileResponse.body, {
        httpMetadata: {
          contentType,
        },
        customMetadata: {
          dataset,
          originalName: file.originalName,
          stagedAt: new Date().toISOString(),
          sourceUrl: config.files.find((f) =>
            f.name === file.originalName || f.name.replace('.gz', '') === file.name
          )?.url ?? '',
        },
      })

      // Verify upload
      const uploaded = await c.env.ANALYTICS_BUCKET.head(r2Key)
      const uploadedSize = uploaded?.size ?? file.size

      uploadedFiles.push({
        name: file.name,
        size: uploadedSize,
        key: r2Key,
      })

      console.log(`Uploaded ${file.name}: ${formatBytes(uploadedSize)}`)
    }

    const totalSize = uploadedFiles.reduce((sum, f) => sum + f.size, 0)

    const response: StageResult = {
      success: true,
      dataset,
      duration: Date.now() - startTime,
      files: uploadedFiles,
      totalSize,
    }

    return c.json(response)
  } catch (error) {
    console.error(`Error staging ${dataset}:`, error)
    return c.json(
      {
        success: false,
        dataset,
        duration: Date.now() - startTime,
        files: [],
        totalSize: 0,
        error: error instanceof Error ? error.message : String(error),
        sandboxLogs: sandboxLogs.slice(-5000), // Last 5KB of logs for debugging
      } as StageResult,
      500
    )
  } finally {
    // Cleanup: delete sandbox
    if (sandboxId) {
      try {
        await sandbox.delete(sandboxId)
        console.log(`Sandbox ${sandboxId} deleted`)
      } catch (e) {
        console.error(`Failed to delete sandbox ${sandboxId}:`, e)
      }
    }
  }
})

// Check dataset status
app.get('/status/:dataset', async (c) => {
  const dataset = c.req.param('dataset') as DatasetType

  // Validate dataset
  if (!DATASET_CONFIGS[dataset]) {
    return c.json({ error: `Invalid dataset: ${dataset}` }, 400)
  }

  const config = DATASET_CONFIGS[dataset]
  const prefix = `analytics/${dataset}/`
  const files = await c.env.ANALYTICS_BUCKET.list({ prefix })

  if (files.objects.length === 0) {
    return c.json({
      exists: false,
      dataset,
      expectedFiles: config.files.map((f) => f.name),
      files: [],
    })
  }

  const fileList = files.objects.map((obj) => ({
    name: obj.key.split('/').pop(),
    size: obj.size,
    key: obj.key,
    uploaded: obj.uploaded,
  }))

  const totalSize = fileList.reduce((sum, f) => sum + f.size, 0)

  return c.json({
    exists: true,
    complete: files.objects.length >= config.files.length,
    dataset,
    files: fileList,
    totalSize,
    totalSizeFormatted: formatBytes(totalSize),
  })
})

// Delete a staged dataset
app.delete('/stage/:dataset', async (c) => {
  const dataset = c.req.param('dataset') as DatasetType

  // Validate dataset
  if (!DATASET_CONFIGS[dataset]) {
    return c.json({ error: `Invalid dataset: ${dataset}` }, 400)
  }

  const prefix = `analytics/${dataset}/`
  const files = await c.env.ANALYTICS_BUCKET.list({ prefix })

  let deleted = 0
  for (const obj of files.objects) {
    await c.env.ANALYTICS_BUCKET.delete(obj.key)
    deleted++
  }

  return c.json({
    success: true,
    dataset,
    deleted,
  })
})

// Stage all datasets
app.post('/stage-all', async (c) => {
  const results: StageResult[] = []
  const datasets: DatasetType[] = ['clickbench', 'imdb']

  for (const dataset of datasets) {
    // Make internal request to stage endpoint
    const response = await app.fetch(
      new Request(`http://localhost/stage/${dataset}`, {
        method: 'POST',
      }),
      c.env
    )
    const result = (await response.json()) as StageResult
    results.push(result)
  }

  const successful = results.filter((r) => r.success).length
  const failed = results.filter((r) => !r.success).length
  const totalSize = results.reduce((sum, r) => sum + (r.totalSize || 0), 0)

  return c.json({
    summary: {
      successful,
      failed,
      total: datasets.length,
      totalSize,
      totalSizeFormatted: formatBytes(totalSize),
    },
    results,
  })
})

// Utility function for formatting bytes
function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
}

export default app
