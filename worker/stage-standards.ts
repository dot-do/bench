/**
 * Standards Dataset Staging Worker
 *
 * Stages standards datasets from standards.org.ai using Cloudflare Containers.
 * Clones TSV files from GitHub, converts to multiple formats, and stores in R2.
 *
 * Endpoint: POST /stage/standards
 * Query params:
 *   - format: comma-separated list of formats (sqlite,jsonl,parquet,triples)
 *             Default: all formats
 *
 * The worker spins up a sandbox container to:
 * 1. Clone/fetch TSV files from github.com/dot-org-ai/standards.org.ai .data/
 * 2. Convert to requested formats using conversion scripts
 * 3. Store results in R2 bucket organized by format
 *
 * @see https://developers.cloudflare.com/containers/
 */

import type { Container } from '@cloudflare/containers'

// =============================================================================
// Types
// =============================================================================

export interface Env {
  // Container binding for the staging sandbox
  STAGING_CONTAINER: Container

  // R2 bucket for staged datasets
  STANDARDS_BUCKET: R2Bucket

  // Optional: GitHub token for higher rate limits
  GITHUB_TOKEN?: string
}

type OutputFormat = 'sqlite' | 'jsonl' | 'parquet' | 'triples'

interface StagingParams {
  formats: OutputFormat[]
}

interface StagingProgress {
  phase: 'init' | 'fetch' | 'convert' | 'upload' | 'complete' | 'error'
  message: string
  filesTotal?: number
  filesProcessed?: number
  currentFile?: string
  formatProgress?: Record<OutputFormat, { processed: number; total: number }>
}

interface StagingResult {
  timestamp: string
  durationMs: number
  formats: OutputFormat[]
  files: {
    format: OutputFormat
    path: string
    sizeBytes: number
    recordCount?: number
  }[]
  summary: {
    totalFiles: number
    totalSizeBytes: number
    byFormat: Record<OutputFormat, { files: number; sizeBytes: number }>
  }
  containerMetrics: {
    coldStartMs: number
    fetchMs: number
    convertMs: number
    uploadMs: number
  }
  errors: string[]
}

// =============================================================================
// Constants
// =============================================================================

const GITHUB_API_CONTENTS = 'https://api.github.com/repos/dot-org-ai/standards.org.ai/contents/.data'
const GITHUB_RAW_BASE = 'https://raw.githubusercontent.com/dot-org-ai/standards.org.ai/main/.data'

const ALL_FORMATS: OutputFormat[] = ['sqlite', 'jsonl', 'parquet', 'triples']

const FORMAT_EXTENSIONS: Record<OutputFormat, string> = {
  sqlite: '.db',
  jsonl: '.jsonl',
  parquet: '.parquet',
  triples: '.nt',
}

// Standard categories from standards.org.ai
const STANDARD_CATEGORIES = [
  'AdvanceCTE', 'APQC', 'BLS', 'Census', 'CPT', 'Ecommerce', 'EDI',
  'Education', 'FHIR', 'Finance', 'Graph', 'GS1', 'GSA', 'HCPCS',
  'IANA', 'ICD', 'ISO', 'LOINC', 'NAICS', 'NAPCS', 'NDC', 'NPI',
  'ONET', 'RxNorm', 'SBA', 'SEC', 'SNOMED', 'Superset', 'UN',
  'UNSPSC', 'USITC', 'USPTO', 'W3C',
] as const

// =============================================================================
// Worker Entry Point
// =============================================================================

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url)
    const path = url.pathname

    // CORS headers
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    }

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders })
    }

    try {
      // POST /stage/standards - Main staging endpoint
      if (path === '/stage/standards' && request.method === 'POST') {
        const params = parseStagingParams(url)
        const result = await stageStandards(params, env, ctx)

        return new Response(JSON.stringify(result, null, 2), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        })
      }

      // GET /stage/standards/status - Check staging status
      if (path === '/stage/standards/status' && request.method === 'GET') {
        const status = await getStagingStatus(env)
        return new Response(JSON.stringify(status, null, 2), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        })
      }

      // GET /stage/standards/list - List available staged files
      if (path === '/stage/standards/list' && request.method === 'GET') {
        const format = url.searchParams.get('format') as OutputFormat | null
        const list = await listStagedFiles(env, format)
        return new Response(JSON.stringify(list, null, 2), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        })
      }

      // GET /health
      if (path === '/health') {
        return new Response(JSON.stringify({
          status: 'ok',
          timestamp: new Date().toISOString(),
          service: 'stage-standards',
        }), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        })
      }

      // API documentation
      return new Response(JSON.stringify({
        service: 'Standards Dataset Staging Worker',
        description: 'Stages standards.org.ai datasets to R2 in multiple formats',
        endpoints: {
          'POST /stage/standards': {
            description: 'Stage standards datasets from GitHub to R2',
            queryParams: {
              format: 'Comma-separated formats: sqlite,jsonl,parquet,triples (default: all)',
            },
            example: 'POST /stage/standards?format=sqlite,jsonl',
          },
          'GET /stage/standards/status': {
            description: 'Get current staging status and last run info',
          },
          'GET /stage/standards/list': {
            description: 'List staged files in R2',
            queryParams: {
              format: 'Filter by format (optional)',
            },
          },
          'GET /health': {
            description: 'Health check endpoint',
          },
        },
        source: 'github.com/dot-org-ai/standards.org.ai',
        categories: STANDARD_CATEGORIES,
      }), {
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      })
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error)
      const errorStack = error instanceof Error ? error.stack : undefined

      console.error('Stage standards error:', errorMessage, errorStack)

      return new Response(JSON.stringify({
        error: errorMessage,
        stack: errorStack,
        timestamp: new Date().toISOString(),
      }), {
        status: 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      })
    }
  },
}

// =============================================================================
// Parameter Parsing
// =============================================================================

function parseStagingParams(url: URL): StagingParams {
  const formatParam = url.searchParams.get('format')

  let formats: OutputFormat[]
  if (formatParam) {
    formats = formatParam.split(',').map(f => f.trim().toLowerCase()) as OutputFormat[]

    // Validate formats
    for (const format of formats) {
      if (!ALL_FORMATS.includes(format)) {
        throw new Error(`Invalid format: ${format}. Valid formats: ${ALL_FORMATS.join(', ')}`)
      }
    }
  } else {
    formats = [...ALL_FORMATS]
  }

  return { formats }
}

// =============================================================================
// Main Staging Logic
// =============================================================================

async function stageStandards(
  params: StagingParams,
  env: Env,
  ctx: ExecutionContext
): Promise<StagingResult> {
  const startTime = performance.now()
  const errors: string[] = []
  const files: StagingResult['files'] = []

  const metrics = {
    coldStartMs: 0,
    fetchMs: 0,
    convertMs: 0,
    uploadMs: 0,
  }

  // Phase 1: Initialize container and measure cold start
  console.log('[stage-standards] Initializing staging container...')
  const coldStartStart = performance.now()

  const containerReady = await initializeContainer(env.STAGING_CONTAINER)
  metrics.coldStartMs = performance.now() - coldStartStart

  if (!containerReady) {
    throw new Error('Failed to initialize staging container')
  }

  // Phase 2: Fetch TSV file list from GitHub
  console.log('[stage-standards] Fetching file list from GitHub...')
  const fetchStart = performance.now()

  const tsvFiles = await fetchTsvFileList(env.GITHUB_TOKEN)
  metrics.fetchMs = performance.now() - fetchStart

  console.log(`[stage-standards] Found ${tsvFiles.length} TSV files`)

  // Phase 3: Process each file through the container
  console.log(`[stage-standards] Converting to formats: ${params.formats.join(', ')}`)
  const convertStart = performance.now()

  for (const tsvFile of tsvFiles) {
    try {
      // Fetch TSV content
      const tsvContent = await fetchTsvContent(tsvFile.download_url, env.GITHUB_TOKEN)

      // Convert through container for each format
      for (const format of params.formats) {
        try {
          const result = await convertInContainer(
            env.STAGING_CONTAINER,
            tsvFile.name,
            tsvContent,
            format
          )

          // Store result
          const r2Key = `${format}/${tsvFile.name.replace('.tsv', FORMAT_EXTENSIONS[format])}`
          await env.STANDARDS_BUCKET.put(r2Key, result.data, {
            customMetadata: {
              sourceFile: tsvFile.name,
              format: format,
              recordCount: String(result.recordCount),
              convertedAt: new Date().toISOString(),
              gitSha: tsvFile.sha,
            },
          })

          files.push({
            format,
            path: r2Key,
            sizeBytes: result.data.byteLength,
            recordCount: result.recordCount,
          })
        } catch (formatError) {
          const msg = `Failed to convert ${tsvFile.name} to ${format}: ${formatError}`
          console.error(`[stage-standards] ${msg}`)
          errors.push(msg)
        }
      }
    } catch (fileError) {
      const msg = `Failed to process ${tsvFile.name}: ${fileError}`
      console.error(`[stage-standards] ${msg}`)
      errors.push(msg)
    }
  }

  metrics.convertMs = performance.now() - convertStart

  // Calculate upload time (already included in convert loop, but measure total)
  metrics.uploadMs = performance.now() - convertStart - metrics.convertMs

  // Build summary
  const byFormat: Record<OutputFormat, { files: number; sizeBytes: number }> = {
    sqlite: { files: 0, sizeBytes: 0 },
    jsonl: { files: 0, sizeBytes: 0 },
    parquet: { files: 0, sizeBytes: 0 },
    triples: { files: 0, sizeBytes: 0 },
  }

  for (const file of files) {
    byFormat[file.format].files++
    byFormat[file.format].sizeBytes += file.sizeBytes
  }

  const totalSizeBytes = files.reduce((sum, f) => sum + f.sizeBytes, 0)

  // Store staging manifest
  const manifest = {
    timestamp: new Date().toISOString(),
    formats: params.formats,
    filesProcessed: files.length,
    totalSizeBytes,
    errors: errors.length,
  }

  await env.STANDARDS_BUCKET.put('_manifest.json', JSON.stringify(manifest, null, 2), {
    customMetadata: {
      type: 'manifest',
      lastUpdated: new Date().toISOString(),
    },
  })

  return {
    timestamp: new Date().toISOString(),
    durationMs: performance.now() - startTime,
    formats: params.formats,
    files,
    summary: {
      totalFiles: files.length,
      totalSizeBytes,
      byFormat,
    },
    containerMetrics: metrics,
    errors,
  }
}

// =============================================================================
// Container Operations
// =============================================================================

interface GitHubFile {
  name: string
  path: string
  sha: string
  size: number
  download_url: string
  type: 'file' | 'dir'
}

async function initializeContainer(container: Container): Promise<boolean> {
  try {
    // Send health check to wake up container
    const healthRequest = new Request('http://localhost:8080/health', {
      method: 'GET',
    })

    const response = await container.fetch(healthRequest)
    return response.ok
  } catch (error) {
    console.error('[stage-standards] Container init error:', error)
    return false
  }
}

async function fetchTsvFileList(githubToken?: string): Promise<GitHubFile[]> {
  const headers: HeadersInit = {
    'Accept': 'application/vnd.github.v3+json',
    'User-Agent': 'dotdo-bench-stage-standards',
  }

  if (githubToken) {
    headers['Authorization'] = `token ${githubToken}`
  }

  const response = await fetch(GITHUB_API_CONTENTS, { headers })

  if (!response.ok) {
    throw new Error(`GitHub API error: ${response.status} ${response.statusText}`)
  }

  const files: GitHubFile[] = await response.json()

  // Filter to only TSV files
  return files.filter(f => f.type === 'file' && f.name.endsWith('.tsv'))
}

async function fetchTsvContent(downloadUrl: string, githubToken?: string): Promise<string> {
  const headers: HeadersInit = {
    'User-Agent': 'dotdo-bench-stage-standards',
  }

  if (githubToken) {
    headers['Authorization'] = `token ${githubToken}`
  }

  const response = await fetch(downloadUrl, { headers })

  if (!response.ok) {
    throw new Error(`Failed to fetch TSV: ${response.status}`)
  }

  return response.text()
}

interface ConversionResult {
  data: ArrayBuffer
  recordCount: number
}

async function convertInContainer(
  container: Container,
  filename: string,
  tsvContent: string,
  format: OutputFormat
): Promise<ConversionResult> {
  // Send conversion request to container
  const request = new Request('http://localhost:8080/convert', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      filename,
      content: tsvContent,
      format,
    }),
  })

  const response = await container.fetch(request)

  if (!response.ok) {
    const errorText = await response.text()
    throw new Error(`Conversion failed: ${response.status} - ${errorText}`)
  }

  // Get record count from header
  const recordCount = parseInt(response.headers.get('X-Record-Count') || '0', 10)

  return {
    data: await response.arrayBuffer(),
    recordCount,
  }
}

// =============================================================================
// Status and Listing
// =============================================================================

async function getStagingStatus(env: Env): Promise<{
  status: 'ready' | 'unknown'
  lastRun?: {
    timestamp: string
    formats: string[]
    filesProcessed: number
    totalSizeBytes: number
    errors: number
  }
}> {
  try {
    const manifest = await env.STANDARDS_BUCKET.get('_manifest.json')

    if (!manifest) {
      return { status: 'ready' }
    }

    const data = await manifest.json() as {
      timestamp: string
      formats: string[]
      filesProcessed: number
      totalSizeBytes: number
      errors: number
    }

    return {
      status: 'ready',
      lastRun: data,
    }
  } catch {
    return { status: 'unknown' }
  }
}

async function listStagedFiles(
  env: Env,
  format?: OutputFormat | null
): Promise<{
  files: {
    key: string
    size: number
    uploaded: string
    format: string
    sourceFile?: string
    recordCount?: number
  }[]
  totalFiles: number
  totalSizeBytes: number
}> {
  const prefix = format ? `${format}/` : ''
  const listed = await env.STANDARDS_BUCKET.list({ prefix })

  const files = listed.objects
    .filter(obj => !obj.key.startsWith('_')) // Exclude manifest
    .map(obj => ({
      key: obj.key,
      size: obj.size,
      uploaded: obj.uploaded.toISOString(),
      format: obj.key.split('/')[0],
      sourceFile: obj.customMetadata?.sourceFile,
      recordCount: obj.customMetadata?.recordCount
        ? parseInt(obj.customMetadata.recordCount, 10)
        : undefined,
    }))

  return {
    files,
    totalFiles: files.length,
    totalSizeBytes: files.reduce((sum, f) => sum + f.size, 0),
  }
}
