/**
 * Standards Dataset Staging Worker
 *
 * A Cloudflare Worker that generates synthetic technical standards/specification
 * data directly in-Worker. Uses deterministic seeding for reproducible data
 * generation. Generated JSONL files are stored in R2 for benchmarking document
 * search and retrieval.
 *
 * Endpoint: POST /stage/standards/{size}
 * - size: 1mb | 10mb | 100mb | 1gb
 *
 * Generated Data:
 * - standards.jsonl: Technical standards with IDs, titles, versions, dates
 * - sections.jsonl: Document sections and clauses within standards
 * - references.jsonl: Cross-references between standards
 *
 * @see worker/stage-oltp.ts - Reference for in-Worker data generation pattern
 */

import { Hono } from 'hono'

// =============================================================================
// Types
// =============================================================================

interface Env {
  // R2 bucket for storing generated datasets
  STANDARDS_BUCKET: R2Bucket
}

type SizeOption = '1mb' | '10mb' | '100mb' | '1gb'

// Size configuration for dataset generation
interface SizeConfig {
  standards: number
  sectionsPerStandard: number
  referencesPerStandard: number
}

const SIZE_CONFIGS: Record<SizeOption, SizeConfig> = {
  '1mb': { standards: 50, sectionsPerStandard: 15, referencesPerStandard: 5 },
  '10mb': { standards: 500, sectionsPerStandard: 15, referencesPerStandard: 5 },
  '100mb': { standards: 5000, sectionsPerStandard: 15, referencesPerStandard: 5 },
  '1gb': { standards: 50000, sectionsPerStandard: 15, referencesPerStandard: 5 },
}

const VALID_SIZES: SizeOption[] = ['1mb', '10mb', '100mb', '1gb']

// =============================================================================
// Deterministic Random Number Generator (Mulberry32)
// =============================================================================

function createRng(seed: number): () => number {
  return function () {
    let t = (seed += 0x6d2b79f5)
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

function pick<T>(arr: readonly T[], rng: () => number): T {
  return arr[Math.floor(rng() * arr.length)]
}

function pickN<T>(arr: readonly T[], n: number, rng: () => number): T[] {
  const shuffled = [...arr].sort(() => rng() - 0.5)
  return shuffled.slice(0, Math.min(n, arr.length))
}

function generateUuid(rng: () => number): string {
  const hex = '0123456789abcdef'
  let uuid = ''
  for (let i = 0; i < 36; i++) {
    if (i === 8 || i === 13 || i === 18 || i === 23) {
      uuid += '-'
    } else if (i === 14) {
      uuid += '4'
    } else if (i === 19) {
      uuid += hex[Math.floor(rng() * 4) + 8]
    } else {
      uuid += hex[Math.floor(rng() * 16)]
    }
  }
  return uuid
}

function generateDate(rng: () => number, startYear: number, endYear: number): string {
  const year = startYear + Math.floor(rng() * (endYear - startYear + 1))
  const month = Math.floor(rng() * 12) + 1
  const day = Math.floor(rng() * 28) + 1
  return `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`
}

// =============================================================================
// Standards Organization Data
// =============================================================================

const STANDARDS_SEED = 78901

// Standards Development Organizations (SDOs)
const ORGANIZATIONS = [
  { prefix: 'ISO', name: 'International Organization for Standardization', type: 'international' },
  { prefix: 'IEC', name: 'International Electrotechnical Commission', type: 'international' },
  { prefix: 'IEEE', name: 'Institute of Electrical and Electronics Engineers', type: 'professional' },
  { prefix: 'IETF', name: 'Internet Engineering Task Force', type: 'technical' },
  { prefix: 'W3C', name: 'World Wide Web Consortium', type: 'technical' },
  { prefix: 'OASIS', name: 'Organization for the Advancement of Structured Information Standards', type: 'consortium' },
  { prefix: 'NIST', name: 'National Institute of Standards and Technology', type: 'government' },
  { prefix: 'ANSI', name: 'American National Standards Institute', type: 'national' },
  { prefix: 'ECMA', name: 'Ecma International', type: 'international' },
  { prefix: 'ITU', name: 'International Telecommunication Union', type: 'international' },
  { prefix: 'OMG', name: 'Object Management Group', type: 'consortium' },
  { prefix: 'ASTM', name: 'ASTM International', type: 'international' },
  { prefix: 'CEN', name: 'European Committee for Standardization', type: 'regional' },
  { prefix: 'BSI', name: 'British Standards Institution', type: 'national' },
  { prefix: 'DIN', name: 'Deutsches Institut fur Normung', type: 'national' },
] as const

// Technical domains/categories
const CATEGORIES = [
  'information-technology',
  'telecommunications',
  'software-engineering',
  'data-management',
  'security',
  'cryptography',
  'networking',
  'web-technologies',
  'cloud-computing',
  'artificial-intelligence',
  'machine-learning',
  'quality-management',
  'systems-engineering',
  'enterprise-architecture',
  'interoperability',
  'accessibility',
  'privacy',
  'compliance',
  'testing',
  'documentation',
] as const

// Standard statuses
const STATUSES = [
  'draft',
  'proposed',
  'active',
  'under-revision',
  'withdrawn',
  'superseded',
] as const

// Keywords/tags for standards
const KEYWORDS = [
  'api', 'authentication', 'authorization', 'cloud', 'compliance',
  'configuration', 'cryptography', 'data-exchange', 'data-format',
  'database', 'documentation', 'encryption', 'framework', 'governance',
  'guidelines', 'identity', 'implementation', 'integration', 'interface',
  'interoperability', 'lifecycle', 'management', 'messaging', 'metadata',
  'methodology', 'model', 'networking', 'ontology', 'performance',
  'privacy', 'process', 'protocol', 'quality', 'reference-architecture',
  'requirements', 'risk', 'schema', 'security', 'semantic',
  'service', 'specification', 'testing', 'terminology', 'validation',
  'vocabulary', 'workflow', 'xml', 'json', 'rest',
] as const

// Section types for document structure
const SECTION_TYPES = [
  'scope',
  'normative-references',
  'terms-and-definitions',
  'symbols-and-abbreviations',
  'requirements',
  'conformance',
  'architecture',
  'data-model',
  'protocol',
  'security-considerations',
  'implementation-guidelines',
  'examples',
  'annexes',
  'bibliography',
] as const

// Reference relationship types
const REFERENCE_TYPES = [
  'normative',
  'informative',
  'supersedes',
  'superseded-by',
  'related-to',
  'derived-from',
  'extends',
  'part-of',
  'implements',
  'profiles',
] as const

// Title templates for generating realistic standard names
const TITLE_TEMPLATES = [
  '{domain} - {topic} - Part {part}: {subtitle}',
  '{domain} -- {topic} -- {subtitle}',
  '{topic} for {application}',
  '{topic} specification',
  '{topic} - {aspect} requirements',
  '{topic} framework',
  '{topic} reference model',
  '{topic} architecture',
  '{topic} protocol',
  '{topic} data format',
  'Guidelines for {topic}',
  '{topic} best practices',
  'Requirements for {topic} systems',
  '{topic} interoperability specification',
  '{topic} conformance testing',
]

const DOMAINS = [
  'Information technology',
  'Software engineering',
  'Data management',
  'Systems and software engineering',
  'Computer graphics',
  'Database languages',
  'Information security',
  'Cloud computing',
  'Web services',
  'Document management',
]

const TOPICS = [
  'Quality management',
  'Metadata registries',
  'Service-oriented architecture',
  'Data interchange',
  'Process assessment',
  'Software life cycle',
  'Configuration management',
  'Requirements engineering',
  'Testing methodologies',
  'Security techniques',
  'Access control',
  'Identity management',
  'Cryptographic algorithms',
  'Digital signatures',
  'Key management',
  'Risk assessment',
  'Incident management',
  'Business continuity',
  'Data governance',
  'Privacy framework',
]

const SUBTITLES = [
  'General concepts and principles',
  'Vocabulary and terminology',
  'Reference architecture',
  'Conformance requirements',
  'Implementation guidelines',
  'Data exchange format',
  'Protocol specification',
  'Security framework',
  'Assessment framework',
  'Metrics and measurement',
  'Process model',
  'Capability levels',
  'Functional requirements',
  'Non-functional requirements',
  'Interoperability profile',
]

const APPLICATIONS = [
  'enterprise systems',
  'distributed applications',
  'web applications',
  'mobile applications',
  'cloud services',
  'IoT devices',
  'embedded systems',
  'healthcare systems',
  'financial services',
  'government services',
]

const ASPECTS = [
  'functional',
  'performance',
  'security',
  'reliability',
  'maintainability',
  'portability',
  'usability',
  'accessibility',
  'compatibility',
  'scalability',
]

// =============================================================================
// Content Generation Helpers
// =============================================================================

function generateStandardId(org: typeof ORGANIZATIONS[number], rng: () => number): string {
  const number = Math.floor(rng() * 99999) + 1
  const part = rng() > 0.7 ? `-${Math.floor(rng() * 10) + 1}` : ''
  return `${org.prefix} ${number}${part}`
}

function generateTitle(rng: () => number): string {
  const template = pick(TITLE_TEMPLATES, rng)
  return template
    .replace('{domain}', pick(DOMAINS, rng))
    .replace('{topic}', pick(TOPICS, rng))
    .replace('{subtitle}', pick(SUBTITLES, rng))
    .replace('{application}', pick(APPLICATIONS, rng))
    .replace('{aspect}', pick(ASPECTS, rng))
    .replace('{part}', String(Math.floor(rng() * 10) + 1))
}

function generateVersion(rng: () => number): string {
  const major = Math.floor(rng() * 5) + 1
  const minor = Math.floor(rng() * 10)
  const hasRevision = rng() > 0.5
  return hasRevision ? `${major}.${minor}.${Math.floor(rng() * 5)}` : `${major}.${minor}`
}

function generateAbstract(title: string, category: string, rng: () => number): string {
  const templates = [
    `This standard specifies requirements for ${title.toLowerCase()}. It provides a framework for organizations to implement and assess conformance to ${category.replace(/-/g, ' ')} practices.`,
    `This document defines the ${category.replace(/-/g, ' ')} aspects of ${title.toLowerCase()}. It establishes requirements, guidelines, and best practices for implementation.`,
    `This specification provides normative requirements and informative guidance for ${title.toLowerCase()}. It is intended for use by organizations developing, implementing, or assessing ${category.replace(/-/g, ' ')} solutions.`,
    `This international standard establishes a common vocabulary and conceptual framework for ${title.toLowerCase()}. It defines key terms, concepts, and relationships relevant to ${category.replace(/-/g, ' ')}.`,
  ]
  return pick(templates, rng)
}

function generateSectionTitle(type: typeof SECTION_TYPES[number], index: number, rng: () => number): string {
  const titleMap: Record<string, string[]> = {
    'scope': ['Scope', 'Scope and field of application', 'Scope of this document'],
    'normative-references': ['Normative references', 'Referenced documents', 'Normative reference documents'],
    'terms-and-definitions': ['Terms and definitions', 'Terminology', 'Terms, definitions and abbreviations'],
    'symbols-and-abbreviations': ['Symbols and abbreviated terms', 'Abbreviations', 'Symbols'],
    'requirements': ['Requirements', 'Functional requirements', 'Normative requirements', 'Mandatory requirements'],
    'conformance': ['Conformance', 'Conformance requirements', 'Conformance clauses'],
    'architecture': ['Architecture', 'Reference architecture', 'System architecture', 'Architectural overview'],
    'data-model': ['Data model', 'Information model', 'Conceptual model', 'Data structures'],
    'protocol': ['Protocol', 'Protocol specification', 'Communication protocol', 'Message exchange'],
    'security-considerations': ['Security considerations', 'Security requirements', 'Security aspects'],
    'implementation-guidelines': ['Implementation guidelines', 'Implementation notes', 'Implementation considerations'],
    'examples': ['Examples', 'Use cases', 'Illustrative examples', 'Sample implementations'],
    'annexes': ['Annexes', 'Appendices', 'Supplementary information'],
    'bibliography': ['Bibliography', 'References', 'Further reading'],
  }
  return pick(titleMap[type] || [type], rng)
}

function generateSectionContent(type: typeof SECTION_TYPES[number], title: string, rng: () => number): string {
  const paragraphs: string[] = []
  const numParagraphs = Math.floor(rng() * 3) + 2

  for (let i = 0; i < numParagraphs; i++) {
    const templates = [
      `This section specifies the ${type.replace(/-/g, ' ')} for conforming implementations. Organizations implementing this standard shall comply with all normative requirements specified herein.`,
      `The ${type.replace(/-/g, ' ')} defined in this clause establish the baseline for conformance. Implementations may provide additional features beyond those specified.`,
      `For the purposes of this document, the following ${type.replace(/-/g, ' ')} apply. These definitions are consistent with established terminology in the field.`,
      `This clause provides ${type.replace(/-/g, ' ')} that implementers should consider. While not normative, these guidelines represent best practices.`,
      `The ${type.replace(/-/g, ' ')} specified here are derived from analysis of existing implementations and stakeholder requirements.`,
    ]
    paragraphs.push(pick(templates, rng))
  }

  return paragraphs.join('\n\n')
}

// =============================================================================
// Data Generators
// =============================================================================

interface Standard {
  id: string
  standard_id: string
  organization: string
  organization_name: string
  organization_type: string
  title: string
  abstract: string
  version: string
  status: string
  publication_date: string
  effective_date: string | null
  withdrawal_date: string | null
  category: string
  keywords: string[]
  page_count: number
  language: string
  is_free: boolean
  url: string | null
  created_at: string
  updated_at: string
}

interface Section {
  id: string
  standard_id: string
  section_number: string
  section_type: string
  title: string
  content: string
  word_count: number
  has_subsections: boolean
  parent_section_id: string | null
  order_index: number
  created_at: string
}

interface Reference {
  id: string
  source_standard_id: string
  target_standard_id: string
  reference_type: string
  clause_reference: string | null
  is_dated: boolean
  reference_date: string | null
  note: string | null
  created_at: string
}

interface StandardsData {
  standards: string
  sections: string
  references: string
}

function generateStandardsData(size: SizeOption): StandardsData {
  const config = SIZE_CONFIGS[size]
  const rng = createRng(STANDARDS_SEED)

  // Generate standards
  const standards: string[] = []
  const standardRecords: Standard[] = []

  for (let i = 0; i < config.standards; i++) {
    const org = pick(ORGANIZATIONS, rng)
    const standardId = generateStandardId(org, rng)
    const title = generateTitle(rng)
    const category = pick(CATEGORIES, rng)
    const status = pick(STATUSES, rng)
    const pubDate = generateDate(rng, 2000, 2024)
    const id = generateUuid(rng)

    const record: Standard = {
      id,
      standard_id: standardId,
      organization: org.prefix,
      organization_name: org.name,
      organization_type: org.type,
      title,
      abstract: generateAbstract(title, category, rng),
      version: generateVersion(rng),
      status,
      publication_date: pubDate,
      effective_date: status === 'active' ? generateDate(rng, parseInt(pubDate.slice(0, 4)), 2024) : null,
      withdrawal_date: status === 'withdrawn' || status === 'superseded' ? generateDate(rng, parseInt(pubDate.slice(0, 4)) + 1, 2024) : null,
      category,
      keywords: pickN(KEYWORDS, Math.floor(rng() * 5) + 3, rng),
      page_count: Math.floor(rng() * 200) + 10,
      language: rng() > 0.1 ? 'en' : pick(['fr', 'de', 'es', 'zh', 'ja'], rng),
      is_free: rng() > 0.7,
      url: rng() > 0.3 ? `https://standards.example.org/${org.prefix.toLowerCase()}/${id}` : null,
      created_at: pubDate + 'T00:00:00Z',
      updated_at: generateDate(rng, parseInt(pubDate.slice(0, 4)), 2024) + 'T00:00:00Z',
    }

    standardRecords.push(record)
    standards.push(JSON.stringify(record))
  }

  // Generate sections for each standard
  const sections: string[] = []
  for (const standard of standardRecords) {
    const numSections = Math.floor(rng() * config.sectionsPerStandard) + 5
    let parentSectionId: string | null = null

    for (let j = 0; j < numSections; j++) {
      const sectionType = pick(SECTION_TYPES, rng)
      const isSubsection = j > 0 && rng() > 0.7 && parentSectionId !== null
      const sectionNumber = isSubsection
        ? `${j}.${Math.floor(rng() * 5) + 1}`
        : `${j + 1}`
      const title = generateSectionTitle(sectionType, j, rng)
      const content = generateSectionContent(sectionType, title, rng)
      const id = generateUuid(rng)

      if (!isSubsection) {
        parentSectionId = id
      }

      const section: Section = {
        id,
        standard_id: standard.id,
        section_number: sectionNumber,
        section_type: sectionType,
        title,
        content,
        word_count: content.split(/\s+/).length,
        has_subsections: rng() > 0.6,
        parent_section_id: isSubsection ? parentSectionId : null,
        order_index: j,
        created_at: standard.created_at,
      }

      sections.push(JSON.stringify(section))
    }
  }

  // Generate cross-references between standards
  const references: string[] = []
  const seenRefs = new Set<string>()

  for (const standard of standardRecords) {
    const numRefs = Math.floor(rng() * config.referencesPerStandard) + 1

    for (let j = 0; j < numRefs; j++) {
      // Pick a random target standard (different from source)
      let targetStandard: Standard
      let attempts = 0
      do {
        targetStandard = pick(standardRecords, rng)
        attempts++
      } while (targetStandard.id === standard.id && attempts < 10)

      if (targetStandard.id === standard.id) continue

      // Avoid duplicate references
      const refKey = `${standard.id}:${targetStandard.id}`
      if (seenRefs.has(refKey)) continue
      seenRefs.add(refKey)

      const refType = pick(REFERENCE_TYPES, rng)
      const isDated = rng() > 0.5

      const reference: Reference = {
        id: generateUuid(rng),
        source_standard_id: standard.id,
        target_standard_id: targetStandard.id,
        reference_type: refType,
        clause_reference: rng() > 0.5 ? `Clause ${Math.floor(rng() * 10) + 1}` : null,
        is_dated: isDated,
        reference_date: isDated ? targetStandard.publication_date : null,
        note: rng() > 0.7 ? `See ${targetStandard.standard_id} for additional requirements.` : null,
        created_at: standard.created_at,
      }

      references.push(JSON.stringify(reference))
    }
  }

  return {
    standards: standards.join('\n'),
    sections: sections.join('\n'),
    references: references.join('\n'),
  }
}

// =============================================================================
// Response Types
// =============================================================================

interface StageResult {
  success: boolean
  size: SizeOption
  duration: number
  files: Array<{
    name: string
    size: number
    key: string
    recordCount: number
  }>
  totalSize: number
  cached?: boolean
  error?: string
}

// =============================================================================
// Hono Application
// =============================================================================

const app = new Hono<{ Bindings: Env }>()

// CORS middleware
app.use('*', async (c, next) => {
  await next()
  c.header('Access-Control-Allow-Origin', '*')
  c.header('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS')
  c.header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
})

// Handle OPTIONS requests
app.options('*', (c) => {
  return new Response(null, { status: 204 })
})

// Health check
app.get('/health', (c) => {
  return c.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    service: 'stage-standards',
  })
})

// API documentation
app.get('/', (c) => {
  return c.json({
    service: 'Standards Dataset Staging Worker',
    description: 'Generates synthetic technical standards data for benchmarking document search and retrieval',
    endpoints: {
      'POST /stage/standards/:size': {
        description: 'Generate and stage standards dataset',
        params: { size: 'Dataset size: 1mb, 10mb, 100mb, or 1gb' },
        example: 'POST /stage/standards/10mb',
      },
      'GET /status/:size': {
        description: 'Check if dataset is already staged',
        params: { size: 'Dataset size to check' },
      },
      'DELETE /stage/standards/:size': {
        description: 'Delete a staged dataset',
        params: { size: 'Dataset size to delete' },
      },
      'GET /datasets': {
        description: 'List available dataset configurations',
      },
      'GET /health': {
        description: 'Health check endpoint',
      },
    },
    dataModel: {
      standards: 'Technical standards with IDs, titles, versions, publication dates, categories',
      sections: 'Document sections and clauses within each standard',
      references: 'Cross-references between standards (normative, informative, supersedes, etc.)',
    },
    sizes: VALID_SIZES.map((s) => ({
      size: s,
      standards: SIZE_CONFIGS[s].standards,
      estimatedSections: SIZE_CONFIGS[s].standards * SIZE_CONFIGS[s].sectionsPerStandard,
      estimatedReferences: SIZE_CONFIGS[s].standards * SIZE_CONFIGS[s].referencesPerStandard,
    })),
  })
})

// List available datasets
app.get('/datasets', (c) => {
  return c.json({
    datasets: VALID_SIZES.map((size) => ({
      size,
      config: SIZE_CONFIGS[size],
      files: ['standards.jsonl', 'sections.jsonl', 'references.jsonl'],
    })),
    organizations: ORGANIZATIONS.map((o) => ({
      prefix: o.prefix,
      name: o.name,
      type: o.type,
    })),
    categories: CATEGORIES,
  })
})

// Stage a dataset
app.post('/stage/standards/:size', async (c) => {
  const size = c.req.param('size') as SizeOption
  const startTime = Date.now()

  // Validate size
  if (!VALID_SIZES.includes(size)) {
    return c.json(
      {
        success: false,
        error: `Invalid size: ${size}. Valid options: ${VALID_SIZES.join(', ')}`,
      },
      400
    )
  }

  // Check if dataset already exists
  const existingPrefix = `standards/${size}/`
  const existingFiles = await c.env.STANDARDS_BUCKET.list({ prefix: existingPrefix })

  if (existingFiles.objects.length >= 3) {
    // Return existing dataset info
    const files = existingFiles.objects.map((obj) => ({
      name: obj.key.split('/').pop()!,
      size: obj.size,
      key: obj.key,
      recordCount: parseInt(obj.customMetadata?.recordCount || '0', 10),
    }))
    const totalSize = files.reduce((sum, f) => sum + f.size, 0)

    return c.json({
      success: true,
      cached: true,
      size,
      duration: Date.now() - startTime,
      files,
      totalSize,
    })
  }

  try {
    // Generate dataset in-worker
    console.log(`Generating standards dataset (${size}) in-worker...`)
    const data = generateStandardsData(size)
    const generationTime = Date.now() - startTime
    console.log(`Generation complete in ${generationTime}ms`)

    // Upload generated files to R2
    const uploadedFiles: StageResult['files'] = []
    const tables = [
      { name: 'standards', content: data.standards },
      { name: 'sections', content: data.sections },
      { name: 'references', content: data.references },
    ]

    for (const table of tables) {
      const filename = `${table.name}.jsonl`
      const content = table.content
      const recordCount = content.split('\n').filter((line) => line.trim()).length

      console.log(`Uploading ${filename} (${recordCount} records)...`)

      // Upload to R2
      const r2Key = `standards/${size}/${filename}`
      await c.env.STANDARDS_BUCKET.put(r2Key, content, {
        httpMetadata: {
          contentType: 'application/x-ndjson',
        },
        customMetadata: {
          size,
          table: table.name,
          recordCount: String(recordCount),
          generatedAt: new Date().toISOString(),
          seed: String(STANDARDS_SEED),
        },
      })

      uploadedFiles.push({
        name: filename,
        size: new Blob([content]).size,
        key: r2Key,
        recordCount,
      })
    }

    const totalSize = uploadedFiles.reduce((sum, f) => sum + f.size, 0)

    const response: StageResult = {
      success: true,
      size,
      duration: Date.now() - startTime,
      files: uploadedFiles,
      totalSize,
    }

    return c.json(response)
  } catch (error) {
    console.error(`Error staging standards/${size}:`, error)
    return c.json(
      {
        success: false,
        size,
        duration: Date.now() - startTime,
        files: [],
        totalSize: 0,
        error: error instanceof Error ? error.message : String(error),
      } as StageResult,
      500
    )
  }
})

// Check dataset status
app.get('/status/:size', async (c) => {
  const size = c.req.param('size') as SizeOption

  // Validate size
  if (!VALID_SIZES.includes(size)) {
    return c.json({ error: `Invalid size: ${size}` }, 400)
  }

  const prefix = `standards/${size}/`
  const files = await c.env.STANDARDS_BUCKET.list({ prefix })

  if (files.objects.length === 0) {
    return c.json({
      exists: false,
      size,
      files: [],
    })
  }

  const fileList = files.objects.map((obj) => ({
    name: obj.key.split('/').pop(),
    size: obj.size,
    key: obj.key,
    uploaded: obj.uploaded,
    recordCount: obj.customMetadata?.recordCount
      ? parseInt(obj.customMetadata.recordCount, 10)
      : undefined,
  }))

  const totalSize = fileList.reduce((sum, f) => sum + f.size, 0)

  return c.json({
    exists: true,
    complete: files.objects.length >= 3,
    size,
    files: fileList,
    totalSize,
  })
})

// Delete a staged dataset
app.delete('/stage/standards/:size', async (c) => {
  const size = c.req.param('size') as SizeOption

  // Validate size
  if (!VALID_SIZES.includes(size)) {
    return c.json({ error: `Invalid size: ${size}` }, 400)
  }

  const prefix = `standards/${size}/`
  const files = await c.env.STANDARDS_BUCKET.list({ prefix })

  let deleted = 0
  for (const obj of files.objects) {
    await c.env.STANDARDS_BUCKET.delete(obj.key)
    deleted++
  }

  return c.json({
    success: true,
    size,
    deleted,
  })
})

// Stage all sizes
app.post('/stage-all', async (c) => {
  const results: StageResult[] = []

  for (const size of VALID_SIZES) {
    // Make internal request to stage endpoint
    const response = await app.fetch(
      new Request(`http://localhost/stage/standards/${size}`, {
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
      total: VALID_SIZES.length,
      totalSize,
    },
    results,
  })
})

// Legacy endpoint support for backwards compatibility
app.post('/stage/standards', async (c) => {
  // Default to 10mb for legacy endpoint
  const url = new URL(c.req.url)
  const size = (url.searchParams.get('size') || '10mb') as SizeOption

  // Forward to new endpoint
  const response = await app.fetch(
    new Request(`http://localhost/stage/standards/${size}`, {
      method: 'POST',
    }),
    c.env
  )

  return response
})

// List staged files (legacy endpoint)
app.get('/stage/standards/list', async (c) => {
  const allFiles: Array<{
    key: string
    size: number
    uploaded: Date
    sizeCategory: string
    recordCount?: number
  }> = []

  for (const size of VALID_SIZES) {
    const prefix = `standards/${size}/`
    const files = await c.env.STANDARDS_BUCKET.list({ prefix })

    for (const obj of files.objects) {
      allFiles.push({
        key: obj.key,
        size: obj.size,
        uploaded: obj.uploaded,
        sizeCategory: size,
        recordCount: obj.customMetadata?.recordCount
          ? parseInt(obj.customMetadata.recordCount, 10)
          : undefined,
      })
    }
  }

  const totalSize = allFiles.reduce((sum, f) => sum + f.size, 0)

  return c.json({
    files: allFiles,
    totalFiles: allFiles.length,
    totalSizeBytes: totalSize,
  })
})

// Status endpoint (legacy)
app.get('/stage/standards/status', async (c) => {
  const statuses: Record<string, { exists: boolean; files: number; totalSize: number }> = {}

  for (const size of VALID_SIZES) {
    const prefix = `standards/${size}/`
    const files = await c.env.STANDARDS_BUCKET.list({ prefix })
    const totalSize = files.objects.reduce((sum, f) => sum + f.size, 0)

    statuses[size] = {
      exists: files.objects.length > 0,
      files: files.objects.length,
      totalSize,
    }
  }

  return c.json({
    status: 'ready',
    sizes: statuses,
  })
})

export default app
