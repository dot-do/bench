/**
 * Stage OLTP Datasets Worker
 *
 * Generates realistic OLTP datasets using Faker and streams them to R2.
 * Uses Durable Objects for chunked generation to handle CPU limits.
 * Uses deterministic seeding for reproducible data.
 *
 * GET /                          - API documentation
 * GET /stage/:dataset/:size      - Generate and stage dataset
 * GET /status/:dataset/:size     - Check if dataset exists
 */

import { Hono } from 'hono'
import { faker } from '@faker-js/faker'
import { DurableObject } from 'cloudflare:workers'

interface Env {
  DATASETS: R2Bucket
  DATA_GENERATOR: DurableObjectNamespace<DataGeneratorDO>
}

type DatasetType = 'ecommerce' | 'saas' | 'social'
type SizeOption = '1mb' | '10mb' | '100mb' | '1gb'

const SIZES: Record<SizeOption, number> = {
  '1mb': 1_000,
  '10mb': 10_000,
  '100mb': 100_000,
  '1gb': 1_000_000,
}

// Chunk size for DO generation - balance between CPU usage and RPC overhead
const CHUNK_SIZE = 10_000

// Table configuration for each dataset
type TableConfig = {
  tables: string[]
  multipliers: Record<string, number>
}

const DATASET_CONFIGS: Record<DatasetType, TableConfig> = {
  ecommerce: {
    tables: ['customers', 'products', 'orders', 'reviews'],
    multipliers: { customers: 0.2, products: 0.5, orders: 1, reviews: 0.8 },
  },
  saas: {
    tables: ['orgs', 'users', 'documents'],
    multipliers: { orgs: 0.01, users: 0.1, documents: 1 },
  },
  social: {
    tables: ['users', 'posts', 'reviews'],
    multipliers: { users: 0.1, posts: 1, reviews: 0.5 },
  },
}

// =============================================================================
// Record Generators - each generates a single record as JSON string
// =============================================================================

function generateCustomer(): string {
  return JSON.stringify({
    id: faker.string.uuid(),
    email: faker.internet.email(),
    first_name: faker.person.firstName(),
    last_name: faker.person.lastName(),
    phone: faker.phone.number(),
    company: faker.company.name(),
    tier: faker.helpers.arrayElement(['free', 'pro', 'enterprise']),
    total_spent: parseFloat(faker.commerce.price({ min: 0, max: 50000 })),
    address: {
      street: faker.location.streetAddress(),
      city: faker.location.city(),
      state: faker.location.state({ abbreviated: true }),
      postal_code: faker.location.zipCode(),
      country: faker.location.countryCode(),
    },
    created_at: faker.date.past({ years: 3 }).toISOString(),
    updated_at: faker.date.recent({ days: 90 }).toISOString(),
  })
}

function generateProduct(): string {
  const price = parseFloat(faker.commerce.price({ min: 1, max: 1000 }))
  return JSON.stringify({
    id: faker.string.uuid(),
    sku: `SKU-${faker.string.alphanumeric(8).toUpperCase()}`,
    name: faker.commerce.productName(),
    description: faker.commerce.productDescription(),
    category: faker.commerce.department(),
    brand: faker.company.name(),
    price,
    cost: parseFloat((price * faker.number.float({ min: 0.3, max: 0.7 })).toFixed(2)),
    stock: faker.number.int({ min: 0, max: 1000 }),
    is_active: faker.datatype.boolean({ probability: 0.9 }),
    rating: faker.number.float({ min: 1, max: 5, fractionDigits: 1 }),
    review_count: faker.number.int({ min: 0, max: 500 }),
    created_at: faker.date.past({ years: 2 }).toISOString(),
    updated_at: faker.date.recent({ days: 30 }).toISOString(),
  })
}

function generateOrder(): string {
  const itemCount = faker.number.int({ min: 1, max: 5 })
  const items = Array.from({ length: itemCount }, () => ({
    product_id: faker.string.uuid(),
    quantity: faker.number.int({ min: 1, max: 5 }),
    unit_price: parseFloat(faker.commerce.price({ min: 10, max: 500 })),
  }))
  const subtotal = items.reduce((sum, item) => sum + item.quantity * item.unit_price, 0)
  const discount = parseFloat((subtotal * faker.number.float({ min: 0, max: 0.2 })).toFixed(2))
  const tax = parseFloat((subtotal * 0.08).toFixed(2))
  const shipping = parseFloat(faker.commerce.price({ min: 0, max: 25 }))

  return JSON.stringify({
    id: faker.string.uuid(),
    customer_id: faker.string.uuid(),
    items,
    subtotal: parseFloat(subtotal.toFixed(2)),
    discount,
    tax,
    shipping,
    total: parseFloat((subtotal - discount + tax + shipping).toFixed(2)),
    status: faker.helpers.arrayElement(['pending', 'processing', 'shipped', 'delivered', 'cancelled']),
    payment_status: faker.helpers.arrayElement(['pending', 'paid', 'refunded', 'failed']),
    created_at: faker.date.recent({ days: 365 }).toISOString(),
    updated_at: faker.date.recent({ days: 30 }).toISOString(),
  })
}

function generateReview(): string {
  const rating = faker.number.int({ min: 1, max: 5 })
  return JSON.stringify({
    id: faker.string.uuid(),
    product_id: faker.string.uuid(),
    customer_id: faker.string.uuid(),
    rating,
    title: faker.lorem.sentence({ min: 3, max: 8 }),
    body: faker.lorem.paragraph(),
    helpful_votes: faker.number.int({ min: 0, max: 100 }),
    verified: faker.datatype.boolean({ probability: 0.7 }),
    created_at: faker.date.recent({ days: 180 }).toISOString(),
  })
}

function generateUser(): string {
  return JSON.stringify({
    id: faker.string.uuid(),
    username: faker.internet.username(),
    email: faker.internet.email(),
    display_name: faker.person.fullName(),
    bio: faker.lorem.sentence(),
    avatar_url: faker.image.avatar(),
    follower_count: faker.number.int({ min: 0, max: 100000 }),
    following_count: faker.number.int({ min: 0, max: 5000 }),
    is_verified: faker.datatype.boolean({ probability: 0.1 }),
    created_at: faker.date.past({ years: 5 }).toISOString(),
  })
}

function generatePost(): string {
  return JSON.stringify({
    id: faker.string.uuid(),
    user_id: faker.string.uuid(),
    content: faker.lorem.paragraphs({ min: 1, max: 3 }),
    media_urls: faker.datatype.boolean({ probability: 0.3 })
      ? [faker.image.url()]
      : [],
    like_count: faker.number.int({ min: 0, max: 10000 }),
    comment_count: faker.number.int({ min: 0, max: 500 }),
    share_count: faker.number.int({ min: 0, max: 1000 }),
    is_pinned: faker.datatype.boolean({ probability: 0.05 }),
    created_at: faker.date.recent({ days: 365 }).toISOString(),
  })
}

function generateOrg(): string {
  return JSON.stringify({
    id: faker.string.uuid(),
    name: faker.company.name(),
    slug: faker.helpers.slugify(faker.company.name()).toLowerCase(),
    plan: faker.helpers.arrayElement(['free', 'starter', 'pro', 'enterprise']),
    seats: faker.number.int({ min: 1, max: 500 }),
    mrr: faker.number.int({ min: 0, max: 50000 }),
    industry: faker.company.buzzNoun(),
    created_at: faker.date.past({ years: 3 }).toISOString(),
  })
}

function generateDocument(): string {
  return JSON.stringify({
    id: faker.string.uuid(),
    org_id: faker.string.uuid(),
    user_id: faker.string.uuid(),
    title: faker.lorem.sentence({ min: 3, max: 10 }),
    content: faker.lorem.paragraphs({ min: 2, max: 10 }),
    type: faker.helpers.arrayElement(['document', 'spreadsheet', 'presentation', 'note']),
    is_public: faker.datatype.boolean({ probability: 0.2 }),
    word_count: faker.number.int({ min: 100, max: 10000 }),
    created_at: faker.date.past({ years: 2 }).toISOString(),
    updated_at: faker.date.recent({ days: 60 }).toISOString(),
  })
}

// Map table names to generator functions
const TABLE_GENERATORS: Record<string, () => string> = {
  customers: generateCustomer,
  products: generateProduct,
  orders: generateOrder,
  reviews: generateReview,
  users: generateUser,
  posts: generatePost,
  orgs: generateOrg,
  documents: generateDocument,
}

// =============================================================================
// Durable Object for Chunked Data Generation
// =============================================================================

export class DataGeneratorDO extends DurableObject {
  /**
   * Generate a chunk of records for a specific table
   * Each call gets fresh CPU time, allowing large datasets to be generated
   * across multiple RPC calls.
   */
  async generateChunk(
    table: string,
    offset: number,
    count: number
  ): Promise<string> {
    // Seed faker for reproducibility: base seed + offset ensures
    // the same offset always produces the same records
    faker.seed(42 + offset)

    const generator = TABLE_GENERATORS[table]
    if (!generator) {
      throw new Error(`Unknown table: ${table}`)
    }

    const lines: string[] = []
    for (let i = 0; i < count; i++) {
      lines.push(generator())
    }

    return lines.join('\n') + '\n'
  }
}

// =============================================================================
// Legacy generators for small datasets (non-DO path)
// =============================================================================

function* generateRecords(table: string, count: number): Generator<string> {
  const generator = TABLE_GENERATORS[table]
  if (!generator) {
    throw new Error(`Unknown table: ${table}`)
  }
  for (let i = 0; i < count; i++) {
    yield generator()
  }
}

// Collect generator output into a string (for smaller datasets)
function collectGenerator(gen: Generator<string>): string {
  const lines: string[] = []
  for (const line of gen) {
    lines.push(line)
  }
  return lines.join('\n') + '\n'
}

// =============================================================================
// R2 Multipart Upload Helpers
// =============================================================================

/**
 * Stream chunks from DO to R2 using multipart upload
 * Each DO call generates CHUNK_SIZE records, and we buffer until we have
 * enough data to upload a part.
 *
 * R2 requires all non-trailing parts to have the same BYTE size, so we use
 * a Uint8Array buffer and accumulate exactly PART_SIZE bytes before uploading.
 */
async function streamChunksToR2(
  bucket: R2Bucket,
  doStub: DurableObjectStub<DataGeneratorDO>,
  key: string,
  table: string,
  totalRecords: number,
  metadata: { dataset: string; size: string; table: string; records: string }
): Promise<{ size: number; chunks: number }> {
  const upload = await bucket.createMultipartUpload(key, {
    httpMetadata: { contentType: 'application/x-ndjson' },
    customMetadata: metadata,
  })

  const parts: R2UploadedPart[] = []
  const encoder = new TextEncoder()

  // Use a byte buffer to ensure consistent part sizes
  let byteBuffer = new Uint8Array(0)
  let totalSize = 0
  let chunksGenerated = 0
  // R2 minimum part size is 5MB, we'll use exactly 5MB for consistency
  const PART_SIZE = 5 * 1024 * 1024

  try {
    // Generate records in chunks via DO RPC calls
    for (let offset = 0; offset < totalRecords; offset += CHUNK_SIZE) {
      const count = Math.min(CHUNK_SIZE, totalRecords - offset)

      // Each RPC call gets fresh CPU time
      const chunk = await doStub.generateChunk(table, offset, count)
      const chunkBytes = encoder.encode(chunk)
      chunksGenerated++

      // Append to byte buffer
      const newBuffer = new Uint8Array(byteBuffer.length + chunkBytes.length)
      newBuffer.set(byteBuffer)
      newBuffer.set(chunkBytes, byteBuffer.length)
      byteBuffer = newBuffer

      console.log(`Generated chunk ${chunksGenerated}: ${table} offset=${offset} count=${count} bufferBytes=${byteBuffer.length}`)

      // Upload parts of exactly PART_SIZE bytes (except final part)
      while (byteBuffer.length >= PART_SIZE) {
        const partNumber = parts.length + 1
        const partData = byteBuffer.slice(0, PART_SIZE)
        byteBuffer = byteBuffer.slice(PART_SIZE)

        const part = await upload.uploadPart(partNumber, partData)
        parts.push(part)
        totalSize += partData.length
        console.log(`Uploaded part ${partNumber}: ${partData.length} bytes`)
      }
    }

    // Upload remaining buffer as final part
    if (byteBuffer.length > 0) {
      const partNumber = parts.length + 1
      const part = await upload.uploadPart(partNumber, byteBuffer)
      parts.push(part)
      totalSize += byteBuffer.length
      console.log(`Uploaded final part ${partNumber}: ${byteBuffer.length} bytes`)
    }

    await upload.complete(parts)
    return { size: totalSize, chunks: chunksGenerated }
  } catch (error) {
    console.error(`Error during multipart upload: ${error}`)
    await upload.abort()
    throw error
  }
}

// =============================================================================
// Hono App
// =============================================================================

const app = new Hono<{ Bindings: Env }>()

app.get('/', (c) => c.json({
  service: 'stage-oltp',
  endpoints: {
    'GET /stage/:dataset/:size': 'Generate and stage dataset (e.g., /stage/ecommerce/100mb)',
    'GET /status/:dataset/:size': 'Check if dataset exists',
  },
  datasets: Object.keys(DATASET_CONFIGS),
  sizes: Object.keys(SIZES),
  architecture: 'Uses Durable Objects for chunked generation to handle CPU limits on large datasets',
}))

app.get('/health', (c) => c.json({ status: 'ok', service: 'stage-oltp' }))

app.get('/datasets', (c) => c.json({
  datasets: Object.entries(DATASET_CONFIGS).map(([id, config]) => ({
    id,
    tables: config.tables,
    sizes: Object.keys(SIZES),
  })),
}))

// Main staging endpoint
app.get('/stage/:dataset/:size', async (c) => {
  const dataset = c.req.param('dataset') as DatasetType
  const size = c.req.param('size') as SizeOption

  if (!DATASET_CONFIGS[dataset]) {
    return c.json({ error: `Invalid dataset: ${dataset}` }, 400)
  }
  if (!SIZES[size]) {
    return c.json({ error: `Invalid size: ${size}` }, 400)
  }

  // Check cache
  const prefix = `oltp/${dataset}/${size}/`
  const existing = await c.env.DATASETS.list({ prefix })
  if (existing.objects.length > 0) {
    return c.json({
      success: true,
      cached: true,
      dataset,
      size,
      files: existing.objects.map((o) => ({ key: o.key, size: o.size })),
    })
  }

  const config = DATASET_CONFIGS[dataset]
  const baseCount = SIZES[size]
  const files: { key: string; size: number; records: number; chunks?: number }[] = []
  const start = Date.now()

  // Use DO for large datasets (100mb, 1gb)
  const useDO = size === '100mb' || size === '1gb'

  // Get DO stub if needed
  const doId = c.env.DATA_GENERATOR.idFromName('generator')
  const doStub = c.env.DATA_GENERATOR.get(doId)

  try {
    for (const table of config.tables) {
      const count = Math.floor(baseCount * (config.multipliers[table] ?? 1))
      const key = `${prefix}${table}.jsonl`
      const metadata = { dataset, size, table, records: String(count) }

      console.log(`${useDO ? 'Streaming via DO' : 'Generating'} ${table} (${count} records)...`)

      if (useDO) {
        // Use Durable Object for chunked generation
        const result = await streamChunksToR2(
          c.env.DATASETS,
          doStub,
          key,
          table,
          count,
          metadata
        )
        files.push({ key, size: result.size, records: count, chunks: result.chunks })
      } else {
        // Direct generation for small datasets
        faker.seed(42 + table.charCodeAt(0))
        const content = collectGenerator(generateRecords(table, count))
        await c.env.DATASETS.put(key, content, {
          httpMetadata: { contentType: 'application/x-ndjson' },
          customMetadata: metadata,
        })
        files.push({ key, size: new Blob([content]).size, records: count })
      }
    }

    const duration = Date.now() - start
    const totalSize = files.reduce((sum, f) => sum + f.size, 0)

    return c.json({
      success: true,
      dataset,
      size,
      duration,
      totalSize,
      totalSizeMB: (totalSize / (1024 * 1024)).toFixed(2),
      usedDurableObjects: useDO,
      files,
    })
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error)
    const errorStack = error instanceof Error ? error.stack : undefined
    console.error(`Staging error: ${errorMessage}`, errorStack)
    return c.json({
      success: false,
      error: errorMessage,
      stack: errorStack,
      dataset,
      size,
      filesCompleted: files,
    }, 500)
  }
})

// Keep POST for backward compatibility
app.post('/stage/:dataset/:size', async (c) => {
  const dataset = c.req.param('dataset')
  const size = c.req.param('size')
  return c.redirect(`/stage/${dataset}/${size}`, 307)
})

app.get('/status/:dataset/:size', async (c) => {
  const dataset = c.req.param('dataset') as DatasetType
  const size = c.req.param('size') as SizeOption

  const prefix = `oltp/${dataset}/${size}/`
  const existing = await c.env.DATASETS.list({ prefix })

  return c.json({
    exists: existing.objects.length > 0,
    dataset,
    size,
    files: existing.objects.map((o) => ({ key: o.key, size: o.size })),
  })
})

// Delete endpoint to clear cached data
app.delete('/stage/:dataset/:size', async (c) => {
  const dataset = c.req.param('dataset') as DatasetType
  const size = c.req.param('size') as SizeOption

  const prefix = `oltp/${dataset}/${size}/`
  const existing = await c.env.DATASETS.list({ prefix })

  if (existing.objects.length === 0) {
    return c.json({ success: true, deleted: 0 })
  }

  const deleted = await Promise.all(
    existing.objects.map((o) => c.env.DATASETS.delete(o.key))
  )

  return c.json({
    success: true,
    deleted: deleted.length,
    keys: existing.objects.map((o) => o.key),
  })
})

export default app
