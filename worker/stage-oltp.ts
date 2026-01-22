/**
 * Stage OLTP Datasets Worker
 *
 * Generates realistic OLTP datasets using Faker and streams them to R2.
 * Uses deterministic seeding for reproducible data.
 *
 * GET /                          - API documentation
 * GET /stage/:dataset/:size      - Generate and stage dataset
 * GET /status/:dataset/:size     - Check if dataset exists
 */

import { Hono } from 'hono'
import { faker } from '@faker-js/faker'

interface Env {
  DATASETS: R2Bucket
}

type DatasetType = 'ecommerce' | 'saas' | 'social'
type SizeOption = '1mb' | '10mb' | '100mb' | '1gb'

const SIZES: Record<SizeOption, number> = {
  '1mb': 1_000,
  '10mb': 10_000,
  '100mb': 100_000,
  '1gb': 1_000_000,
}

// Record generators - each yields JSONL lines
function* generateCustomers(count: number): Generator<string> {
  for (let i = 0; i < count; i++) {
    yield JSON.stringify({
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
}

function* generateProducts(count: number): Generator<string> {
  for (let i = 0; i < count; i++) {
    const price = parseFloat(faker.commerce.price({ min: 1, max: 1000 }))
    yield JSON.stringify({
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
}

function* generateOrders(count: number): Generator<string> {
  for (let i = 0; i < count; i++) {
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

    yield JSON.stringify({
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
}

function* generateReviews(count: number): Generator<string> {
  for (let i = 0; i < count; i++) {
    const rating = faker.number.int({ min: 1, max: 5 })
    yield JSON.stringify({
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
}

function* generateUsers(count: number): Generator<string> {
  for (let i = 0; i < count; i++) {
    yield JSON.stringify({
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
}

function* generatePosts(count: number): Generator<string> {
  for (let i = 0; i < count; i++) {
    yield JSON.stringify({
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
}

function* generateOrgs(count: number): Generator<string> {
  for (let i = 0; i < count; i++) {
    yield JSON.stringify({
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
}

function* generateDocuments(count: number): Generator<string> {
  for (let i = 0; i < count; i++) {
    yield JSON.stringify({
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
}

const DATASETS: Record<DatasetType, { tables: Record<string, (count: number) => Generator<string>>; multipliers: Record<string, number> }> = {
  ecommerce: {
    tables: { customers: generateCustomers, products: generateProducts, orders: generateOrders, reviews: generateReviews },
    multipliers: { customers: 0.2, products: 0.5, orders: 1, reviews: 0.8 },
  },
  saas: {
    tables: { orgs: generateOrgs, users: generateUsers, documents: generateDocuments },
    multipliers: { orgs: 0.01, users: 0.1, documents: 1 },
  },
  social: {
    tables: { users: generateUsers, posts: generatePosts, reviews: generateReviews },
    multipliers: { users: 0.1, posts: 1, reviews: 0.5 },
  },
}

// Collect generator output into a string (for smaller datasets)
function collectGenerator(gen: Generator<string>): string {
  const lines: string[] = []
  for (const line of gen) {
    lines.push(line)
  }
  return lines.join('\n') + '\n'
}

// Stream generator in chunks to R2 using multipart upload (for large datasets)
async function streamToR2Multipart(
  bucket: R2Bucket,
  key: string,
  gen: Generator<string>,
  metadata: { dataset: string; size: string; table: string; records: string }
): Promise<number> {
  const upload = await bucket.createMultipartUpload(key, {
    httpMetadata: { contentType: 'application/x-ndjson' },
    customMetadata: metadata,
  })

  const parts: R2UploadedPart[] = []
  let buffer = ''
  let totalSize = 0
  const CHUNK_SIZE = 5 * 1024 * 1024 // 5MB minimum for R2 multipart

  try {
    for (const line of gen) {
      buffer += line + '\n'

      if (buffer.length >= CHUNK_SIZE) {
        const partNumber = parts.length + 1
        const part = await upload.uploadPart(partNumber, buffer)
        parts.push(part)
        totalSize += buffer.length
        buffer = ''
      }
    }

    // Upload remaining buffer
    if (buffer.length > 0) {
      const partNumber = parts.length + 1
      const part = await upload.uploadPart(partNumber, buffer)
      parts.push(part)
      totalSize += buffer.length
    }

    await upload.complete(parts)
    return totalSize
  } catch (error) {
    await upload.abort()
    throw error
  }
}

const app = new Hono<{ Bindings: Env }>()

app.get('/', (c) => c.json({
  service: 'stage-oltp',
  endpoints: {
    'GET /stage/:dataset/:size': 'Generate and stage dataset (e.g., /stage/ecommerce/100mb)',
    'GET /status/:dataset/:size': 'Check if dataset exists',
  },
  datasets: Object.keys(DATASETS),
  sizes: Object.keys(SIZES),
}))

app.get('/health', (c) => c.json({ status: 'ok', service: 'stage-oltp' }))

app.get('/datasets', (c) => c.json({
  datasets: Object.entries(DATASETS).map(([id, config]) => ({
    id,
    tables: Object.keys(config.tables),
    sizes: Object.keys(SIZES),
  })),
}))

// Main staging endpoint - now GET for easy browser access
app.get('/stage/:dataset/:size', async (c) => {
  const dataset = c.req.param('dataset') as DatasetType
  const size = c.req.param('size') as SizeOption

  if (!DATASETS[dataset]) {
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

  // Seed Faker for reproducibility
  faker.seed(42)

  const config = DATASETS[dataset]
  const baseCount = SIZES[size]
  const files: { key: string; size: number; records: number }[] = []
  const start = Date.now()

  const useMultipart = size === '100mb' || size === '1gb'

  for (const [table, generator] of Object.entries(config.tables)) {
    const count = Math.floor(baseCount * (config.multipliers[table] ?? 1))
    const key = `${prefix}${table}.jsonl`

    console.log(`${useMultipart ? 'Streaming' : 'Generating'} ${table} (${count} records)...`)
    faker.seed(42 + table.charCodeAt(0)) // Different seed per table

    const metadata = { dataset, size, table, records: String(count) }
    let fileSize: number

    if (useMultipart) {
      // Use multipart upload for large datasets
      fileSize = await streamToR2Multipart(c.env.DATASETS, key, generator(count), metadata)
    } else {
      // Collect and upload for small datasets
      const content = collectGenerator(generator(count))
      await c.env.DATASETS.put(key, content, {
        httpMetadata: { contentType: 'application/x-ndjson' },
        customMetadata: metadata,
      })
      fileSize = new Blob([content]).size
    }

    files.push({ key, size: fileSize, records: count })
  }

  return c.json({
    success: true,
    dataset,
    size,
    duration: Date.now() - start,
    files,
  })
})

// Keep POST for backward compatibility
app.post('/stage/:dataset/:size', async (c) => {
  // Redirect to GET handler
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

export default app
