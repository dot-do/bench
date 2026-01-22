/**
 * E-commerce Synthetic OLTP Dataset Generator
 *
 * Generates fake data for: products, orders, customers, reviews
 * Uses deterministic seeding for reproducibility.
 */

import * as fs from 'fs'
import * as path from 'path'

// Seeded random number generator (Mulberry32)
function createRng(seed: number) {
  return function () {
    let t = (seed += 0x6d2b79f5)
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

type SizeOption = '1mb' | '10mb' | '100mb' | '1gb'

interface GenerateOptions {
  size?: SizeOption
}

const SIZE_CONFIGS: Record<SizeOption, { customers: number; products: number; orders: number; reviews: number }> = {
  '1mb': { customers: 200, products: 500, orders: 1000, reviews: 800 },
  '10mb': { customers: 2000, products: 5000, orders: 10000, reviews: 8000 },
  '100mb': { customers: 20000, products: 50000, orders: 100000, reviews: 80000 },
  '1gb': { customers: 200000, products: 500000, orders: 1000000, reviews: 800000 },
}

// Sample data for realistic generation
const FIRST_NAMES = ['James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda', 'William', 'Elizabeth', 'David', 'Barbara', 'Richard', 'Susan', 'Joseph', 'Jessica', 'Thomas', 'Sarah', 'Charles', 'Karen']
const LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin']
const CITIES = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose']
const STATES = ['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX', 'CA', 'TX', 'CA']
const PRODUCT_ADJECTIVES = ['Premium', 'Classic', 'Modern', 'Deluxe', 'Essential', 'Professional', 'Ultimate', 'Elite', 'Basic', 'Advanced']
const PRODUCT_NOUNS = ['Widget', 'Gadget', 'Device', 'Tool', 'System', 'Kit', 'Set', 'Pack', 'Bundle', 'Collection']
const CATEGORIES = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys', 'Health', 'Beauty', 'Automotive', 'Food']
const BRANDS = ['Acme', 'TechCorp', 'GlobalBrand', 'PrimeLine', 'ValueMax', 'QualityFirst', 'ProGrade', 'EcoSmart', 'FastTrack', 'SureGrip']
const ORDER_STATUSES = ['pending', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled']
const PAYMENT_STATUSES = ['pending', 'authorized', 'captured', 'failed', 'refunded']
const TIERS = ['standard', 'premium', 'vip']
const REVIEW_TITLES = ['Great product!', 'Exactly what I needed', 'Good value', 'Decent quality', 'Not bad', 'Could be better', 'Disappointed', 'Amazing!', 'Highly recommend', 'Works as expected']

function pick<T>(arr: T[], rng: () => number): T {
  return arr[Math.floor(rng() * arr.length)]
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
      uuid += hex[(Math.floor(rng() * 4) + 8)]
    } else {
      uuid += hex[Math.floor(rng() * 16)]
    }
  }
  return uuid
}

function generateTimestamp(rng: () => number, startYear: number, endYear: number): string {
  const start = new Date(startYear, 0, 1).getTime()
  const end = new Date(endYear, 11, 31).getTime()
  const timestamp = new Date(start + rng() * (end - start))
  return timestamp.toISOString()
}

function generateEmail(firstName: string, lastName: string, rng: () => number): string {
  const domains = ['gmail.com', 'yahoo.com', 'outlook.com', 'email.com', 'mail.com']
  const num = Math.floor(rng() * 1000)
  return `${firstName.toLowerCase()}.${lastName.toLowerCase()}${num}@${pick(domains, rng)}`
}

function generateCustomers(count: number, rng: () => number): any[] {
  const customers: any[] = []
  for (let i = 0; i < count; i++) {
    const firstName = pick(FIRST_NAMES, rng)
    const lastName = pick(LAST_NAMES, rng)
    const cityIdx = Math.floor(rng() * CITIES.length)
    customers.push({
      id: generateUuid(rng),
      email: generateEmail(firstName, lastName, rng),
      first_name: firstName,
      last_name: lastName,
      phone: `+1${Math.floor(rng() * 9000000000 + 1000000000)}`,
      tier: pick(TIERS, rng),
      total_spent: Math.round(rng() * 50000 * 100) / 100,
      order_count: Math.floor(rng() * 100),
      address: {
        street: `${Math.floor(rng() * 9999 + 1)} ${pick(['Main', 'Oak', 'Maple', 'Cedar', 'Pine'], rng)} ${pick(['St', 'Ave', 'Blvd', 'Rd'], rng)}`,
        city: CITIES[cityIdx],
        state: STATES[cityIdx],
        postal_code: String(Math.floor(rng() * 90000 + 10000)),
        country: 'US',
      },
      created_at: generateTimestamp(rng, 2020, 2024),
      updated_at: generateTimestamp(rng, 2023, 2024),
    })
  }
  return customers
}

function generateProducts(count: number, rng: () => number): any[] {
  const products: any[] = []
  for (let i = 0; i < count; i++) {
    const price = Math.round((rng() * 999 + 1) * 100) / 100
    products.push({
      id: generateUuid(rng),
      sku: `SKU-${String(i + 1).padStart(8, '0')}`,
      name: `${pick(PRODUCT_ADJECTIVES, rng)} ${pick(PRODUCT_NOUNS, rng)}`,
      description: `High-quality ${pick(PRODUCT_NOUNS, rng).toLowerCase()} for everyday use. Features advanced technology and durable construction.`,
      category: pick(CATEGORIES, rng),
      brand: pick(BRANDS, rng),
      price: price,
      cost: Math.round(price * (0.3 + rng() * 0.4) * 100) / 100,
      stock_quantity: Math.floor(rng() * 1000),
      is_active: rng() > 0.1,
      is_featured: rng() < 0.05,
      rating_avg: Math.round((1 + rng() * 4) * 100) / 100,
      rating_count: Math.floor(rng() * 500),
      view_count: Math.floor(rng() * 100000),
      created_at: generateTimestamp(rng, 2020, 2024),
      updated_at: generateTimestamp(rng, 2023, 2024),
    })
  }
  return products
}

function generateOrders(count: number, customerIds: string[], productIds: string[], rng: () => number): any[] {
  const orders: any[] = []
  for (let i = 0; i < count; i++) {
    const itemCount = Math.floor(rng() * 5) + 1
    const items: any[] = []
    let subtotal = 0
    for (let j = 0; j < itemCount; j++) {
      const quantity = Math.floor(rng() * 3) + 1
      const unitPrice = Math.round((rng() * 500 + 10) * 100) / 100
      const itemTotal = Math.round(quantity * unitPrice * 100) / 100
      subtotal += itemTotal
      items.push({
        product_id: pick(productIds, rng),
        quantity,
        unit_price: unitPrice,
        total: itemTotal,
      })
    }
    const discount = Math.round(subtotal * rng() * 0.2 * 100) / 100
    const tax = Math.round((subtotal - discount) * 0.08 * 100) / 100
    const shipping = Math.round(rng() * 20 * 100) / 100
    const total = Math.round((subtotal - discount + tax + shipping) * 100) / 100

    orders.push({
      id: generateUuid(rng),
      order_number: `ORD-${String(i + 1).padStart(10, '0')}`,
      customer_id: pick(customerIds, rng),
      status: pick(ORDER_STATUSES, rng),
      payment_status: pick(PAYMENT_STATUSES, rng),
      subtotal,
      discount_amount: discount,
      tax_amount: tax,
      shipping_amount: shipping,
      total,
      currency: 'USD',
      items,
      shipping_method: pick(['standard', 'express', 'overnight', 'pickup'], rng),
      created_at: generateTimestamp(rng, 2023, 2024),
      updated_at: generateTimestamp(rng, 2024, 2024),
    })
  }
  return orders
}

function generateReviews(count: number, customerIds: string[], productIds: string[], rng: () => number): any[] {
  const reviews: any[] = []
  for (let i = 0; i < count; i++) {
    const rating = Math.floor(rng() * 5) + 1
    reviews.push({
      id: generateUuid(rng),
      product_id: pick(productIds, rng),
      customer_id: pick(customerIds, rng),
      rating,
      title: pick(REVIEW_TITLES, rng),
      body: rating >= 4
        ? 'This product exceeded my expectations. Great quality and fast shipping. Would definitely recommend!'
        : rating >= 3
          ? 'Product is okay. Does what it says but nothing special. Average quality for the price.'
          : 'Not satisfied with this purchase. Quality could be better. Expected more based on the description.',
      is_verified_purchase: rng() > 0.3,
      helpful_count: Math.floor(rng() * 100),
      status: rng() > 0.1 ? 'approved' : 'pending',
      created_at: generateTimestamp(rng, 2023, 2024),
      updated_at: generateTimestamp(rng, 2024, 2024),
    })
  }
  return reviews
}

function writeJsonl(filePath: string, data: any[]): void {
  const stream = fs.createWriteStream(filePath)
  for (const item of data) {
    stream.write(JSON.stringify(item) + '\n')
  }
  stream.end()
}

export async function generate(outputDir: string, options?: GenerateOptions): Promise<void> {
  const size = options?.size || '1mb'
  const config = SIZE_CONFIGS[size]
  const seed = 12345 // Fixed seed for reproducibility

  console.log(`Generating e-commerce dataset (${size})...`)

  // Create output directory if it doesn't exist
  fs.mkdirSync(outputDir, { recursive: true })

  // Generate with deterministic seed
  const rng = createRng(seed)

  console.log(`  Generating ${config.customers} customers...`)
  const customers = generateCustomers(config.customers, rng)
  writeJsonl(path.join(outputDir, 'customers.jsonl'), customers)

  console.log(`  Generating ${config.products} products...`)
  const products = generateProducts(config.products, rng)
  writeJsonl(path.join(outputDir, 'products.jsonl'), products)

  const customerIds = customers.map(c => c.id)
  const productIds = products.map(p => p.id)

  console.log(`  Generating ${config.orders} orders...`)
  const orders = generateOrders(config.orders, customerIds, productIds, rng)
  writeJsonl(path.join(outputDir, 'orders.jsonl'), orders)

  console.log(`  Generating ${config.reviews} reviews...`)
  const reviews = generateReviews(config.reviews, customerIds, productIds, rng)
  writeJsonl(path.join(outputDir, 'reviews.jsonl'), reviews)

  console.log(`E-commerce dataset generated in ${outputDir}`)
}

// CLI support
if (typeof require !== 'undefined' && require.main === module) {
  const args = process.argv.slice(2)
  const outputDir = args[0] || './data/ecommerce'
  const size = (args[1] as SizeOption) || '1mb'
  generate(outputDir, { size }).catch(console.error)
}
