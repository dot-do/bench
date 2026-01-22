/**
 * Stage OLTP Datasets Worker
 *
 * A Cloudflare Worker that generates and stages OLTP datasets directly in-Worker.
 * Uses deterministic seeding for reproducible data generation.
 * Generated JSONL files are stored in R2 for benchmarking.
 *
 * Endpoint: POST /stage/{dataset}/{size}
 * - dataset: ecommerce | saas | social | iot
 * - size: 1mb | 10mb | 100mb | 1gb
 *
 * @see scripts/download/ecommerce.ts - E-commerce dataset generator reference
 * @see scripts/download/saas.ts - SaaS multi-tenant dataset generator reference
 * @see scripts/download/social.ts - Social network dataset generator reference
 * @see scripts/download/iot.ts - IoT timeseries dataset generator reference
 */

import { Hono } from 'hono'

// Environment bindings
interface Env {
  // R2 bucket for storing generated datasets
  DATASETS: R2Bucket
}

// Valid dataset types
type DatasetType = 'ecommerce' | 'saas' | 'social' | 'iot'

// Valid size options
type SizeOption = '1mb' | '10mb' | '100mb' | '1gb'

// Dataset configuration for each type
const DATASET_CONFIGS: Record<DatasetType, { name: string; description: string; tables: string[] }> = {
  ecommerce: {
    name: 'E-commerce OLTP',
    description: 'Products, orders, customers, reviews',
    tables: ['customers', 'products', 'orders', 'reviews'],
  },
  saas: {
    name: 'SaaS Multi-Tenant',
    description: 'Organizations, users, workspaces, documents',
    tables: ['orgs', 'users', 'workspaces', 'documents'],
  },
  social: {
    name: 'Social Network',
    description: 'Users, posts, comments, likes, follows',
    tables: ['users', 'posts', 'comments', 'likes', 'follows'],
  },
  iot: {
    name: 'IoT Timeseries',
    description: 'Devices, sensors, readings',
    tables: ['devices', 'sensors', 'readings'],
  },
}

// Size validation
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
      uuid += hex[Math.floor(rng() * 4) + 8]
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

// =============================================================================
// Common Data Arrays
// =============================================================================

const FIRST_NAMES = ['James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda', 'William', 'Elizabeth', 'David', 'Barbara', 'Richard', 'Susan', 'Joseph', 'Jessica', 'Thomas', 'Sarah', 'Charles', 'Karen']
const LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin']

// =============================================================================
// E-commerce Dataset Generator
// =============================================================================

const ECOMMERCE_SEED = 12345

const ECOMMERCE_SIZE_CONFIGS: Record<SizeOption, { customers: number; products: number; orders: number; reviews: number }> = {
  '1mb': { customers: 200, products: 500, orders: 1000, reviews: 800 },
  '10mb': { customers: 2000, products: 5000, orders: 10000, reviews: 8000 },
  '100mb': { customers: 20000, products: 50000, orders: 100000, reviews: 80000 },
  '1gb': { customers: 200000, products: 500000, orders: 1000000, reviews: 800000 },
}

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

function generateEmail(firstName: string, lastName: string, rng: () => number): string {
  const domains = ['gmail.com', 'yahoo.com', 'outlook.com', 'email.com', 'mail.com']
  const num = Math.floor(rng() * 1000)
  return `${firstName.toLowerCase()}.${lastName.toLowerCase()}${num}@${pick(domains, rng)}`
}

interface EcommerceData {
  customers: string
  products: string
  orders: string
  reviews: string
}

function generateEcommerceData(size: SizeOption): EcommerceData {
  const config = ECOMMERCE_SIZE_CONFIGS[size]
  const rng = createRng(ECOMMERCE_SEED)

  // Generate customers
  const customers: string[] = []
  const customerIds: string[] = []
  for (let i = 0; i < config.customers; i++) {
    const firstName = pick(FIRST_NAMES, rng)
    const lastName = pick(LAST_NAMES, rng)
    const cityIdx = Math.floor(rng() * CITIES.length)
    const id = generateUuid(rng)
    customerIds.push(id)
    customers.push(JSON.stringify({
      id,
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
    }))
  }

  // Generate products
  const products: string[] = []
  const productIds: string[] = []
  for (let i = 0; i < config.products; i++) {
    const price = Math.round((rng() * 999 + 1) * 100) / 100
    const id = generateUuid(rng)
    productIds.push(id)
    products.push(JSON.stringify({
      id,
      sku: `SKU-${String(i + 1).padStart(8, '0')}`,
      name: `${pick(PRODUCT_ADJECTIVES, rng)} ${pick(PRODUCT_NOUNS, rng)}`,
      description: `High-quality ${pick(PRODUCT_NOUNS, rng).toLowerCase()} for everyday use.`,
      category: pick(CATEGORIES, rng),
      brand: pick(BRANDS, rng),
      price,
      cost: Math.round(price * (0.3 + rng() * 0.4) * 100) / 100,
      stock_quantity: Math.floor(rng() * 1000),
      is_active: rng() > 0.1,
      rating_avg: Math.round((1 + rng() * 4) * 100) / 100,
      rating_count: Math.floor(rng() * 500),
      created_at: generateTimestamp(rng, 2020, 2024),
      updated_at: generateTimestamp(rng, 2023, 2024),
    }))
  }

  // Generate orders
  const orders: string[] = []
  for (let i = 0; i < config.orders; i++) {
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

    orders.push(JSON.stringify({
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
    }))
  }

  // Generate reviews
  const reviews: string[] = []
  for (let i = 0; i < config.reviews; i++) {
    const rating = Math.floor(rng() * 5) + 1
    reviews.push(JSON.stringify({
      id: generateUuid(rng),
      product_id: pick(productIds, rng),
      customer_id: pick(customerIds, rng),
      rating,
      title: pick(REVIEW_TITLES, rng),
      body: rating >= 4 ? 'This product exceeded my expectations. Great quality and fast shipping.' : rating >= 3 ? 'Product is okay. Does what it says but nothing special.' : 'Not satisfied with this purchase. Quality could be better.',
      is_verified_purchase: rng() > 0.3,
      helpful_count: Math.floor(rng() * 100),
      status: rng() > 0.1 ? 'approved' : 'pending',
      created_at: generateTimestamp(rng, 2023, 2024),
      updated_at: generateTimestamp(rng, 2024, 2024),
    }))
  }

  return {
    customers: customers.join('\n'),
    products: products.join('\n'),
    orders: orders.join('\n'),
    reviews: reviews.join('\n'),
  }
}

// =============================================================================
// SaaS Dataset Generator
// =============================================================================

const SAAS_SEED = 23456

const SAAS_SIZE_CONFIGS: Record<SizeOption, { orgs: number; users: number; workspaces: number; documents: number }> = {
  '1mb': { orgs: 10, users: 100, workspaces: 50, documents: 500 },
  '10mb': { orgs: 50, users: 1000, workspaces: 400, documents: 5000 },
  '100mb': { orgs: 200, users: 10000, workspaces: 3000, documents: 50000 },
  '1gb': { orgs: 1000, users: 100000, workspaces: 25000, documents: 500000 },
}

const COMPANY_SUFFIXES = ['Inc', 'LLC', 'Corp', 'Co', 'Ltd', 'Group', 'Solutions', 'Technologies', 'Systems', 'Labs']
const COMPANY_PREFIXES = ['Tech', 'Global', 'Digital', 'Smart', 'Cloud', 'Data', 'Cyber', 'Net', 'Web', 'App']
const PLANS = ['free', 'starter', 'professional', 'enterprise']
const ORG_STATUSES = ['active', 'trial', 'suspended', 'cancelled']
const USER_ROLES = ['owner', 'admin', 'manager', 'member', 'viewer']
const DOC_TYPES = ['document', 'spreadsheet', 'presentation', 'note', 'wiki', 'template']
const WORKSPACE_NAMES = ['Engineering', 'Marketing', 'Sales', 'Product', 'Design', 'Operations']
const DOC_TITLES = ['Project Proposal', 'Meeting Notes', 'Technical Spec', 'User Guide', 'API Documentation', 'Budget Report', 'Roadmap']

function generateSlug(name: string, rng: () => number): string {
  return name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '') + '-' + Math.floor(rng() * 10000)
}

interface SaasData {
  orgs: string
  users: string
  workspaces: string
  documents: string
}

function generateSaasData(size: SizeOption): SaasData {
  const config = SAAS_SIZE_CONFIGS[size]
  const rng = createRng(SAAS_SEED)

  // Generate orgs
  const orgs: string[] = []
  const orgIds: string[] = []
  const orgDomains = new Map<string, string>()
  for (let i = 0; i < config.orgs; i++) {
    const name = `${pick(COMPANY_PREFIXES, rng)} ${pick(COMPANY_SUFFIXES, rng)}`
    const plan = pick(PLANS, rng)
    const id = generateUuid(rng)
    const domain = name.toLowerCase().replace(/\s+/g, '') + '.com'
    orgIds.push(id)
    orgDomains.set(id, domain)
    orgs.push(JSON.stringify({
      id,
      name,
      slug: generateSlug(name, rng),
      domain,
      plan,
      status: pick(ORG_STATUSES, rng),
      max_users: plan === 'free' ? 5 : plan === 'starter' ? 10 : plan === 'professional' ? 50 : 1000,
      created_at: generateTimestamp(rng, 2020, 2024),
      updated_at: generateTimestamp(rng, 2023, 2024),
    }))
  }

  // Generate users
  const users: string[] = []
  const userIds: string[] = []
  const usersByOrg = new Map<string, string[]>()
  for (let i = 0; i < config.users; i++) {
    const firstName = pick(FIRST_NAMES, rng)
    const lastName = pick(LAST_NAMES, rng)
    const orgId = pick(orgIds, rng)
    const domain = orgDomains.get(orgId) || 'example.com'
    const id = generateUuid(rng)
    userIds.push(id)

    if (!usersByOrg.has(orgId)) {
      usersByOrg.set(orgId, [])
    }
    usersByOrg.get(orgId)!.push(id)

    users.push(JSON.stringify({
      id,
      org_id: orgId,
      email: `${firstName.toLowerCase()}.${lastName.toLowerCase()}${Math.floor(rng() * 100)}@${domain}`,
      first_name: firstName,
      last_name: lastName,
      display_name: `${firstName} ${lastName}`,
      role: pick(USER_ROLES, rng),
      status: pick(['active', 'pending', 'suspended'], rng),
      created_at: generateTimestamp(rng, 2020, 2024),
      updated_at: generateTimestamp(rng, 2023, 2024),
    }))
  }

  // Generate workspaces
  const workspaces: string[] = []
  const workspaceIds: string[] = []
  for (let i = 0; i < config.workspaces; i++) {
    const orgId = pick(orgIds, rng)
    const orgUsers = usersByOrg.get(orgId) || []
    const ownerId = orgUsers.length > 0 ? pick(orgUsers, rng) : generateUuid(rng)
    const name = pick(WORKSPACE_NAMES, rng)
    const id = generateUuid(rng)
    workspaceIds.push(id)
    workspaces.push(JSON.stringify({
      id,
      org_id: orgId,
      name: `${name} ${Math.floor(rng() * 100)}`,
      slug: generateSlug(name, rng),
      description: `Workspace for ${name.toLowerCase()} work.`,
      type: pick(['project', 'team', 'personal', 'shared'], rng),
      visibility: pick(['private', 'team', 'internal', 'public'], rng),
      owner_id: ownerId,
      created_at: generateTimestamp(rng, 2021, 2024),
      updated_at: generateTimestamp(rng, 2023, 2024),
    }))
  }

  // Generate documents
  const documents: string[] = []
  for (let i = 0; i < config.documents; i++) {
    const type = pick(DOC_TYPES, rng)
    const title = `${pick(DOC_TITLES, rng)} ${Math.floor(rng() * 1000)}`
    documents.push(JSON.stringify({
      id: generateUuid(rng),
      workspace_id: pick(workspaceIds, rng),
      title,
      slug: generateSlug(title, rng),
      type,
      status: pick(['draft', 'published', 'archived'], rng),
      content: `# ${title}\n\nThis is a sample ${type} with placeholder content.`,
      word_count: Math.floor(rng() * 5000) + 100,
      version: Math.floor(rng() * 20) + 1,
      created_by: pick(userIds, rng),
      last_edited_by: pick(userIds, rng),
      created_at: generateTimestamp(rng, 2022, 2024),
      updated_at: generateTimestamp(rng, 2024, 2024),
    }))
  }

  return {
    orgs: orgs.join('\n'),
    users: users.join('\n'),
    workspaces: workspaces.join('\n'),
    documents: documents.join('\n'),
  }
}

// =============================================================================
// Social Network Dataset Generator
// =============================================================================

const SOCIAL_SEED = 34567

const SOCIAL_SIZE_CONFIGS: Record<SizeOption, { users: number; posts: number; comments: number; likes: number; follows: number }> = {
  '1mb': { users: 500, posts: 2000, comments: 3000, likes: 15000, follows: 5000 },
  '10mb': { users: 5000, posts: 20000, comments: 30000, likes: 150000, follows: 50000 },
  '100mb': { users: 50000, posts: 200000, comments: 300000, likes: 1500000, follows: 500000 },
  '1gb': { users: 500000, posts: 2000000, comments: 3000000, likes: 15000000, follows: 5000000 },
}

const LOCATIONS = ['New York, NY', 'Los Angeles, CA', 'Chicago, IL', 'Houston, TX', 'Phoenix, AZ', 'London, UK', 'Toronto, CA', 'Sydney, AU']
const POST_TYPES = ['text', 'image', 'video', 'link']
const BIOS = ['Living life to the fullest', 'Coffee enthusiast | Travel lover', 'Tech geek | Entrepreneur', 'Artist | Dreamer | Creator']
const POST_TEMPLATES = ['Just had an amazing day!', 'Working on something exciting.', 'Check out this view!', 'Great meeting today.', 'Learning something new.']
const COMMENT_TEMPLATES = ['Love this!', 'So cool!', 'Amazing!', 'Great post!', 'This is awesome!']

function generateUsername(firstName: string, lastName: string, rng: () => number): string {
  return firstName.toLowerCase() + lastName.toLowerCase() + Math.floor(rng() * 10000)
}

interface SocialData {
  users: string
  posts: string
  comments: string
  likes: string
  follows: string
}

function generateSocialData(size: SizeOption): SocialData {
  const config = SOCIAL_SIZE_CONFIGS[size]
  const rng = createRng(SOCIAL_SEED)

  // Generate users
  const users: string[] = []
  const userIds: string[] = []
  for (let i = 0; i < config.users; i++) {
    const firstName = pick(FIRST_NAMES, rng)
    const lastName = pick(LAST_NAMES, rng)
    const username = generateUsername(firstName, lastName, rng)
    const followerTier = rng()
    const followerCount = followerTier < 0.7 ? Math.floor(rng() * 500) :
      followerTier < 0.9 ? Math.floor(rng() * 10000) :
      followerTier < 0.98 ? Math.floor(rng() * 100000) :
      Math.floor(rng() * 1000000)
    const id = generateUuid(rng)
    userIds.push(id)
    users.push(JSON.stringify({
      id,
      username,
      email: `${username}@${pick(['gmail.com', 'yahoo.com', 'outlook.com'], rng)}`,
      display_name: `${firstName} ${lastName}`,
      bio: rng() > 0.3 ? pick(BIOS, rng) : null,
      location: rng() > 0.4 ? pick(LOCATIONS, rng) : null,
      is_verified: rng() < 0.05,
      is_private: rng() < 0.2,
      status: pick(['active', 'suspended', 'deactivated'], rng),
      follower_count: followerCount,
      following_count: Math.floor(rng() * Math.min(5000, followerCount * 2 + 100)),
      post_count: Math.floor(rng() * 1000),
      created_at: generateTimestamp(rng, 2015, 2024),
      updated_at: generateTimestamp(rng, 2023, 2024),
    }))
  }

  // Generate posts
  const posts: string[] = []
  const postIds: string[] = []
  for (let i = 0; i < config.posts; i++) {
    const type = pick(POST_TYPES, rng)
    const viralTier = rng()
    const likeCount = viralTier < 0.8 ? Math.floor(rng() * 100) :
      viralTier < 0.95 ? Math.floor(rng() * 5000) :
      Math.floor(rng() * 50000)
    const id = generateUuid(rng)
    postIds.push(id)
    posts.push(JSON.stringify({
      id,
      user_id: pick(userIds, rng),
      content: pick(POST_TEMPLATES, rng),
      type,
      visibility: pick(['public', 'followers', 'private'], rng),
      like_count: likeCount,
      comment_count: Math.floor(likeCount * (0.05 + rng() * 0.15)),
      view_count: likeCount * (5 + Math.floor(rng() * 20)),
      created_at: generateTimestamp(rng, 2020, 2024),
      updated_at: rng() > 0.9 ? generateTimestamp(rng, 2024, 2024) : null,
    }))
  }

  // Generate comments
  const comments: string[] = []
  const commentIds: string[] = []
  for (let i = 0; i < config.comments; i++) {
    const id = generateUuid(rng)
    commentIds.push(id)
    comments.push(JSON.stringify({
      id,
      post_id: pick(postIds, rng),
      user_id: pick(userIds, rng),
      parent_id: rng() < 0.2 && commentIds.length > 0 ? commentIds[Math.floor(rng() * Math.min(100, commentIds.length))] : null,
      content: pick(COMMENT_TEMPLATES, rng),
      like_count: Math.floor(rng() * 100),
      created_at: generateTimestamp(rng, 2020, 2024),
    }))
  }

  // Generate likes
  const likes: string[] = []
  const seenLikes = new Set<string>()
  for (let i = 0; i < config.likes; i++) {
    let postId: string, userId: string, key: string
    let attempts = 0
    do {
      postId = pick(postIds, rng)
      userId = pick(userIds, rng)
      key = `${postId}:${userId}`
      attempts++
    } while (seenLikes.has(key) && attempts < 10)
    if (attempts >= 10) continue
    seenLikes.add(key)
    likes.push(JSON.stringify({
      id: generateUuid(rng),
      post_id: postId,
      user_id: userId,
      created_at: generateTimestamp(rng, 2020, 2024),
    }))
  }

  // Generate follows
  const follows: string[] = []
  const seenFollows = new Set<string>()
  for (let i = 0; i < config.follows; i++) {
    let followerId: string, followingId: string, key: string
    let attempts = 0
    do {
      followerId = pick(userIds, rng)
      followingId = pick(userIds, rng)
      key = `${followerId}:${followingId}`
      attempts++
    } while ((seenFollows.has(key) || followerId === followingId) && attempts < 10)
    if (attempts >= 10) continue
    seenFollows.add(key)
    follows.push(JSON.stringify({
      id: generateUuid(rng),
      follower_id: followerId,
      following_id: followingId,
      status: rng() > 0.1 ? 'accepted' : rng() > 0.5 ? 'pending' : 'blocked',
      created_at: generateTimestamp(rng, 2015, 2024),
    }))
  }

  return {
    users: users.join('\n'),
    posts: posts.join('\n'),
    comments: comments.join('\n'),
    likes: likes.join('\n'),
    follows: follows.join('\n'),
  }
}

// =============================================================================
// IoT Dataset Generator
// =============================================================================

const IOT_SEED = 45678

const IOT_SIZE_CONFIGS: Record<SizeOption, { devices: number; sensors: number; readings: number }> = {
  '1mb': { devices: 50, sensors: 150, readings: 50000 },
  '10mb': { devices: 500, sensors: 1500, readings: 500000 },
  '100mb': { devices: 5000, sensors: 15000, readings: 5000000 },
  '1gb': { devices: 50000, sensors: 150000, readings: 50000000 },
}

const DEVICE_TYPES = ['temperature_sensor', 'humidity_sensor', 'pressure_sensor', 'motion_detector', 'smart_meter', 'air_quality']
const DEVICE_CATEGORIES = ['sensor', 'actuator', 'gateway', 'controller', 'meter']
const MANUFACTURERS = ['SensorCorp', 'IoTech', 'SmartSense', 'DataFlow', 'EdgeDevices', 'ConnectAll']
const DEVICE_STATUSES = ['online', 'offline', 'maintenance', 'error', 'provisioning']
const METRIC_NAMES = ['temperature', 'humidity', 'pressure', 'co2', 'pm25', 'voltage', 'current', 'power', 'flow_rate', 'vibration']
const METRIC_UNITS: Record<string, string> = { temperature: 'celsius', humidity: 'percent', pressure: 'hPa', co2: 'ppm', pm25: 'ug/m3', voltage: 'volts', current: 'amps', power: 'watts', flow_rate: 'l/min', vibration: 'mm/s' }
const METRIC_RANGES: Record<string, [number, number]> = { temperature: [-20, 50], humidity: [0, 100], pressure: [950, 1050], co2: [300, 2000], pm25: [0, 500], voltage: [100, 250], current: [0, 50], power: [0, 10000], flow_rate: [0, 100], vibration: [0, 50] }
const BUILDING_NAMES = ['Building A', 'Building B', 'Main Office', 'Warehouse', 'Factory', 'Data Center']

function generateSerialNumber(rng: () => number): string {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
  let serial = ''
  for (let i = 0; i < 12; i++) {
    serial += chars[Math.floor(rng() * chars.length)]
  }
  return serial
}

function generateMacAddress(rng: () => number): string {
  const hex = '0123456789ABCDEF'
  const parts: string[] = []
  for (let i = 0; i < 6; i++) {
    parts.push(hex[Math.floor(rng() * 16)] + hex[Math.floor(rng() * 16)])
  }
  return parts.join(':')
}

interface IoTData {
  devices: string
  sensors: string
  readings: string
}

interface SensorInfo {
  id: string
  device_id: string
  metric_name: string
  unit: string
}

function generateIoTData(size: SizeOption): IoTData {
  const config = IOT_SIZE_CONFIGS[size]
  const rng = createRng(IOT_SEED)

  // Generate devices
  const devices: string[] = []
  const deviceIds: string[] = []
  for (let i = 0; i < config.devices; i++) {
    const deviceType = pick(DEVICE_TYPES, rng)
    const manufacturer = pick(MANUFACTURERS, rng)
    const status = pick(DEVICE_STATUSES, rng)
    const id = generateUuid(rng)
    deviceIds.push(id)
    devices.push(JSON.stringify({
      id,
      serial_number: generateSerialNumber(rng),
      name: `${deviceType.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())} ${i + 1}`,
      device_type: deviceType,
      category: pick(DEVICE_CATEGORIES, rng),
      manufacturer,
      model: `${manufacturer.slice(0, 3).toUpperCase()}-${Math.floor(rng() * 9000) + 1000}`,
      firmware_version: `${Math.floor(rng() * 5 + 1)}.${Math.floor(rng() * 10)}.${Math.floor(rng() * 100)}`,
      status,
      is_active: status !== 'maintenance' && rng() > 0.1,
      location: {
        name: `${pick(BUILDING_NAMES, rng)} - Zone ${Math.floor(rng() * 100 + 1)}`,
        latitude: 37.7749 + (rng() - 0.5) * 0.1,
        longitude: -122.4194 + (rng() - 0.5) * 0.1,
      },
      network: {
        ip_address: `192.168.${Math.floor(rng() * 256)}.${Math.floor(rng() * 256)}`,
        mac_address: generateMacAddress(rng),
      },
      battery_level: rng() > 0.3 ? Math.floor(rng() * 100) : null,
      registered_at: generateTimestamp(rng, 2020, 2024),
      last_seen_at: status === 'online' ? generateTimestamp(rng, 2024, 2024) : generateTimestamp(rng, 2023, 2024),
    }))
  }

  // Generate sensors
  const sensors: string[] = []
  const sensorInfos: SensorInfo[] = []
  for (let i = 0; i < config.sensors; i++) {
    const metricName = pick(METRIC_NAMES, rng)
    const [minVal, maxVal] = METRIC_RANGES[metricName]
    const id = generateUuid(rng)
    const deviceId = pick(deviceIds, rng)
    sensorInfos.push({ id, device_id: deviceId, metric_name: metricName, unit: METRIC_UNITS[metricName] })
    sensors.push(JSON.stringify({
      id,
      device_id: deviceId,
      name: `${metricName.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())} Sensor`,
      metric_name: metricName,
      unit: METRIC_UNITS[metricName],
      min_value: minVal,
      max_value: maxVal,
      precision: metricName === 'temperature' || metricName === 'humidity' ? 2 : metricName === 'pressure' ? 1 : 0,
      calibration: {
        offset: (rng() - 0.5) * 2,
        scale: 0.98 + rng() * 0.04,
        last_calibrated_at: generateTimestamp(rng, 2023, 2024),
      },
      is_active: rng() > 0.05,
      created_at: generateTimestamp(rng, 2020, 2024),
      updated_at: generateTimestamp(rng, 2024, 2024),
    }))
  }

  // Generate readings
  const readings: string[] = []
  const baseTime = new Date(2024, 0, 1).getTime()
  const endTime = new Date(2024, 11, 31).getTime()
  const timeSpan = endTime - baseTime

  for (let i = 0; i < config.readings; i++) {
    const sensor = pick(sensorInfos, rng)
    const [minVal, maxVal] = METRIC_RANGES[sensor.metric_name] || [0, 100]
    const midpoint = (minVal + maxVal) / 2
    const range = maxVal - minVal
    const u1 = rng()
    const u2 = rng()
    const gaussian = Math.sqrt(-2 * Math.log(u1 + 0.0001)) * Math.cos(2 * Math.PI * u2)
    const value = Math.max(minVal, Math.min(maxVal, midpoint + gaussian * range * 0.15))

    readings.push(JSON.stringify({
      id: i + 1,
      sensor_id: sensor.id,
      device_id: sensor.device_id,
      metric_name: sensor.metric_name,
      timestamp: new Date(baseTime + rng() * timeSpan).toISOString(),
      value: Math.round(value * 1000) / 1000,
      unit: sensor.unit,
      quality: rng() < 0.92 ? 'good' : rng() < 0.97 ? 'uncertain' : 'bad',
    }))
  }

  return {
    devices: devices.join('\n'),
    sensors: sensors.join('\n'),
    readings: readings.join('\n'),
  }
}

// =============================================================================
// Dataset Generation Dispatcher
// =============================================================================

type DatasetData = EcommerceData | SaasData | SocialData | IoTData

function generateDataset(dataset: DatasetType, size: SizeOption): DatasetData {
  switch (dataset) {
    case 'ecommerce':
      return generateEcommerceData(size)
    case 'saas':
      return generateSaasData(size)
    case 'social':
      return generateSocialData(size)
    case 'iot':
      return generateIoTData(size)
  }
}

// Response types
interface StageResult {
  success: boolean
  dataset: DatasetType
  size: SizeOption
  duration: number
  files: Array<{
    name: string
    size: number
    key: string
  }>
  error?: string
}

// Create Hono app
const app = new Hono<{ Bindings: Env }>()

// Health check
app.get('/health', (c) => {
  return c.json({ status: 'ok', service: 'stage-oltp' })
})

// List available datasets
app.get('/datasets', (c) => {
  return c.json({
    datasets: Object.entries(DATASET_CONFIGS).map(([key, config]) => ({
      id: key,
      ...config,
      sizes: VALID_SIZES,
    })),
  })
})

// Stage a dataset
app.post('/stage/:dataset/:size', async (c) => {
  const dataset = c.req.param('dataset') as DatasetType
  const size = c.req.param('size') as SizeOption
  const startTime = Date.now()

  // Validate inputs
  if (!DATASET_CONFIGS[dataset]) {
    return c.json(
      {
        success: false,
        error: `Invalid dataset: ${dataset}. Valid options: ${Object.keys(DATASET_CONFIGS).join(', ')}`,
      },
      400
    )
  }

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
  const existingPrefix = `oltp/${dataset}/${size}/`
  const existingFiles = await c.env.DATASETS.list({ prefix: existingPrefix })
  if (existingFiles.objects.length > 0) {
    // Return existing dataset info
    const files = existingFiles.objects.map((obj) => ({
      name: obj.key.split('/').pop()!,
      size: obj.size,
      key: obj.key,
    }))
    return c.json({
      success: true,
      cached: true,
      dataset,
      size,
      duration: Date.now() - startTime,
      files,
    })
  }

  try {
    // Generate dataset in-worker
    console.log(`Generating ${dataset} dataset (${size}) in-worker...`)
    const data = generateDataset(dataset, size)
    const generationTime = Date.now() - startTime
    console.log(`Generation complete in ${generationTime}ms`)

    // Upload generated files to R2
    const config = DATASET_CONFIGS[dataset]
    const uploadedFiles: Array<{ name: string; size: number; key: string }> = []

    for (const table of config.tables) {
      const filename = `${table}.jsonl`
      const content = (data as unknown as Record<string, string>)[table]

      if (!content) {
        console.warn(`Warning: No data generated for table ${table}`)
        continue
      }

      console.log(`Uploading ${filename}...`)

      // Upload to R2
      const r2Key = `oltp/${dataset}/${size}/${filename}`
      await c.env.DATASETS.put(r2Key, content, {
        httpMetadata: {
          contentType: 'application/x-ndjson',
        },
        customMetadata: {
          dataset,
          size,
          table,
          generatedAt: new Date().toISOString(),
        },
      })

      uploadedFiles.push({
        name: filename,
        size: new Blob([content]).size,
        key: r2Key,
      })
    }

    const response: StageResult = {
      success: true,
      dataset,
      size,
      duration: Date.now() - startTime,
      files: uploadedFiles,
    }

    return c.json(response)
  } catch (error) {
    console.error(`Error staging ${dataset}/${size}:`, error)
    return c.json(
      {
        success: false,
        dataset,
        size,
        duration: Date.now() - startTime,
        files: [],
        error: error instanceof Error ? error.message : String(error),
      } as StageResult,
      500
    )
  }
})

// Check dataset status
app.get('/status/:dataset/:size', async (c) => {
  const dataset = c.req.param('dataset') as DatasetType
  const size = c.req.param('size') as SizeOption

  // Validate inputs
  if (!DATASET_CONFIGS[dataset]) {
    return c.json({ error: `Invalid dataset: ${dataset}` }, 400)
  }
  if (!VALID_SIZES.includes(size)) {
    return c.json({ error: `Invalid size: ${size}` }, 400)
  }

  const prefix = `oltp/${dataset}/${size}/`
  const files = await c.env.DATASETS.list({ prefix })

  if (files.objects.length === 0) {
    return c.json({
      exists: false,
      dataset,
      size,
      files: [],
    })
  }

  return c.json({
    exists: true,
    dataset,
    size,
    files: files.objects.map((obj) => ({
      name: obj.key.split('/').pop(),
      size: obj.size,
      key: obj.key,
      uploaded: obj.uploaded,
    })),
  })
})

// Delete a staged dataset
app.delete('/stage/:dataset/:size', async (c) => {
  const dataset = c.req.param('dataset') as DatasetType
  const size = c.req.param('size') as SizeOption

  // Validate inputs
  if (!DATASET_CONFIGS[dataset]) {
    return c.json({ error: `Invalid dataset: ${dataset}` }, 400)
  }
  if (!VALID_SIZES.includes(size)) {
    return c.json({ error: `Invalid size: ${size}` }, 400)
  }

  const prefix = `oltp/${dataset}/${size}/`
  const files = await c.env.DATASETS.list({ prefix })

  let deleted = 0
  for (const obj of files.objects) {
    await c.env.DATASETS.delete(obj.key)
    deleted++
  }

  return c.json({
    success: true,
    dataset,
    size,
    deleted,
  })
})

// Stage all datasets of a specific size
app.post('/stage-all/:size', async (c) => {
  const size = c.req.param('size') as SizeOption

  if (!VALID_SIZES.includes(size)) {
    return c.json({ error: `Invalid size: ${size}` }, 400)
  }

  const results: StageResult[] = []
  const datasets: DatasetType[] = ['ecommerce', 'saas', 'social', 'iot']

  for (const dataset of datasets) {
    // Make internal request to stage endpoint
    const response = await app.fetch(
      new Request(`http://localhost/stage/${dataset}/${size}`, {
        method: 'POST',
      }),
      c.env
    )
    const result = await response.json() as StageResult
    results.push(result)
  }

  const successful = results.filter((r) => r.success).length
  const failed = results.filter((r) => !r.success).length

  return c.json({
    summary: {
      size,
      successful,
      failed,
      total: datasets.length,
    },
    results,
  })
})

export default app
