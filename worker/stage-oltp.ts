/**
 * Stage OLTP Datasets Worker
 *
 * A Cloudflare Worker that uses Sandbox SDK to generate and stage OLTP datasets.
 * Spins up sandbox containers to run dataset generators and stores output in R2.
 *
 * Endpoint: POST /stage/{dataset}/{size}
 * - dataset: ecommerce | saas | social | iot
 * - size: 1mb | 10mb | 100mb | 1gb
 *
 * @see scripts/download/ecommerce.ts - E-commerce dataset generator
 * @see scripts/download/saas.ts - SaaS multi-tenant dataset generator
 * @see scripts/download/social.ts - Social network dataset generator
 * @see scripts/download/iot.ts - IoT timeseries dataset generator
 */

import { Hono } from 'hono'

// Environment bindings
interface Env {
  // R2 bucket for storing generated datasets
  DATASETS: R2Bucket
  // Sandbox API token for authentication
  DO_TOKEN: string
  // Sandbox API base URL (optional, defaults to api.do)
  SANDBOX_API_URL?: string
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
   * Create a new sandbox environment
   */
  async create(options: {
    name: string
    runtime: 'node'
    memory?: number
    timeout?: number
    networkAccess?: boolean
  }): Promise<{ id: string; name: string; status: string }> {
    return this.request('POST', '/sandboxs', {
      name: options.name,
      type: 'vm',
      runtime: options.runtime,
      memory: options.memory ?? 512,
      timeout: options.timeout ?? 300000, // 5 minutes default
      networkAccess: options.networkAccess ?? false,
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
      language: options.language ?? 'typescript',
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
   * Write a file in the sandbox
   */
  async writeFile(
    sandboxId: string,
    path: string,
    content: string
  ): Promise<{ path: string; size: number }> {
    return this.request('POST', `/sandboxs/${sandboxId}/write`, {
      path,
      content,
    })
  }

  /**
   * Read a file from the sandbox
   */
  async readFile(sandboxId: string, path: string): Promise<{ content: string; size: number }> {
    return this.request('POST', `/sandboxs/${sandboxId}/read`, {
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
 * Generator script template for sandbox execution
 * This wraps the existing generator functions to run in the sandbox
 */
function getGeneratorScript(dataset: DatasetType, size: SizeOption): string {
  // Base generator code that's shared across all dataset types
  const baseCode = `
// Seeded random number generator (Mulberry32)
function createRng(seed) {
  return function () {
    let t = (seed += 0x6d2b79f5);
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function pick(arr, rng) {
  return arr[Math.floor(rng() * arr.length)];
}

function generateUuid(rng) {
  const hex = '0123456789abcdef';
  let uuid = '';
  for (let i = 0; i < 36; i++) {
    if (i === 8 || i === 13 || i === 18 || i === 23) {
      uuid += '-';
    } else if (i === 14) {
      uuid += '4';
    } else if (i === 19) {
      uuid += hex[Math.floor(rng() * 4) + 8];
    } else {
      uuid += hex[Math.floor(rng() * 16)];
    }
  }
  return uuid;
}

function generateTimestamp(rng, startYear, endYear) {
  const start = new Date(startYear, 0, 1).getTime();
  const end = new Date(endYear, 11, 31).getTime();
  const timestamp = new Date(start + rng() * (end - start));
  return timestamp.toISOString();
}

const fs = require('fs');
const path = require('path');

function writeJsonl(filePath, data) {
  const stream = fs.createWriteStream(filePath);
  for (const item of data) {
    stream.write(JSON.stringify(item) + '\\n');
  }
  stream.end();
}

const OUTPUT_DIR = '/output';
fs.mkdirSync(OUTPUT_DIR, { recursive: true });
`

  // Dataset-specific generators
  const generators: Record<DatasetType, string> = {
    ecommerce: getEcommerceGenerator(size),
    saas: getSaasGenerator(size),
    social: getSocialGenerator(size),
    iot: getIotGenerator(size),
  }

  return baseCode + generators[dataset]
}

function getEcommerceGenerator(size: SizeOption): string {
  const configs: Record<SizeOption, string> = {
    '1mb': '{ customers: 200, products: 500, orders: 1000, reviews: 800 }',
    '10mb': '{ customers: 2000, products: 5000, orders: 10000, reviews: 8000 }',
    '100mb': '{ customers: 20000, products: 50000, orders: 100000, reviews: 80000 }',
    '1gb': '{ customers: 200000, products: 500000, orders: 1000000, reviews: 800000 }',
  }

  return `
const CONFIG = ${configs[size]};
const SEED = 12345;
const rng = createRng(SEED);

const FIRST_NAMES = ['James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda', 'William', 'Elizabeth', 'David', 'Barbara', 'Richard', 'Susan', 'Joseph', 'Jessica', 'Thomas', 'Sarah', 'Charles', 'Karen'];
const LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin'];
const CITIES = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose'];
const STATES = ['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX', 'CA', 'TX', 'CA'];
const PRODUCT_ADJECTIVES = ['Premium', 'Classic', 'Modern', 'Deluxe', 'Essential', 'Professional', 'Ultimate', 'Elite', 'Basic', 'Advanced'];
const PRODUCT_NOUNS = ['Widget', 'Gadget', 'Device', 'Tool', 'System', 'Kit', 'Set', 'Pack', 'Bundle', 'Collection'];
const CATEGORIES = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys', 'Health', 'Beauty', 'Automotive', 'Food'];
const BRANDS = ['Acme', 'TechCorp', 'GlobalBrand', 'PrimeLine', 'ValueMax', 'QualityFirst', 'ProGrade', 'EcoSmart', 'FastTrack', 'SureGrip'];
const ORDER_STATUSES = ['pending', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled'];
const PAYMENT_STATUSES = ['pending', 'authorized', 'captured', 'failed', 'refunded'];
const TIERS = ['standard', 'premium', 'vip'];
const REVIEW_TITLES = ['Great product!', 'Exactly what I needed', 'Good value', 'Decent quality', 'Not bad', 'Could be better', 'Disappointed', 'Amazing!', 'Highly recommend', 'Works as expected'];

function generateEmail(firstName, lastName, rng) {
  const domains = ['gmail.com', 'yahoo.com', 'outlook.com', 'email.com', 'mail.com'];
  const num = Math.floor(rng() * 1000);
  return \`\${firstName.toLowerCase()}.\${lastName.toLowerCase()}\${num}@\${pick(domains, rng)}\`;
}

// Generate customers
console.log('Generating customers...');
const customers = [];
for (let i = 0; i < CONFIG.customers; i++) {
  const firstName = pick(FIRST_NAMES, rng);
  const lastName = pick(LAST_NAMES, rng);
  const cityIdx = Math.floor(rng() * CITIES.length);
  customers.push({
    id: generateUuid(rng),
    email: generateEmail(firstName, lastName, rng),
    first_name: firstName,
    last_name: lastName,
    phone: '+1' + Math.floor(rng() * 9000000000 + 1000000000),
    tier: pick(TIERS, rng),
    total_spent: Math.round(rng() * 50000 * 100) / 100,
    order_count: Math.floor(rng() * 100),
    address: {
      street: Math.floor(rng() * 9999 + 1) + ' ' + pick(['Main', 'Oak', 'Maple', 'Cedar', 'Pine'], rng) + ' ' + pick(['St', 'Ave', 'Blvd', 'Rd'], rng),
      city: CITIES[cityIdx],
      state: STATES[cityIdx],
      postal_code: String(Math.floor(rng() * 90000 + 10000)),
      country: 'US',
    },
    created_at: generateTimestamp(rng, 2020, 2024),
    updated_at: generateTimestamp(rng, 2023, 2024),
  });
}
writeJsonl(path.join(OUTPUT_DIR, 'customers.jsonl'), customers);

// Generate products
console.log('Generating products...');
const products = [];
for (let i = 0; i < CONFIG.products; i++) {
  const price = Math.round((rng() * 999 + 1) * 100) / 100;
  products.push({
    id: generateUuid(rng),
    sku: 'SKU-' + String(i + 1).padStart(8, '0'),
    name: pick(PRODUCT_ADJECTIVES, rng) + ' ' + pick(PRODUCT_NOUNS, rng),
    description: 'High-quality ' + pick(PRODUCT_NOUNS, rng).toLowerCase() + ' for everyday use.',
    category: pick(CATEGORIES, rng),
    brand: pick(BRANDS, rng),
    price: price,
    cost: Math.round(price * (0.3 + rng() * 0.4) * 100) / 100,
    stock_quantity: Math.floor(rng() * 1000),
    is_active: rng() > 0.1,
    rating_avg: Math.round((1 + rng() * 4) * 100) / 100,
    rating_count: Math.floor(rng() * 500),
    created_at: generateTimestamp(rng, 2020, 2024),
    updated_at: generateTimestamp(rng, 2023, 2024),
  });
}
writeJsonl(path.join(OUTPUT_DIR, 'products.jsonl'), products);

const customerIds = customers.map(c => c.id);
const productIds = products.map(p => p.id);

// Generate orders
console.log('Generating orders...');
const orders = [];
for (let i = 0; i < CONFIG.orders; i++) {
  const itemCount = Math.floor(rng() * 5) + 1;
  const items = [];
  let subtotal = 0;
  for (let j = 0; j < itemCount; j++) {
    const quantity = Math.floor(rng() * 3) + 1;
    const unitPrice = Math.round((rng() * 500 + 10) * 100) / 100;
    const itemTotal = Math.round(quantity * unitPrice * 100) / 100;
    subtotal += itemTotal;
    items.push({
      product_id: pick(productIds, rng),
      quantity,
      unit_price: unitPrice,
      total: itemTotal,
    });
  }
  const discount = Math.round(subtotal * rng() * 0.2 * 100) / 100;
  const tax = Math.round((subtotal - discount) * 0.08 * 100) / 100;
  const shipping = Math.round(rng() * 20 * 100) / 100;
  const total = Math.round((subtotal - discount + tax + shipping) * 100) / 100;

  orders.push({
    id: generateUuid(rng),
    order_number: 'ORD-' + String(i + 1).padStart(10, '0'),
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
  });
}
writeJsonl(path.join(OUTPUT_DIR, 'orders.jsonl'), orders);

// Generate reviews
console.log('Generating reviews...');
const reviews = [];
for (let i = 0; i < CONFIG.reviews; i++) {
  const rating = Math.floor(rng() * 5) + 1;
  reviews.push({
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
  });
}
writeJsonl(path.join(OUTPUT_DIR, 'reviews.jsonl'), reviews);

console.log('Generation complete!');
console.log(JSON.stringify({ tables: ['customers', 'products', 'orders', 'reviews'] }));
`
}

function getSaasGenerator(size: SizeOption): string {
  const configs: Record<SizeOption, string> = {
    '1mb': '{ orgs: 10, users: 100, workspaces: 50, documents: 500 }',
    '10mb': '{ orgs: 50, users: 1000, workspaces: 400, documents: 5000 }',
    '100mb': '{ orgs: 200, users: 10000, workspaces: 3000, documents: 50000 }',
    '1gb': '{ orgs: 1000, users: 100000, workspaces: 25000, documents: 500000 }',
  }

  return `
const CONFIG = ${configs[size]};
const SEED = 23456;
const rng = createRng(SEED);

const FIRST_NAMES = ['James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda', 'William', 'Elizabeth'];
const LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez'];
const COMPANY_SUFFIXES = ['Inc', 'LLC', 'Corp', 'Co', 'Ltd', 'Group', 'Solutions', 'Technologies', 'Systems', 'Labs'];
const COMPANY_PREFIXES = ['Tech', 'Global', 'Digital', 'Smart', 'Cloud', 'Data', 'Cyber', 'Net', 'Web', 'App'];
const PLANS = ['free', 'starter', 'professional', 'enterprise'];
const STATUSES = ['active', 'trial', 'suspended', 'cancelled'];
const USER_ROLES = ['owner', 'admin', 'manager', 'member', 'viewer'];
const DOC_TYPES = ['document', 'spreadsheet', 'presentation', 'note', 'wiki', 'template'];

function generateSlug(name, rng) {
  return name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '') + '-' + Math.floor(rng() * 10000);
}

// Generate orgs
console.log('Generating orgs...');
const orgs = [];
for (let i = 0; i < CONFIG.orgs; i++) {
  const name = pick(COMPANY_PREFIXES, rng) + ' ' + pick(COMPANY_SUFFIXES, rng);
  const plan = pick(PLANS, rng);
  orgs.push({
    id: generateUuid(rng),
    name,
    slug: generateSlug(name, rng),
    domain: name.toLowerCase().replace(/\\s+/g, '') + '.com',
    plan,
    status: pick(STATUSES, rng),
    max_users: plan === 'free' ? 5 : plan === 'starter' ? 10 : plan === 'professional' ? 50 : 1000,
    created_at: generateTimestamp(rng, 2020, 2024),
    updated_at: generateTimestamp(rng, 2023, 2024),
  });
}
writeJsonl(path.join(OUTPUT_DIR, 'orgs.jsonl'), orgs);

const orgIds = orgs.map(o => o.id);
const orgDomains = new Map(orgs.map(o => [o.id, o.domain]));

// Generate users
console.log('Generating users...');
const users = [];
for (let i = 0; i < CONFIG.users; i++) {
  const firstName = pick(FIRST_NAMES, rng);
  const lastName = pick(LAST_NAMES, rng);
  const orgId = pick(orgIds, rng);
  const domain = orgDomains.get(orgId) || 'example.com';
  users.push({
    id: generateUuid(rng),
    org_id: orgId,
    email: firstName.toLowerCase() + '.' + lastName.toLowerCase() + Math.floor(rng() * 100) + '@' + domain,
    first_name: firstName,
    last_name: lastName,
    display_name: firstName + ' ' + lastName,
    role: pick(USER_ROLES, rng),
    status: pick(['active', 'pending', 'suspended'], rng),
    created_at: generateTimestamp(rng, 2020, 2024),
    updated_at: generateTimestamp(rng, 2023, 2024),
  });
}
writeJsonl(path.join(OUTPUT_DIR, 'users.jsonl'), users);

const userIds = users.map(u => u.id);
const usersByOrg = new Map();
for (const user of users) {
  const orgUsers = usersByOrg.get(user.org_id) || [];
  orgUsers.push(user.id);
  usersByOrg.set(user.org_id, orgUsers);
}

// Generate workspaces
console.log('Generating workspaces...');
const workspaces = [];
const WORKSPACE_NAMES = ['Engineering', 'Marketing', 'Sales', 'Product', 'Design', 'Operations'];
for (let i = 0; i < CONFIG.workspaces; i++) {
  const orgId = pick(orgIds, rng);
  const orgUsers = usersByOrg.get(orgId) || [];
  const ownerId = orgUsers.length > 0 ? pick(orgUsers, rng) : generateUuid(rng);
  const name = pick(WORKSPACE_NAMES, rng);
  workspaces.push({
    id: generateUuid(rng),
    org_id: orgId,
    name: name + ' ' + Math.floor(rng() * 100),
    slug: generateSlug(name, rng),
    description: 'Workspace for ' + name.toLowerCase() + ' work.',
    type: pick(['project', 'team', 'personal', 'shared'], rng),
    visibility: pick(['private', 'team', 'internal', 'public'], rng),
    owner_id: ownerId,
    created_at: generateTimestamp(rng, 2021, 2024),
    updated_at: generateTimestamp(rng, 2023, 2024),
  });
}
writeJsonl(path.join(OUTPUT_DIR, 'workspaces.jsonl'), workspaces);

const workspaceIds = workspaces.map(w => w.id);

// Generate documents
console.log('Generating documents...');
const documents = [];
const DOC_TITLES = ['Project Proposal', 'Meeting Notes', 'Technical Spec', 'User Guide', 'API Documentation', 'Budget Report', 'Roadmap'];
for (let i = 0; i < CONFIG.documents; i++) {
  const type = pick(DOC_TYPES, rng);
  const title = pick(DOC_TITLES, rng) + ' ' + Math.floor(rng() * 1000);
  documents.push({
    id: generateUuid(rng),
    workspace_id: pick(workspaceIds, rng),
    title,
    slug: generateSlug(title, rng),
    type,
    status: pick(['draft', 'published', 'archived'], rng),
    content: '# ' + title + '\\n\\nThis is a sample ' + type + ' with placeholder content.',
    word_count: Math.floor(rng() * 5000) + 100,
    version: Math.floor(rng() * 20) + 1,
    created_by: pick(userIds, rng),
    last_edited_by: pick(userIds, rng),
    created_at: generateTimestamp(rng, 2022, 2024),
    updated_at: generateTimestamp(rng, 2024, 2024),
  });
}
writeJsonl(path.join(OUTPUT_DIR, 'documents.jsonl'), documents);

console.log('Generation complete!');
console.log(JSON.stringify({ tables: ['orgs', 'users', 'workspaces', 'documents'] }));
`
}

function getSocialGenerator(size: SizeOption): string {
  const configs: Record<SizeOption, string> = {
    '1mb': '{ users: 500, posts: 2000, comments: 3000, likes: 15000, follows: 5000 }',
    '10mb': '{ users: 5000, posts: 20000, comments: 30000, likes: 150000, follows: 50000 }',
    '100mb': '{ users: 50000, posts: 200000, comments: 300000, likes: 1500000, follows: 500000 }',
    '1gb': '{ users: 500000, posts: 2000000, comments: 3000000, likes: 15000000, follows: 5000000 }',
  }

  return `
const CONFIG = ${configs[size]};
const SEED = 34567;
const rng = createRng(SEED);

const FIRST_NAMES = ['James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda', 'William', 'Elizabeth', 'Alex', 'Sam', 'Jordan', 'Taylor', 'Morgan'];
const LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez'];
const LOCATIONS = ['New York, NY', 'Los Angeles, CA', 'Chicago, IL', 'Houston, TX', 'Phoenix, AZ', 'London, UK', 'Toronto, CA', 'Sydney, AU'];
const POST_TYPES = ['text', 'image', 'video', 'link'];
const BIOS = ['Living life to the fullest', 'Coffee enthusiast | Travel lover', 'Tech geek | Entrepreneur', 'Artist | Dreamer | Creator'];
const POST_TEMPLATES = ['Just had an amazing day!', 'Working on something exciting.', 'Check out this view!', 'Great meeting today.', 'Learning something new.'];
const COMMENT_TEMPLATES = ['Love this!', 'So cool!', 'Amazing!', 'Great post!', 'This is awesome!'];

function generateUsername(firstName, lastName, rng) {
  return firstName.toLowerCase() + lastName.toLowerCase() + Math.floor(rng() * 10000);
}

// Generate users
console.log('Generating users...');
const users = [];
for (let i = 0; i < CONFIG.users; i++) {
  const firstName = pick(FIRST_NAMES, rng);
  const lastName = pick(LAST_NAMES, rng);
  const username = generateUsername(firstName, lastName, rng);
  const followerTier = rng();
  const followerCount = followerTier < 0.7 ? Math.floor(rng() * 500) :
    followerTier < 0.9 ? Math.floor(rng() * 10000) :
    followerTier < 0.98 ? Math.floor(rng() * 100000) :
    Math.floor(rng() * 1000000);
  users.push({
    id: generateUuid(rng),
    username,
    email: username + '@' + pick(['gmail.com', 'yahoo.com', 'outlook.com'], rng),
    display_name: firstName + ' ' + lastName,
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
  });
}
writeJsonl(path.join(OUTPUT_DIR, 'users.jsonl'), users);

const userIds = users.map(u => u.id);

// Generate posts
console.log('Generating posts...');
const posts = [];
for (let i = 0; i < CONFIG.posts; i++) {
  const type = pick(POST_TYPES, rng);
  const viralTier = rng();
  const likeCount = viralTier < 0.8 ? Math.floor(rng() * 100) :
    viralTier < 0.95 ? Math.floor(rng() * 5000) :
    Math.floor(rng() * 50000);
  posts.push({
    id: generateUuid(rng),
    user_id: pick(userIds, rng),
    content: pick(POST_TEMPLATES, rng),
    type,
    visibility: pick(['public', 'followers', 'private'], rng),
    like_count: likeCount,
    comment_count: Math.floor(likeCount * (0.05 + rng() * 0.15)),
    view_count: likeCount * (5 + Math.floor(rng() * 20)),
    created_at: generateTimestamp(rng, 2020, 2024),
    updated_at: rng() > 0.9 ? generateTimestamp(rng, 2024, 2024) : null,
  });
}
writeJsonl(path.join(OUTPUT_DIR, 'posts.jsonl'), posts);

const postIds = posts.map(p => p.id);

// Generate comments
console.log('Generating comments...');
const comments = [];
for (let i = 0; i < CONFIG.comments; i++) {
  comments.push({
    id: generateUuid(rng),
    post_id: pick(postIds, rng),
    user_id: pick(userIds, rng),
    parent_id: rng() < 0.2 && comments.length > 0 ? comments[Math.floor(rng() * Math.min(100, comments.length))].id : null,
    content: pick(COMMENT_TEMPLATES, rng),
    like_count: Math.floor(rng() * 100),
    created_at: generateTimestamp(rng, 2020, 2024),
  });
}
writeJsonl(path.join(OUTPUT_DIR, 'comments.jsonl'), comments);

// Generate likes
console.log('Generating likes...');
const likes = [];
const seenLikes = new Set();
for (let i = 0; i < CONFIG.likes; i++) {
  let postId, userId, key;
  let attempts = 0;
  do {
    postId = pick(postIds, rng);
    userId = pick(userIds, rng);
    key = postId + ':' + userId;
    attempts++;
  } while (seenLikes.has(key) && attempts < 10);
  if (attempts >= 10) continue;
  seenLikes.add(key);
  likes.push({
    id: generateUuid(rng),
    post_id: postId,
    user_id: userId,
    created_at: generateTimestamp(rng, 2020, 2024),
  });
}
writeJsonl(path.join(OUTPUT_DIR, 'likes.jsonl'), likes);

// Generate follows
console.log('Generating follows...');
const follows = [];
const seenFollows = new Set();
for (let i = 0; i < CONFIG.follows; i++) {
  let followerId, followingId, key;
  let attempts = 0;
  do {
    followerId = pick(userIds, rng);
    followingId = pick(userIds, rng);
    key = followerId + ':' + followingId;
    attempts++;
  } while ((seenFollows.has(key) || followerId === followingId) && attempts < 10);
  if (attempts >= 10) continue;
  seenFollows.add(key);
  follows.push({
    id: generateUuid(rng),
    follower_id: followerId,
    following_id: followingId,
    status: rng() > 0.1 ? 'accepted' : rng() > 0.5 ? 'pending' : 'blocked',
    created_at: generateTimestamp(rng, 2015, 2024),
  });
}
writeJsonl(path.join(OUTPUT_DIR, 'follows.jsonl'), follows);

console.log('Generation complete!');
console.log(JSON.stringify({ tables: ['users', 'posts', 'comments', 'likes', 'follows'] }));
`
}

function getIotGenerator(size: SizeOption): string {
  const configs: Record<SizeOption, string> = {
    '1mb': '{ devices: 50, sensors: 150, readings: 50000 }',
    '10mb': '{ devices: 500, sensors: 1500, readings: 500000 }',
    '100mb': '{ devices: 5000, sensors: 15000, readings: 5000000 }',
    '1gb': '{ devices: 50000, sensors: 150000, readings: 50000000 }',
  }

  return `
const CONFIG = ${configs[size]};
const SEED = 45678;
const rng = createRng(SEED);

const DEVICE_TYPES = ['temperature_sensor', 'humidity_sensor', 'pressure_sensor', 'motion_detector', 'smart_meter', 'air_quality'];
const DEVICE_CATEGORIES = ['sensor', 'actuator', 'gateway', 'controller', 'meter'];
const MANUFACTURERS = ['SensorCorp', 'IoTech', 'SmartSense', 'DataFlow', 'EdgeDevices', 'ConnectAll'];
const DEVICE_STATUSES = ['online', 'offline', 'maintenance', 'error', 'provisioning'];
const METRIC_NAMES = ['temperature', 'humidity', 'pressure', 'co2', 'pm25', 'voltage', 'current', 'power', 'flow_rate', 'vibration'];
const METRIC_UNITS = { temperature: 'celsius', humidity: 'percent', pressure: 'hPa', co2: 'ppm', pm25: 'ug/m3', voltage: 'volts', current: 'amps', power: 'watts', flow_rate: 'l/min', vibration: 'mm/s' };
const METRIC_RANGES = { temperature: [-20, 50], humidity: [0, 100], pressure: [950, 1050], co2: [300, 2000], pm25: [0, 500], voltage: [100, 250], current: [0, 50], power: [0, 10000], flow_rate: [0, 100], vibration: [0, 50] };
const BUILDING_NAMES = ['Building A', 'Building B', 'Main Office', 'Warehouse', 'Factory', 'Data Center'];

function generateSerialNumber(rng) {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let serial = '';
  for (let i = 0; i < 12; i++) {
    serial += chars[Math.floor(rng() * chars.length)];
  }
  return serial;
}

function generateMacAddress(rng) {
  const hex = '0123456789ABCDEF';
  const parts = [];
  for (let i = 0; i < 6; i++) {
    parts.push(hex[Math.floor(rng() * 16)] + hex[Math.floor(rng() * 16)]);
  }
  return parts.join(':');
}

// Generate devices
console.log('Generating devices...');
const devices = [];
for (let i = 0; i < CONFIG.devices; i++) {
  const deviceType = pick(DEVICE_TYPES, rng);
  const manufacturer = pick(MANUFACTURERS, rng);
  const status = pick(DEVICE_STATUSES, rng);
  devices.push({
    id: generateUuid(rng),
    serial_number: generateSerialNumber(rng),
    name: deviceType.replace(/_/g, ' ').replace(/\\b\\w/g, l => l.toUpperCase()) + ' ' + (i + 1),
    device_type: deviceType,
    category: pick(DEVICE_CATEGORIES, rng),
    manufacturer,
    model: manufacturer.slice(0, 3).toUpperCase() + '-' + (Math.floor(rng() * 9000) + 1000),
    firmware_version: Math.floor(rng() * 5 + 1) + '.' + Math.floor(rng() * 10) + '.' + Math.floor(rng() * 100),
    status,
    is_active: status !== 'maintenance' && rng() > 0.1,
    location: {
      name: pick(BUILDING_NAMES, rng) + ' - Zone ' + Math.floor(rng() * 100 + 1),
      latitude: 37.7749 + (rng() - 0.5) * 0.1,
      longitude: -122.4194 + (rng() - 0.5) * 0.1,
    },
    network: {
      ip_address: '192.168.' + Math.floor(rng() * 256) + '.' + Math.floor(rng() * 256),
      mac_address: generateMacAddress(rng),
    },
    battery_level: rng() > 0.3 ? Math.floor(rng() * 100) : null,
    registered_at: generateTimestamp(rng, 2020, 2024),
    last_seen_at: status === 'online' ? generateTimestamp(rng, 2024, 2024) : generateTimestamp(rng, 2023, 2024),
  });
}
writeJsonl(path.join(OUTPUT_DIR, 'devices.jsonl'), devices);

const deviceIds = devices.map(d => d.id);

// Generate sensors
console.log('Generating sensors...');
const sensors = [];
for (let i = 0; i < CONFIG.sensors; i++) {
  const metricName = pick(METRIC_NAMES, rng);
  const [minVal, maxVal] = METRIC_RANGES[metricName];
  sensors.push({
    id: generateUuid(rng),
    device_id: pick(deviceIds, rng),
    name: metricName.replace(/_/g, ' ').replace(/\\b\\w/g, l => l.toUpperCase()) + ' Sensor',
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
  });
}
writeJsonl(path.join(OUTPUT_DIR, 'sensors.jsonl'), sensors);

// Generate readings (in batches for large datasets)
console.log('Generating readings...');
const baseTime = new Date(2024, 0, 1).getTime();
const endTime = new Date(2024, 11, 31).getTime();
const timeSpan = endTime - baseTime;
const batchSize = 100000;
const readingsPath = path.join(OUTPUT_DIR, 'readings.jsonl');

if (CONFIG.readings <= batchSize) {
  const readings = [];
  for (let i = 0; i < CONFIG.readings; i++) {
    const sensor = pick(sensors, rng);
    const [minVal, maxVal] = METRIC_RANGES[sensor.metric_name] || [0, 100];
    const midpoint = (minVal + maxVal) / 2;
    const range = maxVal - minVal;
    const u1 = rng();
    const u2 = rng();
    const gaussian = Math.sqrt(-2 * Math.log(u1 + 0.0001)) * Math.cos(2 * Math.PI * u2);
    const value = Math.max(minVal, Math.min(maxVal, midpoint + gaussian * range * 0.15));
    readings.push({
      id: i + 1,
      sensor_id: sensor.id,
      device_id: sensor.device_id,
      metric_name: sensor.metric_name,
      timestamp: new Date(baseTime + rng() * timeSpan).toISOString(),
      value: Math.round(value * 1000) / 1000,
      unit: sensor.unit,
      quality: rng() < 0.92 ? 'good' : rng() < 0.97 ? 'uncertain' : 'bad',
    });
  }
  readings.sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime());
  writeJsonl(readingsPath, readings);
} else {
  const stream = fs.createWriteStream(readingsPath);
  let generated = 0;
  let batchNum = 0;
  while (generated < CONFIG.readings) {
    const batchCount = Math.min(batchSize, CONFIG.readings - generated);
    const readings = [];
    for (let i = 0; i < batchCount; i++) {
      const sensor = pick(sensors, rng);
      const [minVal, maxVal] = METRIC_RANGES[sensor.metric_name] || [0, 100];
      const midpoint = (minVal + maxVal) / 2;
      const range = maxVal - minVal;
      const u1 = rng();
      const u2 = rng();
      const gaussian = Math.sqrt(-2 * Math.log(u1 + 0.0001)) * Math.cos(2 * Math.PI * u2);
      const value = Math.max(minVal, Math.min(maxVal, midpoint + gaussian * range * 0.15));
      readings.push({
        id: generated + i + 1,
        sensor_id: sensor.id,
        device_id: sensor.device_id,
        metric_name: sensor.metric_name,
        timestamp: new Date(baseTime + rng() * timeSpan).toISOString(),
        value: Math.round(value * 1000) / 1000,
        unit: sensor.unit,
        quality: rng() < 0.92 ? 'good' : rng() < 0.97 ? 'uncertain' : 'bad',
      });
    }
    for (const reading of readings) {
      stream.write(JSON.stringify(reading) + '\\n');
    }
    generated += batchCount;
    batchNum++;
    if (batchNum % 10 === 0) {
      console.log('Generated ' + generated + ' / ' + CONFIG.readings + ' readings...');
    }
  }
  stream.end();
}

console.log('Generation complete!');
console.log(JSON.stringify({ tables: ['devices', 'sensors', 'readings'] }));
`
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

  // Create sandbox client
  const sandbox = new SandboxClient(
    c.env.DO_TOKEN,
    c.env.SANDBOX_API_URL || 'https://api.do'
  )

  let sandboxId: string | null = null

  try {
    // Create sandbox
    console.log(`Creating sandbox for ${dataset}/${size}...`)
    const created = await sandbox.create({
      name: `oltp-${dataset}-${size}-${Date.now()}`,
      runtime: 'node',
      memory: size === '1gb' ? 2048 : size === '100mb' ? 1024 : 512,
      timeout: size === '1gb' ? 600000 : size === '100mb' ? 300000 : 120000, // 10/5/2 minutes
    })
    sandboxId = created.id
    console.log(`Sandbox created: ${sandboxId}`)

    // Generate the dataset
    console.log(`Generating ${dataset} dataset (${size})...`)
    const script = getGeneratorScript(dataset, size)
    const result = await sandbox.execute(sandboxId, {
      code: script,
      language: 'javascript',
    })

    if (result.exitCode !== 0) {
      throw new Error(`Generator failed: ${result.output}`)
    }
    console.log(`Generation complete. Duration: ${result.duration}ms`)

    // Read and upload generated files
    const config = DATASET_CONFIGS[dataset]
    const uploadedFiles: Array<{ name: string; size: number; key: string }> = []

    for (const table of config.tables) {
      const filename = `${table}.jsonl`
      const filePath = `/output/${filename}`

      console.log(`Uploading ${filename}...`)

      // Read file from sandbox
      const file = await sandbox.readFile(sandboxId, filePath)

      // Upload to R2
      const r2Key = `oltp/${dataset}/${size}/${filename}`
      await c.env.DATASETS.put(r2Key, file.content, {
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
        size: file.size,
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
