/**
 * SaaS Multi-Tenant Synthetic OLTP Dataset Generator
 *
 * Generates fake data for: orgs, users, workspaces, documents
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

const SIZE_CONFIGS: Record<SizeOption, { orgs: number; users: number; workspaces: number; documents: number }> = {
  '1mb': { orgs: 10, users: 100, workspaces: 50, documents: 500 },
  '10mb': { orgs: 50, users: 1000, workspaces: 400, documents: 5000 },
  '100mb': { orgs: 200, users: 10000, workspaces: 3000, documents: 50000 },
  '1gb': { orgs: 1000, users: 100000, workspaces: 25000, documents: 500000 },
}

// Sample data
const FIRST_NAMES = ['James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda', 'William', 'Elizabeth', 'David', 'Barbara', 'Richard', 'Susan', 'Joseph', 'Jessica', 'Thomas', 'Sarah', 'Charles', 'Karen']
const LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin']
const COMPANY_SUFFIXES = ['Inc', 'LLC', 'Corp', 'Co', 'Ltd', 'Group', 'Solutions', 'Technologies', 'Systems', 'Labs']
const COMPANY_PREFIXES = ['Tech', 'Global', 'Digital', 'Smart', 'Cloud', 'Data', 'Cyber', 'Net', 'Web', 'App']
const PLANS = ['free', 'starter', 'professional', 'enterprise']
const STATUSES = ['active', 'trial', 'suspended', 'cancelled']
const USER_ROLES = ['owner', 'admin', 'manager', 'member', 'viewer']
const USER_STATUSES = ['active', 'pending', 'suspended', 'deactivated']
const WORKSPACE_TYPES = ['project', 'team', 'personal', 'shared']
const WORKSPACE_VISIBILITY = ['private', 'team', 'internal', 'public']
const DOC_TYPES = ['document', 'spreadsheet', 'presentation', 'note', 'wiki', 'template']
const DOC_STATUSES = ['draft', 'published', 'archived', 'deleted']

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

function generateSlug(name: string, rng: () => number): string {
  return name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '') + '-' + Math.floor(rng() * 10000)
}

function generateEmail(firstName: string, lastName: string, domain: string, rng: () => number): string {
  const num = Math.floor(rng() * 100)
  return `${firstName.toLowerCase()}.${lastName.toLowerCase()}${num > 50 ? num : ''}@${domain}`
}

function generateOrgs(count: number, rng: () => number): any[] {
  const orgs: any[] = []
  for (let i = 0; i < count; i++) {
    const name = `${pick(COMPANY_PREFIXES, rng)} ${pick(COMPANY_SUFFIXES, rng)}`
    const plan = pick(PLANS, rng)
    const maxUsers = plan === 'free' ? 5 : plan === 'starter' ? 10 : plan === 'professional' ? 50 : 1000
    const maxWorkspaces = plan === 'free' ? 3 : plan === 'starter' ? 10 : plan === 'professional' ? 50 : 1000

    orgs.push({
      id: generateUuid(rng),
      name,
      slug: generateSlug(name, rng),
      domain: `${name.toLowerCase().replace(/\s+/g, '')}.com`,
      plan,
      status: pick(STATUSES, rng),
      max_users: maxUsers,
      max_workspaces: maxWorkspaces,
      storage_limit_mb: plan === 'free' ? 500 : plan === 'starter' ? 5000 : plan === 'professional' ? 50000 : 500000,
      storage_used_mb: Math.floor(rng() * 5000),
      billing_email: `billing@${name.toLowerCase().replace(/\s+/g, '')}.com`,
      settings: {
        sso_enabled: plan === 'enterprise' && rng() > 0.5,
        two_factor_required: rng() > 0.7,
        allowed_domains: [],
      },
      created_at: generateTimestamp(rng, 2020, 2024),
      updated_at: generateTimestamp(rng, 2023, 2024),
    })
  }
  return orgs
}

function generateUsers(count: number, orgIds: string[], orgDomains: Map<string, string>, rng: () => number): any[] {
  const users: any[] = []
  for (let i = 0; i < count; i++) {
    const firstName = pick(FIRST_NAMES, rng)
    const lastName = pick(LAST_NAMES, rng)
    const orgId = pick(orgIds, rng)
    const domain = orgDomains.get(orgId) || 'example.com'

    users.push({
      id: generateUuid(rng),
      org_id: orgId,
      email: generateEmail(firstName, lastName, domain, rng),
      first_name: firstName,
      last_name: lastName,
      display_name: `${firstName} ${lastName}`,
      avatar_url: rng() > 0.3 ? `https://avatars.example.com/${generateUuid(rng)}.jpg` : null,
      role: pick(USER_ROLES, rng),
      status: pick(USER_STATUSES, rng),
      timezone: pick(['UTC', 'America/New_York', 'America/Los_Angeles', 'Europe/London', 'Asia/Tokyo'], rng),
      locale: pick(['en-US', 'en-GB', 'es', 'fr', 'de', 'ja'], rng),
      mfa_enabled: rng() > 0.7,
      last_login_at: rng() > 0.2 ? generateTimestamp(rng, 2024, 2024) : null,
      preferences: {
        theme: pick(['light', 'dark', 'system'], rng),
        notifications_email: rng() > 0.3,
        notifications_push: rng() > 0.5,
      },
      created_at: generateTimestamp(rng, 2020, 2024),
      updated_at: generateTimestamp(rng, 2023, 2024),
    })
  }
  return users
}

function generateWorkspaces(count: number, orgIds: string[], usersByOrg: Map<string, string[]>, rng: () => number): any[] {
  const workspaces: any[] = []
  const WORKSPACE_NAMES = ['Engineering', 'Marketing', 'Sales', 'Product', 'Design', 'Operations', 'Finance', 'HR', 'Legal', 'Support']
  const PROJECT_NAMES = ['Q1 Launch', 'Website Redesign', 'Mobile App', 'API v2', 'Infrastructure', 'Analytics', 'Customer Portal', 'Admin Dashboard']

  for (let i = 0; i < count; i++) {
    const orgId = pick(orgIds, rng)
    const orgUsers = usersByOrg.get(orgId) || []
    const ownerId = orgUsers.length > 0 ? pick(orgUsers, rng) : generateUuid(rng)
    const type = pick(WORKSPACE_TYPES, rng)
    const name = type === 'project' ? pick(PROJECT_NAMES, rng) : pick(WORKSPACE_NAMES, rng)

    workspaces.push({
      id: generateUuid(rng),
      org_id: orgId,
      name: `${name} ${Math.floor(rng() * 100)}`,
      slug: generateSlug(name, rng),
      description: `Workspace for ${name.toLowerCase()} related work and collaboration.`,
      type,
      visibility: pick(WORKSPACE_VISIBILITY, rng),
      owner_id: ownerId,
      member_count: Math.floor(rng() * 20) + 1,
      document_count: Math.floor(rng() * 100),
      settings: {
        default_permissions: pick(['view', 'edit', 'admin'], rng),
        allow_guest_access: rng() > 0.8,
      },
      created_at: generateTimestamp(rng, 2021, 2024),
      updated_at: generateTimestamp(rng, 2023, 2024),
    })
  }
  return workspaces
}

function generateDocuments(count: number, workspaceIds: string[], userIds: string[], rng: () => number): any[] {
  const documents: any[] = []
  const DOC_TITLES = [
    'Project Proposal', 'Meeting Notes', 'Technical Spec', 'User Guide', 'API Documentation',
    'Budget Report', 'Roadmap', 'Strategy Document', 'Process Flow', 'Requirements Doc',
    'Release Notes', 'FAQ', 'Onboarding Guide', 'Best Practices', 'Architecture Overview',
  ]

  for (let i = 0; i < count; i++) {
    const type = pick(DOC_TYPES, rng)
    const title = `${pick(DOC_TITLES, rng)} ${Math.floor(rng() * 1000)}`
    const version = Math.floor(rng() * 20) + 1

    documents.push({
      id: generateUuid(rng),
      workspace_id: pick(workspaceIds, rng),
      title,
      slug: generateSlug(title, rng),
      type,
      status: pick(DOC_STATUSES, rng),
      content: `# ${title}\n\nThis is a sample ${type} with placeholder content. It contains important information about the topic.\n\n## Overview\n\nLorem ipsum dolor sit amet, consectetur adipiscing elit.\n\n## Details\n\nMore detailed content would go here.`,
      content_preview: `This is a sample ${type} with placeholder content...`,
      word_count: Math.floor(rng() * 5000) + 100,
      version,
      is_template: rng() < 0.05,
      is_pinned: rng() < 0.1,
      created_by: pick(userIds, rng),
      last_edited_by: pick(userIds, rng),
      view_count: Math.floor(rng() * 1000),
      edit_count: version,
      comment_count: Math.floor(rng() * 50),
      permissions: {
        public_access: pick(['none', 'view', 'comment', 'edit'], rng),
      },
      created_at: generateTimestamp(rng, 2022, 2024),
      updated_at: generateTimestamp(rng, 2024, 2024),
      published_at: rng() > 0.4 ? generateTimestamp(rng, 2024, 2024) : null,
    })
  }
  return documents
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
  const seed = 23456 // Fixed seed for reproducibility

  console.log(`Generating SaaS dataset (${size})...`)

  // Create output directory if it doesn't exist
  fs.mkdirSync(outputDir, { recursive: true })

  // Generate with deterministic seed
  const rng = createRng(seed)

  console.log(`  Generating ${config.orgs} orgs...`)
  const orgs = generateOrgs(config.orgs, rng)
  writeJsonl(path.join(outputDir, 'orgs.jsonl'), orgs)

  const orgIds = orgs.map(o => o.id)
  const orgDomains = new Map(orgs.map(o => [o.id, o.domain]))

  console.log(`  Generating ${config.users} users...`)
  const users = generateUsers(config.users, orgIds, orgDomains, rng)
  writeJsonl(path.join(outputDir, 'users.jsonl'), users)

  const userIds = users.map(u => u.id)
  const usersByOrg = new Map<string, string[]>()
  for (const user of users) {
    const orgUsers = usersByOrg.get(user.org_id) || []
    orgUsers.push(user.id)
    usersByOrg.set(user.org_id, orgUsers)
  }

  console.log(`  Generating ${config.workspaces} workspaces...`)
  const workspaces = generateWorkspaces(config.workspaces, orgIds, usersByOrg, rng)
  writeJsonl(path.join(outputDir, 'workspaces.jsonl'), workspaces)

  const workspaceIds = workspaces.map(w => w.id)

  console.log(`  Generating ${config.documents} documents...`)
  const documents = generateDocuments(config.documents, workspaceIds, userIds, rng)
  writeJsonl(path.join(outputDir, 'documents.jsonl'), documents)

  console.log(`SaaS dataset generated in ${outputDir}`)
}

// CLI support
if (typeof require !== 'undefined' && require.main === module) {
  const args = process.argv.slice(2)
  const outputDir = args[0] || './data/saas'
  const size = (args[1] as SizeOption) || '1mb'
  generate(outputDir, { size }).catch(console.error)
}
