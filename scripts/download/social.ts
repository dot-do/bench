/**
 * Social Network Synthetic OLTP Dataset Generator
 *
 * Generates fake data for: users, posts, comments, likes, follows
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

const SIZE_CONFIGS: Record<SizeOption, { users: number; posts: number; comments: number; likes: number; follows: number }> = {
  '1mb': { users: 500, posts: 2000, comments: 3000, likes: 15000, follows: 5000 },
  '10mb': { users: 5000, posts: 20000, comments: 30000, likes: 150000, follows: 50000 },
  '100mb': { users: 50000, posts: 200000, comments: 300000, likes: 1500000, follows: 500000 },
  '1gb': { users: 500000, posts: 2000000, comments: 3000000, likes: 15000000, follows: 5000000 },
}

// Sample data
const FIRST_NAMES = ['James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda', 'William', 'Elizabeth', 'David', 'Barbara', 'Richard', 'Susan', 'Joseph', 'Jessica', 'Thomas', 'Sarah', 'Charles', 'Karen', 'Alex', 'Sam', 'Jordan', 'Taylor', 'Morgan']
const LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin']
const LOCATIONS = ['New York, NY', 'Los Angeles, CA', 'Chicago, IL', 'Houston, TX', 'Phoenix, AZ', 'London, UK', 'Toronto, CA', 'Sydney, AU', 'Berlin, DE', 'Tokyo, JP']
const POST_TYPES = ['text', 'image', 'video', 'link']
const VISIBILITY = ['public', 'followers', 'private']
const STATUSES = ['active', 'suspended', 'deactivated']

const POST_TEMPLATES = [
  'Just had an amazing day! #blessed',
  'Working on something exciting. Stay tuned!',
  'Can\'t believe it\'s already the weekend.',
  'Check out this view! Absolutely stunning.',
  'Great meeting with the team today.',
  'Learning something new every day.',
  'Coffee and code - perfect combination.',
  'Grateful for all the support!',
  'Big announcement coming soon...',
  'Had the best meal at this new restaurant!',
  'Sometimes you just need a break.',
  'Excited to share my latest project!',
  'Nature always has the best therapy.',
  'Celebrating small wins today.',
  'Throwback to this amazing trip.',
]

const COMMENT_TEMPLATES = [
  'Love this!',
  'So cool!',
  'Amazing!',
  'Great post!',
  'This is awesome!',
  'Couldn\'t agree more.',
  'Thanks for sharing!',
  'This made my day.',
  'Absolutely beautiful!',
  'Keep up the great work!',
  'Inspiring!',
  'Wow, just wow!',
  'Need more of this!',
  'So true!',
  'Congratulations!',
]

const BIOS = [
  'Living life to the fullest',
  'Coffee enthusiast | Travel lover',
  'Tech geek | Entrepreneur',
  'Artist | Dreamer | Creator',
  'Just here for the memes',
  'Photography | Nature | Adventure',
  'Software engineer by day, gamer by night',
  'Foodie exploring the world one bite at a time',
  'Minimalist | Reader | Thinker',
  'Building cool stuff',
]

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

function generateUsername(firstName: string, lastName: string, rng: () => number): string {
  const patterns = [
    `${firstName.toLowerCase()}${lastName.toLowerCase()}`,
    `${firstName.toLowerCase()}_${lastName.toLowerCase()}`,
    `${firstName.toLowerCase()}${Math.floor(rng() * 1000)}`,
    `the_${firstName.toLowerCase()}`,
    `${firstName.toLowerCase()}.${lastName.toLowerCase().slice(0, 1)}`,
  ]
  return pick(patterns, rng)
}

function generateUsers(count: number, rng: () => number): any[] {
  const users: any[] = []
  for (let i = 0; i < count; i++) {
    const firstName = pick(FIRST_NAMES, rng)
    const lastName = pick(LAST_NAMES, rng)
    const username = generateUsername(firstName, lastName, rng) + Math.floor(rng() * 10000)

    // Zipf-like distribution for follower counts (few influencers, many regular users)
    const followerTier = rng()
    const followerCount = followerTier < 0.7 ? Math.floor(rng() * 500) :
      followerTier < 0.9 ? Math.floor(rng() * 10000) :
        followerTier < 0.98 ? Math.floor(rng() * 100000) :
          Math.floor(rng() * 1000000)

    users.push({
      id: generateUuid(rng),
      username,
      email: `${username}@${pick(['gmail.com', 'yahoo.com', 'outlook.com', 'email.com'], rng)}`,
      display_name: `${firstName} ${lastName}`,
      bio: rng() > 0.3 ? pick(BIOS, rng) : null,
      avatar_url: rng() > 0.2 ? `https://avatars.example.com/${generateUuid(rng)}.jpg` : null,
      location: rng() > 0.4 ? pick(LOCATIONS, rng) : null,
      website: rng() > 0.7 ? `https://${username}.example.com` : null,
      is_verified: rng() < 0.05,
      is_private: rng() < 0.2,
      status: pick(STATUSES, rng),
      follower_count: followerCount,
      following_count: Math.floor(rng() * Math.min(5000, followerCount * 2 + 100)),
      post_count: Math.floor(rng() * 1000),
      created_at: generateTimestamp(rng, 2015, 2024),
      updated_at: generateTimestamp(rng, 2023, 2024),
      last_active_at: rng() > 0.1 ? generateTimestamp(rng, 2024, 2024) : null,
    })
  }
  return users
}

function generatePosts(count: number, userIds: string[], rng: () => number): any[] {
  const posts: any[] = []
  for (let i = 0; i < count; i++) {
    const type = pick(POST_TYPES, rng)
    const content = pick(POST_TEMPLATES, rng)

    // Zipf distribution for engagement (few viral posts, many regular posts)
    const viralTier = rng()
    const likeCount = viralTier < 0.8 ? Math.floor(rng() * 100) :
      viralTier < 0.95 ? Math.floor(rng() * 5000) :
        viralTier < 0.99 ? Math.floor(rng() * 50000) :
          Math.floor(rng() * 500000)

    posts.push({
      id: generateUuid(rng),
      user_id: pick(userIds, rng),
      content,
      type,
      visibility: pick(VISIBILITY, rng),
      like_count: likeCount,
      comment_count: Math.floor(likeCount * (0.05 + rng() * 0.15)),
      repost_count: Math.floor(likeCount * (0.01 + rng() * 0.05)),
      view_count: likeCount * (5 + Math.floor(rng() * 20)),
      is_pinned: rng() < 0.02,
      media: type === 'image' ? [{
        url: `https://media.example.com/${generateUuid(rng)}.jpg`,
        width: 1080,
        height: 1080,
        alt_text: 'Image',
      }] : type === 'video' ? [{
        url: `https://media.example.com/${generateUuid(rng)}.mp4`,
        duration_seconds: Math.floor(rng() * 300) + 5,
        thumbnail_url: `https://media.example.com/${generateUuid(rng)}_thumb.jpg`,
      }] : null,
      hashtags: content.match(/#\w+/g) || [],
      created_at: generateTimestamp(rng, 2020, 2024),
      updated_at: rng() > 0.9 ? generateTimestamp(rng, 2024, 2024) : null,
    })
  }
  return posts
}

function generateComments(count: number, postIds: string[], userIds: string[], rng: () => number): any[] {
  const comments: any[] = []
  for (let i = 0; i < count; i++) {
    comments.push({
      id: generateUuid(rng),
      post_id: pick(postIds, rng),
      user_id: pick(userIds, rng),
      parent_id: rng() < 0.2 && comments.length > 0 ? pick(comments.slice(-100), rng).id : null,
      content: pick(COMMENT_TEMPLATES, rng),
      like_count: Math.floor(rng() * 100),
      reply_count: Math.floor(rng() * 10),
      is_edited: rng() < 0.1,
      created_at: generateTimestamp(rng, 2020, 2024),
      updated_at: rng() > 0.9 ? generateTimestamp(rng, 2024, 2024) : null,
    })
  }
  return comments
}

function generateLikes(count: number, postIds: string[], userIds: string[], rng: () => number): any[] {
  const likes: any[] = []
  const seen = new Set<string>()

  for (let i = 0; i < count; i++) {
    let postId: string
    let userId: string
    let key: string

    // Avoid duplicates
    let attempts = 0
    do {
      postId = pick(postIds, rng)
      userId = pick(userIds, rng)
      key = `${postId}:${userId}`
      attempts++
    } while (seen.has(key) && attempts < 10)

    if (attempts >= 10) continue
    seen.add(key)

    likes.push({
      id: generateUuid(rng),
      post_id: postId,
      user_id: userId,
      created_at: generateTimestamp(rng, 2020, 2024),
    })
  }
  return likes
}

function generateFollows(count: number, userIds: string[], rng: () => number): any[] {
  const follows: any[] = []
  const seen = new Set<string>()

  for (let i = 0; i < count; i++) {
    let followerId: string
    let followingId: string
    let key: string

    // Avoid duplicates and self-follows
    let attempts = 0
    do {
      followerId = pick(userIds, rng)
      followingId = pick(userIds, rng)
      key = `${followerId}:${followingId}`
      attempts++
    } while ((seen.has(key) || followerId === followingId) && attempts < 10)

    if (attempts >= 10) continue
    seen.add(key)

    follows.push({
      id: generateUuid(rng),
      follower_id: followerId,
      following_id: followingId,
      status: rng() > 0.1 ? 'accepted' : rng() > 0.5 ? 'pending' : 'blocked',
      notifications_enabled: rng() > 0.3,
      created_at: generateTimestamp(rng, 2015, 2024),
    })
  }
  return follows
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
  const seed = 34567 // Fixed seed for reproducibility

  console.log(`Generating social network dataset (${size})...`)

  // Create output directory if it doesn't exist
  fs.mkdirSync(outputDir, { recursive: true })

  // Generate with deterministic seed
  const rng = createRng(seed)

  console.log(`  Generating ${config.users} users...`)
  const users = generateUsers(config.users, rng)
  writeJsonl(path.join(outputDir, 'users.jsonl'), users)

  const userIds = users.map(u => u.id)

  console.log(`  Generating ${config.posts} posts...`)
  const posts = generatePosts(config.posts, userIds, rng)
  writeJsonl(path.join(outputDir, 'posts.jsonl'), posts)

  const postIds = posts.map(p => p.id)

  console.log(`  Generating ${config.comments} comments...`)
  const comments = generateComments(config.comments, postIds, userIds, rng)
  writeJsonl(path.join(outputDir, 'comments.jsonl'), comments)

  console.log(`  Generating ${config.likes} likes...`)
  const likes = generateLikes(config.likes, postIds, userIds, rng)
  writeJsonl(path.join(outputDir, 'likes.jsonl'), likes)

  console.log(`  Generating ${config.follows} follows...`)
  const follows = generateFollows(config.follows, userIds, rng)
  writeJsonl(path.join(outputDir, 'follows.jsonl'), follows)

  console.log(`Social network dataset generated in ${outputDir}`)
}

// CLI support
if (typeof require !== 'undefined' && require.main === module) {
  const args = process.argv.slice(2)
  const outputDir = args[0] || './data/social'
  const size = (args[1] as SizeOption) || '1mb'
  generate(outputDir, { size }).catch(console.error)
}
