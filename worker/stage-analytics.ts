/**
 * Analytics Dataset Staging Worker
 *
 * A Cloudflare Worker that generates synthetic analytics datasets directly in-Worker.
 * Uses deterministic seeding for reproducible data generation.
 * Generated JSONL files are stored in R2 for benchmarking.
 *
 * Supported Datasets:
 * - clickbench: Synthetic web analytics data (page views, sessions, user events)
 * - imdb: Synthetic movie/actor/ratings data
 *
 * Endpoint: POST /stage/{dataset}/{size}
 * - POST /stage/clickbench/1mb - Generate ~1MB of web analytics data
 * - POST /stage/imdb/10mb - Generate ~10MB of movie data
 *
 * Size options: 1mb, 10mb, 100mb, 1gb
 */

import { Hono } from 'hono'

// Environment bindings
interface Env {
  // R2 bucket for storing generated datasets
  ANALYTICS_BUCKET: R2Bucket
}

// Valid dataset types
type DatasetType = 'clickbench' | 'imdb'

// Valid size options
type SizeOption = '1mb' | '10mb' | '100mb' | '1gb'

// Dataset configuration
interface DatasetConfig {
  name: string
  description: string
  tables: string[]
}

const DATASET_CONFIGS: Record<DatasetType, DatasetConfig> = {
  clickbench: {
    name: 'ClickBench',
    description: 'Synthetic web analytics benchmark data (page views, sessions, events)',
    tables: ['hits'],
  },
  imdb: {
    name: 'IMDB',
    description: 'Synthetic movie/TV data (titles, names, ratings)',
    tables: ['title_basics', 'name_basics', 'title_ratings'],
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

function generateTimestamp(rng: () => number, startYear: number, endYear: number): string {
  const start = new Date(startYear, 0, 1).getTime()
  const end = new Date(endYear, 11, 31).getTime()
  const timestamp = new Date(start + rng() * (end - start))
  return timestamp.toISOString()
}

function generateUnixTimestamp(rng: () => number, startYear: number, endYear: number): number {
  const start = new Date(startYear, 0, 1).getTime()
  const end = new Date(endYear, 11, 31).getTime()
  return Math.floor((start + rng() * (end - start)) / 1000)
}

// =============================================================================
// ClickBench Dataset Generator
// Simulates web analytics data similar to the real ClickBench hits dataset
// =============================================================================

const CLICKBENCH_SEED = 56789

const CLICKBENCH_SIZE_CONFIGS: Record<SizeOption, { hits: number }> = {
  '1mb': { hits: 2000 },
  '10mb': { hits: 20000 },
  '100mb': { hits: 200000 },
  '1gb': { hits: 2000000 },
}

// ClickBench-style data arrays
const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/91.0.4472.124',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/91.0.4472.114',
  'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15',
  'Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15',
  'Mozilla/5.0 (Android 11; Mobile; rv:89.0) Gecko/89.0 Firefox/89.0',
]

const REFERER_DOMAINS = [
  'google.com', 'facebook.com', 'twitter.com', 'youtube.com', 'reddit.com',
  'linkedin.com', 'instagram.com', 'pinterest.com', 'bing.com', 'yahoo.com',
  'tiktok.com', 'amazon.com', 'wikipedia.org', 'baidu.com', 'yandex.ru',
]

const URL_PATHS = [
  '/', '/products', '/about', '/contact', '/blog', '/news', '/shop',
  '/category/electronics', '/category/clothing', '/category/home',
  '/product/123', '/product/456', '/product/789', '/cart', '/checkout',
  '/search', '/account', '/login', '/register', '/help', '/faq',
]

const SEARCH_PHRASES = [
  'best laptop 2024', 'cheap flights', 'weather today', 'news headlines',
  'recipe chicken', 'how to cook', 'buy shoes online', 'smartphone deals',
  'movie reviews', 'sports scores', 'stock market', 'travel destinations',
  '', '', '', '', // empty searches are common
]

const COUNTRIES = ['US', 'GB', 'DE', 'FR', 'CA', 'AU', 'JP', 'BR', 'IN', 'RU', 'CN', 'ES', 'IT', 'NL', 'PL']
const REGIONS = ['California', 'Texas', 'New York', 'Florida', 'Illinois', 'London', 'Bavaria', 'Paris', 'Ontario', 'Victoria']
const CITIES = ['Los Angeles', 'Houston', 'New York', 'Miami', 'Chicago', 'London', 'Munich', 'Paris', 'Toronto', 'Melbourne']

const OS_NAMES = ['Windows', 'Mac OS X', 'Linux', 'iOS', 'Android']
const BROWSERS = ['Chrome', 'Safari', 'Firefox', 'Edge', 'Opera']
const DEVICE_TYPES = ['desktop', 'mobile', 'tablet']

const TRAFFIC_SOURCES = ['organic', 'paid', 'direct', 'referral', 'social', 'email']
const EVENT_TYPES = ['pageview', 'click', 'scroll', 'form_submit', 'add_to_cart', 'purchase', 'video_play']

interface ClickBenchData {
  hits: string
}

function generateClickBenchData(size: SizeOption): ClickBenchData {
  const config = CLICKBENCH_SIZE_CONFIGS[size]
  const rng = createRng(CLICKBENCH_SEED)

  const hits: string[] = []

  // Generate user IDs for realistic session distribution
  const numUsers = Math.max(100, Math.floor(config.hits / 10))
  const userIds: number[] = []
  for (let i = 0; i < numUsers; i++) {
    userIds.push(Math.floor(rng() * 2147483647))
  }

  for (let i = 0; i < config.hits; i++) {
    const userId = pick(userIds, rng)
    const eventTime = generateUnixTimestamp(rng, 2020, 2024)
    const isNewUser = rng() < 0.3
    const countryCode = pick(COUNTRIES, rng)
    const regionIdx = Math.floor(rng() * REGIONS.length)
    const deviceType = pick(DEVICE_TYPES, rng)
    const isMobile = deviceType === 'mobile' || deviceType === 'tablet'

    // Generate realistic session data
    const sessionDuration = Math.floor(rng() * 1800) // 0-30 minutes
    const pageViews = Math.floor(rng() * 20) + 1
    const bounced = rng() < 0.4

    // Screen resolution based on device type
    const resolutions = isMobile
      ? [[375, 812], [414, 896], [390, 844], [360, 800]]
      : [[1920, 1080], [1366, 768], [1536, 864], [1440, 900], [2560, 1440]]
    const [resWidth, resHeight] = pick(resolutions, rng)

    // Generate click/scroll data
    const clientX = Math.floor(rng() * resWidth)
    const clientY = Math.floor(rng() * resHeight)

    const hit = {
      WatchID: Math.floor(rng() * Number.MAX_SAFE_INTEGER),
      JavaEnable: rng() > 0.1 ? 1 : 0,
      Title: `Page Title ${Math.floor(rng() * 1000)}`,
      GoodEvent: rng() > 0.02 ? 1 : 0, // 98% good events
      EventTime: eventTime,
      EventDate: Math.floor(eventTime / 86400) * 86400,
      CounterID: Math.floor(rng() * 10000),
      ClientIP: Math.floor(rng() * 4294967295),
      CounterClass: Math.floor(rng() * 5),
      OS: pick(OS_NAMES, rng),
      UserAgent: pick(USER_AGENTS, rng),
      URL: `https://example.com${pick(URL_PATHS, rng)}`,
      Referer: rng() > 0.3 ? `https://${pick(REFERER_DOMAINS, rng)}/` : '',
      URLDomain: 'example.com',
      RefererDomain: rng() > 0.3 ? pick(REFERER_DOMAINS, rng) : '',
      IsRefresh: rng() < 0.05 ? 1 : 0,
      IsLink: rng() < 0.3 ? 1 : 0,
      IsDownload: rng() < 0.02 ? 1 : 0,
      IsNotBounce: bounced ? 0 : 1,
      FUniqID: Math.floor(rng() * Number.MAX_SAFE_INTEGER),
      HID: Math.floor(rng() * Number.MAX_SAFE_INTEGER),
      IsOldCounter: rng() < 0.1 ? 1 : 0,
      IsEvent: pick(EVENT_TYPES, rng) !== 'pageview' ? 1 : 0,
      IsParameter: rng() < 0.2 ? 1 : 0,
      DontCountHits: rng() < 0.01 ? 1 : 0,
      WithHash: rng() < 0.1 ? 1 : 0,
      HitColor: pick(['R', 'G', 'B', 'W'], rng),
      UTCEventTime: eventTime,
      Age: Math.floor(rng() * 80) + 18,
      Sex: Math.floor(rng() * 3), // 0=unknown, 1=male, 2=female
      Income: Math.floor(rng() * 5),
      Interests: Math.floor(rng() * 1000),
      Robotness: rng() < 0.02 ? Math.floor(rng() * 100) : 0,
      GeneralInterests: Math.floor(rng() * 100),
      RemoteIP: Math.floor(rng() * 4294967295),
      RemoteIP6: '::ffff:' + [Math.floor(rng() * 256), Math.floor(rng() * 256), Math.floor(rng() * 256), Math.floor(rng() * 256)].join('.'),
      WindowName: Math.floor(rng() * 10),
      OpenerName: Math.floor(rng() * 10),
      HistoryLength: Math.floor(rng() * 20),
      BrowserLanguage: pick(['en', 'en-US', 'en-GB', 'de', 'fr', 'es', 'zh', 'ja', 'ru', 'pt'], rng),
      BrowserCountry: countryCode,
      SocialNetwork: pick(['', '', '', 'Facebook', 'Twitter', 'Instagram', 'LinkedIn'], rng),
      SocialAction: pick(['', '', '', 'like', 'share', 'comment'], rng),
      HTTPError: rng() < 0.01 ? pick([404, 500, 502, 503], rng) : 0,
      SendTiming: Math.floor(rng() * 1000),
      DNSTiming: Math.floor(rng() * 100),
      ConnectTiming: Math.floor(rng() * 200),
      ResponseStartTiming: Math.floor(rng() * 500),
      ResponseEndTiming: Math.floor(rng() * 2000),
      FetchTiming: Math.floor(rng() * 100),
      RedirectTiming: rng() < 0.2 ? Math.floor(rng() * 500) : 0,
      DOMInteractiveTiming: Math.floor(rng() * 3000),
      DOMContentLoadedTiming: Math.floor(rng() * 4000),
      DOMCompleteTiming: Math.floor(rng() * 5000),
      LoadEventStartTiming: Math.floor(rng() * 5500),
      LoadEventEndTiming: Math.floor(rng() * 6000),
      NSToDOMContentLoadedTiming: Math.floor(rng() * 4000),
      FirstPaintTiming: Math.floor(rng() * 2000),
      RedirectCount: rng() < 0.1 ? Math.floor(rng() * 3) : 0,
      SocialSourceNetworkID: Math.floor(rng() * 10),
      SocialSourcePage: '',
      ParamPrice: Math.floor(rng() * 100000),
      ParamOrderID: rng() < 0.1 ? String(Math.floor(rng() * 1000000)) : '',
      ParamCurrency: pick(['USD', 'EUR', 'GBP', 'JPY', 'CNY', ''], rng),
      ParamCurrencyID: Math.floor(rng() * 10),
      GoalsReached: Math.floor(rng() * 10),
      OpenstatServiceName: '',
      OpenstatCampaignID: '',
      OpenstatAdID: '',
      OpenstatSourceID: '',
      UTMSource: pick(['', '', 'google', 'facebook', 'twitter', 'newsletter'], rng),
      UTMMedium: pick(['', '', 'cpc', 'organic', 'social', 'email'], rng),
      UTMCampaign: pick(['', '', 'summer_sale', 'black_friday', 'new_product'], rng),
      UTMContent: '',
      UTMTerm: pick(SEARCH_PHRASES, rng),
      FromTag: '',
      HasGCLID: rng() < 0.1 ? 1 : 0,
      RefererHash: Math.floor(rng() * Number.MAX_SAFE_INTEGER),
      URLHash: Math.floor(rng() * Number.MAX_SAFE_INTEGER),
      CLID: Math.floor(rng() * 1000000),
      YCLID: Math.floor(rng() * 1000000),
      ShareService: '',
      ShareURL: '',
      ShareTitle: '',
      ParsedParamsKey1: '',
      ParsedParamsKey2: '',
      ParsedParamsKey3: '',
      ParsedParamsKey4: '',
      ParsedParamsKey5: '',
      ParsedParamsValueDouble: 0,
      IsLandmark: rng() < 0.05 ? 1 : 0,
      RequestNum: Math.floor(rng() * 100),
      RequestTry: Math.floor(rng() * 3),
      UserID: userId,
      SessionID: Math.floor(rng() * Number.MAX_SAFE_INTEGER),
      PageViews: pageViews,
      SessionDuration: sessionDuration,
      TrafficSource: pick(TRAFFIC_SOURCES, rng),
      DeviceType: deviceType,
      ScreenWidth: resWidth,
      ScreenHeight: resHeight,
      ClientX: clientX,
      ClientY: clientY,
      Country: countryCode,
      Region: REGIONS[regionIdx],
      City: CITIES[regionIdx % CITIES.length],
      Browser: pick(BROWSERS, rng),
      IsNewUser: isNewUser ? 1 : 0,
    }

    hits.push(JSON.stringify(hit))
  }

  return {
    hits: hits.join('\n'),
  }
}

// =============================================================================
// IMDB Dataset Generator
// Simulates movie/TV/person data similar to real IMDB datasets
// =============================================================================

const IMDB_SEED = 67890

const IMDB_SIZE_CONFIGS: Record<SizeOption, { titles: number; names: number; ratings: number }> = {
  '1mb': { titles: 2000, names: 3000, ratings: 1500 },
  '10mb': { titles: 20000, names: 30000, ratings: 15000 },
  '100mb': { titles: 200000, names: 300000, ratings: 150000 },
  '1gb': { titles: 2000000, names: 3000000, ratings: 1500000 },
}

// IMDB-style data arrays
const TITLE_TYPES = ['movie', 'tvSeries', 'tvEpisode', 'tvMovie', 'tvMiniSeries', 'short', 'videoGame', 'video']
const GENRES = [
  'Drama', 'Comedy', 'Action', 'Adventure', 'Horror', 'Thriller', 'Romance',
  'Sci-Fi', 'Fantasy', 'Mystery', 'Crime', 'Documentary', 'Animation',
  'Family', 'Biography', 'History', 'War', 'Music', 'Musical', 'Western',
  'Sport', 'Film-Noir', 'News', 'Reality-TV', 'Talk-Show', 'Game-Show',
]

const MOVIE_WORDS = [
  'The', 'A', 'Love', 'Death', 'Life', 'Night', 'Day', 'Dark', 'Light', 'Last',
  'First', 'Final', 'Secret', 'Hidden', 'Lost', 'Found', 'Return', 'Rise', 'Fall',
  'Journey', 'Quest', 'Legend', 'Story', 'Tale', 'Chronicles', 'Adventures',
  'Blood', 'Fire', 'Ice', 'Storm', 'Shadow', 'Sun', 'Moon', 'Star', 'World',
  'Empire', 'Kingdom', 'House', 'Family', 'Brothers', 'Sisters', 'Father', 'Mother',
]

const FIRST_NAMES_ACTORS = [
  'John', 'Michael', 'David', 'James', 'Robert', 'William', 'Christopher', 'Daniel',
  'Emma', 'Olivia', 'Sophia', 'Isabella', 'Mia', 'Charlotte', 'Amelia', 'Harper',
  'Tom', 'Chris', 'Brad', 'Leonardo', 'Meryl', 'Cate', 'Jennifer', 'Scarlett',
  'Denzel', 'Samuel', 'Morgan', 'Anthony', 'Viola', 'Lupita', 'Octavia', 'Halle',
]

const LAST_NAMES_ACTORS = [
  'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Miller', 'Davis', 'Wilson',
  'Anderson', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Thompson', 'White',
  'Hanks', 'Cruise', 'Pitt', 'DiCaprio', 'Streep', 'Blanchett', 'Lawrence', 'Johansson',
  'Washington', 'Freeman', 'Hopkins', 'Davis', 'Nyongo', 'Spencer', 'Berry',
]

const PROFESSIONS = [
  'actor', 'actress', 'director', 'producer', 'writer', 'composer', 'cinematographer',
  'editor', 'production_designer', 'costume_designer', 'make_up_department',
  'sound_department', 'visual_effects', 'stunts', 'miscellaneous',
]

interface ImdbData {
  title_basics: string
  name_basics: string
  title_ratings: string
}

function generateImdbData(size: SizeOption): ImdbData {
  const config = IMDB_SIZE_CONFIGS[size]
  const rng = createRng(IMDB_SEED)

  // Generate title IDs for reference
  const titleIds: string[] = []
  for (let i = 0; i < config.titles; i++) {
    titleIds.push(`tt${String(i + 1).padStart(7, '0')}`)
  }

  // Generate name IDs for reference
  const nameIds: string[] = []
  for (let i = 0; i < config.names; i++) {
    nameIds.push(`nm${String(i + 1).padStart(7, '0')}`)
  }

  // Generate title.basics
  const titleBasics: string[] = []
  for (let i = 0; i < config.titles; i++) {
    const titleType = pick(TITLE_TYPES, rng)
    const isAdult = rng() < 0.05 ? 1 : 0
    const startYear = 1900 + Math.floor(rng() * 125) // 1900-2024
    const endYear = titleType.includes('Series') && rng() > 0.3
      ? startYear + Math.floor(rng() * 15)
      : null
    const runtimeMinutes = titleType === 'short'
      ? Math.floor(rng() * 30) + 5
      : titleType === 'movie'
      ? Math.floor(rng() * 150) + 60
      : titleType.includes('Episode')
      ? Math.floor(rng() * 45) + 20
      : Math.floor(rng() * 120) + 30

    // Generate title
    const numWords = Math.floor(rng() * 4) + 1
    const words: string[] = []
    for (let w = 0; w < numWords; w++) {
      words.push(pick(MOVIE_WORDS, rng))
    }
    const primaryTitle = words.join(' ')

    // Generate genres (1-3 genres)
    const numGenres = Math.floor(rng() * 3) + 1
    const selectedGenres: string[] = []
    for (let g = 0; g < numGenres; g++) {
      const genre = pick(GENRES, rng)
      if (!selectedGenres.includes(genre)) {
        selectedGenres.push(genre)
      }
    }

    titleBasics.push(JSON.stringify({
      tconst: titleIds[i],
      titleType,
      primaryTitle,
      originalTitle: rng() > 0.3 ? primaryTitle : `${primaryTitle} (Original)`,
      isAdult,
      startYear,
      endYear,
      runtimeMinutes,
      genres: selectedGenres.join(','),
    }))
  }

  // Generate name.basics
  const nameBasics: string[] = []
  for (let i = 0; i < config.names; i++) {
    const firstName = pick(FIRST_NAMES_ACTORS, rng)
    const lastName = pick(LAST_NAMES_ACTORS, rng)
    const primaryName = `${firstName} ${lastName}`

    const birthYear = 1920 + Math.floor(rng() * 85) // 1920-2004
    const deathYear = rng() < 0.2 && birthYear < 1970
      ? birthYear + Math.floor(rng() * 80) + 20
      : null

    // Generate professions (1-3)
    const numProfessions = Math.floor(rng() * 3) + 1
    const selectedProfessions: string[] = []
    for (let p = 0; p < numProfessions; p++) {
      const profession = pick(PROFESSIONS, rng)
      if (!selectedProfessions.includes(profession)) {
        selectedProfessions.push(profession)
      }
    }

    // Known for titles (0-4 titles)
    const numKnownFor = Math.floor(rng() * 5)
    const knownForTitles: string[] = []
    for (let k = 0; k < numKnownFor; k++) {
      knownForTitles.push(pick(titleIds, rng))
    }

    nameBasics.push(JSON.stringify({
      nconst: nameIds[i],
      primaryName,
      birthYear,
      deathYear,
      primaryProfession: selectedProfessions.join(','),
      knownForTitles: knownForTitles.join(','),
    }))
  }

  // Generate title.ratings
  const titleRatings: string[] = []
  const ratedTitles = new Set<string>()

  for (let i = 0; i < config.ratings; i++) {
    // Pick a title that hasn't been rated yet (or allow duplicates for simplicity)
    const titleId = pick(titleIds, rng)
    if (ratedTitles.has(titleId) && ratedTitles.size < titleIds.length * 0.9) {
      // Try to find an unrated title
      for (const tid of titleIds) {
        if (!ratedTitles.has(tid)) {
          ratedTitles.add(tid)
          break
        }
      }
    }
    ratedTitles.add(titleId)

    // Generate realistic rating distribution (bell curve around 6-7)
    const u1 = rng()
    const u2 = rng()
    const gaussian = Math.sqrt(-2 * Math.log(u1 + 0.0001)) * Math.cos(2 * Math.PI * u2)
    const averageRating = Math.max(1, Math.min(10, 6.5 + gaussian * 1.5))

    // Number of votes follows power law (most have few votes, some have millions)
    const voteTier = rng()
    const numVotes = voteTier < 0.7
      ? Math.floor(rng() * 1000) + 5
      : voteTier < 0.9
      ? Math.floor(rng() * 50000) + 1000
      : voteTier < 0.98
      ? Math.floor(rng() * 500000) + 50000
      : Math.floor(rng() * 2000000) + 500000

    titleRatings.push(JSON.stringify({
      tconst: titleId,
      averageRating: Math.round(averageRating * 10) / 10,
      numVotes,
    }))
  }

  return {
    title_basics: titleBasics.join('\n'),
    name_basics: nameBasics.join('\n'),
    title_ratings: titleRatings.join('\n'),
  }
}

// =============================================================================
// Dataset Generation Dispatcher
// =============================================================================

type DatasetData = ClickBenchData | ImdbData

function generateDataset(dataset: DatasetType, size: SizeOption): DatasetData {
  switch (dataset) {
    case 'clickbench':
      return generateClickBenchData(size)
    case 'imdb':
      return generateImdbData(size)
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
  totalSize: number
  error?: string
}

// Utility function for formatting bytes
function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
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
      ...config,
      sizes: VALID_SIZES,
    })),
  })
})

// Stage a dataset (with size parameter)
app.post('/stage/:dataset/:size', async (c) => {
  const dataset = c.req.param('dataset') as DatasetType
  const size = c.req.param('size') as SizeOption
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

  const config = DATASET_CONFIGS[dataset]

  // Check if dataset already exists in R2
  const existingPrefix = `analytics/${dataset}/${size}/`
  const existingFiles = await c.env.ANALYTICS_BUCKET.list({ prefix: existingPrefix })

  if (existingFiles.objects.length >= config.tables.length) {
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
      size,
      duration: Date.now() - startTime,
      files,
      totalSize,
      message: 'Dataset already staged in R2',
    })
  }

  try {
    // Generate dataset in-worker
    console.log(`Generating ${dataset} dataset (${size}) in-worker...`)
    const data = generateDataset(dataset, size)
    const generationTime = Date.now() - startTime
    console.log(`Generation complete in ${generationTime}ms`)

    // Upload generated files to R2
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
      const r2Key = `analytics/${dataset}/${size}/${filename}`
      await c.env.ANALYTICS_BUCKET.put(r2Key, content, {
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

      const fileSize = new Blob([content]).size
      uploadedFiles.push({
        name: filename,
        size: fileSize,
        key: r2Key,
      })

      console.log(`Uploaded ${filename}: ${formatBytes(fileSize)}`)
    }

    const totalSize = uploadedFiles.reduce((sum, f) => sum + f.size, 0)

    const response: StageResult = {
      success: true,
      dataset,
      size,
      duration: Date.now() - startTime,
      files: uploadedFiles,
      totalSize,
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
        totalSize: 0,
        error: error instanceof Error ? error.message : String(error),
      } as StageResult,
      500
    )
  }
})

// Legacy endpoint without size (defaults to 10mb)
app.post('/stage/:dataset', async (c) => {
  const dataset = c.req.param('dataset')
  // Redirect to the sized version with default size
  const url = new URL(c.req.url)
  url.pathname = `/stage/${dataset}/10mb`
  return c.redirect(url.toString(), 307)
})

// Check dataset status
app.get('/status/:dataset/:size', async (c) => {
  const dataset = c.req.param('dataset') as DatasetType
  const size = c.req.param('size') as SizeOption

  // Validate dataset
  if (!DATASET_CONFIGS[dataset]) {
    return c.json({ error: `Invalid dataset: ${dataset}` }, 400)
  }

  // Validate size
  if (!VALID_SIZES.includes(size)) {
    return c.json({ error: `Invalid size: ${size}` }, 400)
  }

  const config = DATASET_CONFIGS[dataset]
  const prefix = `analytics/${dataset}/${size}/`
  const files = await c.env.ANALYTICS_BUCKET.list({ prefix })

  if (files.objects.length === 0) {
    return c.json({
      exists: false,
      dataset,
      size,
      expectedTables: config.tables,
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
    complete: files.objects.length >= config.tables.length,
    dataset,
    size,
    files: fileList,
    totalSize,
    totalSizeFormatted: formatBytes(totalSize),
  })
})

// Legacy status endpoint without size
app.get('/status/:dataset', async (c) => {
  const dataset = c.req.param('dataset')
  const url = new URL(c.req.url)
  url.pathname = `/status/${dataset}/10mb`
  return c.redirect(url.toString(), 307)
})

// Delete a staged dataset
app.delete('/stage/:dataset/:size', async (c) => {
  const dataset = c.req.param('dataset') as DatasetType
  const size = c.req.param('size') as SizeOption

  // Validate dataset
  if (!DATASET_CONFIGS[dataset]) {
    return c.json({ error: `Invalid dataset: ${dataset}` }, 400)
  }

  // Validate size
  if (!VALID_SIZES.includes(size)) {
    return c.json({ error: `Invalid size: ${size}` }, 400)
  }

  const prefix = `analytics/${dataset}/${size}/`
  const files = await c.env.ANALYTICS_BUCKET.list({ prefix })

  let deleted = 0
  for (const obj of files.objects) {
    await c.env.ANALYTICS_BUCKET.delete(obj.key)
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
  const datasets: DatasetType[] = ['clickbench', 'imdb']

  for (const dataset of datasets) {
    // Make internal request to stage endpoint
    const response = await app.fetch(
      new Request(`http://localhost/stage/${dataset}/${size}`, {
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
      size,
      successful,
      failed,
      total: datasets.length,
      totalSize,
      totalSizeFormatted: formatBytes(totalSize),
    },
    results,
  })
})

// Legacy stage-all endpoint
app.post('/stage-all', async (c) => {
  const url = new URL(c.req.url)
  url.pathname = '/stage-all/10mb'
  return c.redirect(url.toString(), 307)
})

export default app
