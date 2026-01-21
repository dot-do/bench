# Lazy-Load WASM Architecture for @dotdo/db

This document describes the lazy-loading architecture for WASM-based database modules in the dotdo database layer. The pattern ensures minimal bundle sizes and fast cold starts while supporting multiple database backends.

## Overview

The `@dotdo/db` package supports multiple database backends with vastly different bundle sizes. To maintain fast cold starts and small initial bundles, WASM-heavy databases are loaded lazily on demand.

```
┌─────────────────────────────────────────────────────────┐
│                    @dotdo/db                            │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐       │
│  │  db4    │ │ evodb   │ │ sqlite  │ │postgres │       │
│  │ (types) │ │ (types) │ │ (types) │ │ (types) │       │
│  │ BUNDLED │ │ BUNDLED │ │ BUNDLED │ │ BUNDLED │       │
│  └─────────┘ └─────────┘ └────┬────┘ └────┬────┘       │
│                               │           │            │
│                    LAZY LOAD  │           │            │
│                    ON DEMAND  ▼           ▼            │
│                          ┌────────────────────┐        │
│                          │  WASM Modules      │        │
│                          │  - libsql.wasm     │        │
│                          │  - pglite.wasm     │        │
│                          │  - duckdb-mvp.wasm │        │
│                          └────────────────────┘        │
└─────────────────────────────────────────────────────────┘
```

## Why Lazy Loading Matters

### Cold Start Performance

Cloudflare Workers have strict cold start requirements. Loading large WASM modules synchronously delays the first response:

| Database | WASM Size | Gzipped | Cold Start Impact |
|----------|-----------|---------|-------------------|
| db4      | 0 KB      | 0 KB    | None (pure TS)    |
| evodb    | 0 KB      | 0 KB    | None (pure TS)    |
| SQLite   | 4.4 MB    | 1.3 MB  | ~200-400ms        |
| Postgres | 13-14 MB  | ~4 MB   | ~500-800ms        |
| DuckDB   | 36+ MB    | ~12 MB  | ~1-2s             |

### Bundle Size Constraints

Workers have a 10 MB compressed bundle limit (25 MB uncompressed for paid plans). Including all WASM modules would exceed this limit.

### Memory Efficiency

WASM modules consume memory even when not used. Lazy loading ensures memory is only allocated for databases actually in use.

## Database Categories

### Pure TypeScript (Always Bundled)

These databases have zero WASM dependencies and are always included in the bundle:

**db4** - In-memory key-value store with TypeScript-native implementation
- Bundle cost: ~10 KB minified
- Use case: Caching, session storage, temporary data

**evodb** - Event-sourced database with append-only log
- Bundle cost: ~15 KB minified
- Use case: Event sourcing, audit logs, CQRS patterns

### WASM-Based (Lazy Loaded)

These databases require WASM modules loaded on-demand:

**SQLite (libsql)** - Full SQL support via libsql WASM
- WASM size: 4.4 MB (1.3 MB gzipped)
- Smallest WASM option for SQL workloads
- Use case: General-purpose relational data

**Postgres (PGLite)** - PostgreSQL compatibility via PGLite
- WASM size: 13-14 MB (~4 MB gzipped)
- Full PostgreSQL feature set
- Use case: PostgreSQL-specific features, migrations from Postgres

**DuckDB** - Analytics-focused columnar database
- WASM size: 36+ MB base + 7 MB extensions
- Optimized for OLAP queries
- Use case: Analytics, large aggregations, Parquet files

## Implementation Pattern

### Type Imports vs Dynamic Imports

The key pattern separates **type imports** (compile-time, zero cost) from **value imports** (runtime, triggers WASM load):

```typescript
// types.ts - Always bundled, zero runtime cost
export type { Database as SQLiteDatabase } from '@libsql/client'
export type { PGlite as PostgresDatabase } from '@electric-sql/pglite'
export type { Database as DuckDBDatabase } from '@duckdb/duckdb-wasm'

// Discriminated union for type safety
export type DatabaseConfig =
  | { type: 'db4'; options?: Db4Options }
  | { type: 'evodb'; options?: EvoDbOptions }
  | { type: 'sqlite'; options?: SQLiteOptions }
  | { type: 'postgres'; options?: PostgresOptions }
  | { type: 'duckdb'; options?: DuckDBOptions }
```

### Lazy Loader Implementation

```typescript
// loaders/sqlite.ts
let sqliteModule: typeof import('@libsql/client') | null = null

export async function loadSQLite() {
  if (!sqliteModule) {
    sqliteModule = await import('@libsql/client')
  }
  return sqliteModule
}

export async function createSQLiteDatabase(options: SQLiteOptions) {
  const { createClient } = await loadSQLite()
  return createClient({
    url: options.url ?? ':memory:',
    ...options,
  })
}
```

```typescript
// loaders/postgres.ts
let pgliteModule: typeof import('@electric-sql/pglite') | null = null

export async function loadPostgres() {
  if (!pgliteModule) {
    pgliteModule = await import('@electric-sql/pglite')
  }
  return pgliteModule
}

export async function createPostgresDatabase(options: PostgresOptions) {
  const { PGlite } = await loadPostgres()
  return new PGlite(options.dataDir ?? 'idb://dotdo', options)
}
```

```typescript
// loaders/duckdb.ts
let duckdbModule: typeof import('@duckdb/duckdb-wasm') | null = null

export async function loadDuckDB() {
  if (!duckdbModule) {
    duckdbModule = await import('@duckdb/duckdb-wasm')
  }
  return duckdbModule
}

export async function createDuckDBDatabase(options: DuckDBOptions) {
  const duckdb = await loadDuckDB()

  // DuckDB requires additional setup
  const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles()
  const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES)

  const worker = new Worker(bundle.mainWorker!)
  const logger = new duckdb.ConsoleLogger()
  const db = new duckdb.AsyncDuckDB(logger, worker)

  await db.instantiate(bundle.mainModule, bundle.pthreadWorker)
  return db.connect()
}
```

### Unified Factory

```typescript
// factory.ts
import { createDb4Database } from './db4'
import { createEvoDatabase } from './evodb'
import type { DatabaseConfig, DatabaseInstance } from './types'

export async function createDatabase(config: DatabaseConfig): Promise<DatabaseInstance> {
  switch (config.type) {
    case 'db4':
      // Synchronous - no WASM
      return createDb4Database(config.options)

    case 'evodb':
      // Synchronous - no WASM
      return createEvoDatabase(config.options)

    case 'sqlite': {
      // Lazy load WASM
      const { createSQLiteDatabase } = await import('./loaders/sqlite')
      return createSQLiteDatabase(config.options)
    }

    case 'postgres': {
      // Lazy load WASM
      const { createPostgresDatabase } = await import('./loaders/postgres')
      return createPostgresDatabase(config.options)
    }

    case 'duckdb': {
      // Lazy load WASM
      const { createDuckDBDatabase } = await import('./loaders/duckdb')
      return createDuckDBDatabase(config.options)
    }

    default:
      throw new Error(`Unknown database type: ${(config as any).type}`)
  }
}
```

## Usage Examples

### Pure TypeScript Databases (Instant)

```typescript
import { createDatabase } from '@dotdo/db'

// db4 - instant, no async overhead for simple cases
const cache = await createDatabase({ type: 'db4' })
await cache.set('user:123', { name: 'Alice' })

// evodb - instant, event-sourced
const events = await createDatabase({ type: 'evodb' })
await events.append({ type: 'user.created', payload: { id: '123' } })
```

### WASM Databases (Lazy Loaded)

```typescript
import { createDatabase } from '@dotdo/db'

// SQLite - loads 4.4 MB WASM on first call
const sqlite = await createDatabase({
  type: 'sqlite',
  options: { url: ':memory:' }
})
await sqlite.execute('CREATE TABLE users (id TEXT, name TEXT)')

// Postgres - loads 13 MB WASM on first call
const postgres = await createDatabase({
  type: 'postgres',
  options: { dataDir: 'idb://myapp' }
})
await postgres.query('SELECT NOW()')

// DuckDB - loads 36+ MB WASM on first call
const duckdb = await createDatabase({
  type: 'duckdb',
  options: {}
})
await duckdb.query('SELECT * FROM read_parquet("data.parquet")')
```

### Conditional Loading Pattern

```typescript
import { createDatabase } from '@dotdo/db'

async function getDatabase(requiresSQL: boolean) {
  if (requiresSQL) {
    // Only loads WASM if SQL features are needed
    return createDatabase({ type: 'sqlite' })
  }
  // No WASM overhead for simple key-value
  return createDatabase({ type: 'db4' })
}
```

## Bundle Size Impact Analysis

### Without Lazy Loading

If all databases were bundled statically:

```
@dotdo/db bundle size:
  - Core package:     ~50 KB
  - db4:              ~10 KB
  - evodb:            ~15 KB
  - libsql WASM:      4.4 MB
  - PGLite WASM:      13 MB
  - DuckDB WASM:      36 MB
  ─────────────────────────
  Total:              ~53 MB (EXCEEDS LIMITS)
```

### With Lazy Loading

```
@dotdo/db bundle size (initial):
  - Core package:     ~50 KB
  - db4:              ~10 KB
  - evodb:            ~15 KB
  - Type stubs:       ~5 KB
  ─────────────────────────
  Total:              ~80 KB

WASM loaded on demand:
  - SQLite:           +4.4 MB (when sqlite used)
  - Postgres:         +13 MB (when postgres used)
  - DuckDB:           +36 MB (when duckdb used)
```

## Recommendations

### Default Database Selection

For most dotdo applications, we recommend:

1. **Default: db4** - Zero overhead, sufficient for most DO state management
2. **Event sourcing: evodb** - When audit trails or event replay is needed
3. **Complex queries: sqlite** - When SQL is required, smallest WASM footprint
4. **PostgreSQL compat: postgres** - Only when PostgreSQL-specific features needed
5. **Analytics: duckdb** - Only for heavy analytical workloads

### Decision Matrix

| Requirement | Recommended DB | Reason |
|-------------|---------------|--------|
| Key-value storage | db4 | Zero WASM, instant |
| Event sourcing | evodb | Zero WASM, append-only |
| Basic SQL | sqlite | Smallest WASM (4.4 MB) |
| PostgreSQL compatibility | postgres | Full PG features |
| OLAP / Analytics | duckdb | Columnar optimization |
| Parquet files | duckdb | Native Parquet support |

### Cloudflare Workers Considerations

1. **Prefer db4/evodb** for Durable Object state - they're optimized for the DO storage API
2. **Use SQLite** for complex queries within size constraints
3. **Avoid DuckDB** in Workers - 36 MB exceeds comfortable limits
4. **Always lazy load** - even for frequently used databases (see below)

## Wrangler Static Assets Configuration

WASM modules should be bundled as Workers static assets, not fetched from external CDNs. This ensures atomic deployments and eliminates external network dependencies.

### Example wrangler.jsonc

```jsonc
{
  "name": "dotdo-worker",
  "main": "src/index.ts",
  "compatibility_date": "2024-01-01",

  // Bundle WASM as static assets - deployed with worker
  "rules": [
    { "type": "CompiledWasm", "globs": ["**/*.wasm"], "fallthrough": true }
  ],

  // Or use assets directory for larger WASM files
  "assets": {
    "directory": "./wasm",
    "binding": "ASSETS"
  }
}
```

### Static Assets vs CDN

Bundling WASM in static assets is strongly preferred over fetching from CDNs:

| Aspect | Static Assets | CDN Fetch |
|--------|--------------|-----------|
| **Network** | No external fetch | Requires network call |
| **Deployment** | Atomic with worker code | Separate, can drift |
| **Caching** | Automatic edge caching | Manual cache headers |
| **CORS** | No issues | Potential CORS errors |
| **Reliability** | Worker availability | CDN + Worker availability |
| **Latency** | Local read | Network round-trip |

When WASM is bundled as a static asset:
- It deploys atomically with your worker code
- It's automatically cached at the edge
- No external network dependencies at runtime
- Version consistency guaranteed

## Parallel Warming Pattern

When lazy loading WASM, R2 cold storage can be warmed simultaneously. This is a critical optimization that reduces first-request latency.

### Parallel Initialization

```typescript
// Parallel initialization - don't await sequentially!
async function initializeDatabase(type: 'sqlite' | 'postgres' | 'duckdb') {
  const [wasmModule, coldData] = await Promise.all([
    loadWasmModule(type),           // Load WASM from static assets
    warmR2ColdStorage(type),        // Pre-fetch from R2 in parallel
  ])

  return instantiateDatabase(wasmModule, coldData)
}
```

### Anti-Pattern: Sequential Loading

```typescript
// BAD: Sequential awaits waste time
async function initializeDatabaseSlow(type: 'sqlite' | 'postgres' | 'duckdb') {
  const wasmModule = await loadWasmModule(type)    // Wait for WASM...
  const coldData = await warmR2ColdStorage(type)   // Then wait for R2...
  return instantiateDatabase(wasmModule, coldData) // Total: WASM + R2 time
}
```

### Why Parallel Matters

| Operation | Duration | Sequential Total | Parallel Total |
|-----------|----------|------------------|----------------|
| WASM Load | ~200ms | | |
| R2 Warm | ~150ms | | |
| **Total** | | **~350ms** | **~200ms** |

The parallel pattern reduces initialization latency by overlapping independent operations.

## Why Always Lazy Load

Even for frequently used databases like SQLite, lazy loading is the correct approach:

### 1. Worker Cold Start Doesn't Block

Without lazy loading:
```
Cold Start Timeline:
├── Parse JavaScript (50ms)
├── Instantiate WASM (200-400ms)  ← BLOCKS FIRST REQUEST
└── Ready to serve
Total: 250-450ms before first response
```

With lazy loading:
```
Cold Start Timeline:
├── Parse JavaScript (50ms)
└── Ready to serve               ← IMMEDIATE
    ├── (First request triggers WASM load)
    └── (But request is already being processed)
Total: 50ms to start processing
```

### 2. R2 Warming Happens in Parallel

Lazy loading enables the parallel warming pattern described above. Eager loading would serialize the WASM instantiation before any other initialization.

### 3. First Request Latency Reduced

```typescript
// With lazy loading + parallel warming
app.get('/query', async (c) => {
  // These happen in parallel with request processing
  const db = await getDatabase()  // WASM loads while request parses
  return c.json(await db.query(c.req.query('sql')))
})
```

### 4. Memory Only Allocated When Needed

WASM modules consume significant memory. Lazy loading ensures:
- Workers handling non-SQL routes never allocate WASM memory
- Memory pressure is distributed across the request lifecycle
- Unused database backends consume zero memory

### Preload During Idle (Optional)

If you know SQL queries are expected, preload during idle time:

```typescript
// Preload during idle time (browser environments)
if (typeof requestIdleCallback !== 'undefined') {
  requestIdleCallback(async () => {
    const { loadSQLite } = await import('@dotdo/db/loaders/sqlite')
    await loadSQLite() // Warm the cache
  })
}

// Preload in Workers after first response
export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext) {
    const response = await handleRequest(request, env)

    // Preload WASM after response is sent
    ctx.waitUntil(preloadWasmIfNeeded(env))

    return response
  }
}
```

## Future Considerations

### Streaming WASM Compilation

Modern browsers support streaming WASM compilation which can reduce load times:

```typescript
const response = await fetch(wasmUrl)
const module = await WebAssembly.compileStreaming(response)
```

### Shared WASM Modules

For applications using multiple databases, consider sharing common WASM infrastructure (e.g., memory allocators) to reduce total memory footprint.

### Edge Caching

WASM modules can be cached at the edge to reduce subsequent load times:

```typescript
// Cache WASM in KV or R2 for faster subsequent loads
const wasmCache = await caches.open('wasm-modules')
```
