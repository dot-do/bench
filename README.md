# @dotdo/bench

Performance and cost benchmarks for dotdo database backends.

## Databases Benchmarked

| Database | Type | WASM | Lazy-Load |
|----------|------|------|-----------|
| **db4** | OLTP (4 paradigms) | No | N/A |
| **evodb** | OLTP + Analytics | No | N/A |
| **postgres** | OLTP (PGLite) | Yes (~13MB) | Yes |
| **sqlite** | OLTP (libsql) | Yes (~2MB) | Yes |
| **duckdb** | OLAP | Yes (~40MB) | Yes |

## Benchmark Categories

### 1. Bundle Size (`pnpm bench:bundle`)

Measures:
- TypeScript/JS size (types, bindings)
- WASM size (if applicable)
- Gzipped vs uncompressed
- Tree-shaking effectiveness

### 2. Cold Start (`pnpm bench:cold`)

First query after:
- Worker cold start
- DO cold start (no hibernation)
- DO wake from hibernation

### 3. Warm Query (`pnpm bench:warm`)

Query latency with:
- Cached WASM instance
- Initialized database
- No data in cache

### 4. Hot Cache (`pnpm bench:hot`)

Query latency with:
- Everything cached
- DO SQLite data in memory
- Edge cache populated

### 5. Worker vs DO (`benchmarks/worker-vs-do.bench.ts`)

Same operations in:
- Workers (stateless)
- Durable Objects (stateful)
- Measures overhead of DO routing

### 6. Colo Analysis (`benchmarks/colo.bench.ts`)

Latency from:
- Same colo as DO
- Different colo (cross-region)
- Global distribution patterns

### 7. Cost Analysis (`pnpm bench:cost`)

Calculates $/1M operations:
- Rows read/written (DO SQLite)
- R2 operations (lakehouse)
- CPU time (Workers/DO duration)
- 2MB blob optimization impact

## Running Benchmarks

```bash
# Install dependencies
pnpm install

# Run all benchmarks
pnpm bench:all

# Run specific benchmark
pnpm bench:cold

# Deploy benchmark worker
pnpm deploy
```

## Results

Results are published to:
- `/results/latest.json` - Most recent run
- `/results/{timestamp}.json` - Historical runs
- `bench.dotdo.dev` - Dashboard

## Architecture

```
bench/
├── benchmarks/           # Vitest bench files
│   ├── cold-start.bench.ts
│   ├── warm-query.bench.ts
│   ├── hot-cache.bench.ts
│   ├── worker-vs-do.bench.ts
│   └── colo.bench.ts
├── databases/            # Database adapters
│   ├── db4.ts
│   ├── evodb.ts
│   ├── postgres.ts
│   ├── sqlite.ts
│   └── duckdb.ts
├── scripts/              # Analysis scripts
│   ├── measure-bundles.js
│   └── calculate-costs.js
├── worker/               # Benchmark worker
│   └── index.ts
└── results/              # Benchmark results
```

## Lazy-Load WASM Pattern

For WASM databases (postgres, sqlite, duckdb), we use lazy loading:

```typescript
// Types always available (tiny)
import type { PGLite } from '@dotdo/postgres'

// WASM loaded on first use (~13MB for postgres)
const { PGLite } = await import('@dotdo/postgres')
const db = await PGLite.create()
```

This allows all databases to be "available" with minimal bundle impact.
