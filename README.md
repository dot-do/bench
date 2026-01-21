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
| **tigerbeetle** | Financial/Accounting | WASM | Yes |

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

### 8. Financial Benchmarks (`pnpm bench:financial`)

Compares TigerBeetle vs traditional databases for financial workloads:
- Account creation (single and batch)
- Transfer processing (double-entry bookkeeping)
- Balance lookups
- Transaction throughput

TigerBeetle is purpose-built for financial accounting with:
- Strict double-entry bookkeeping
- ACID guarantees
- Exactly-once processing
- Up to 1M+ TPS

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
│   ├── colo.bench.ts
│   └── financial.bench.ts
├── databases/            # Database adapters
│   ├── db4.ts
│   ├── evodb.ts
│   ├── postgres.ts
│   ├── sqlite.ts
│   ├── duckdb.ts
│   └── tigerbeetle.ts
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

## Database Comparison

### Cost Analysis
All databases use 2MB blob optimization - costs are equal.

### Performance Comparison
| Database | WASM Size | Cold Start | Best For |
|----------|-----------|------------|----------|
| db4 | None | 0ms | Multi-paradigm, IceType |
| evodb | None | 0ms | Schema evolution, columnar |
| sqlite | 4.4 MB | ~150ms | SQL familiarity |
| postgres | 13-14 MB | ~600ms | Full PG, ORMs |
| duckdb | 36+ MB | ~1500ms | OLAP analytics |

### MongoDB Compatibility
| Package | Backend | WASM | Use Case |
|---------|---------|------|----------|
| @db4/mongo | db4 | None | Fast cold start + Mongo API |
| @dotdo/mongodb | PostgreSQL | 13 MB | Full SQL + Mongo API + ACID |
| mongo.do | Managed | N/A | Managed service |

## Datasets

### OLTP Datasets (Transactional)
- E-commerce (1MB - 50GB)
- Multi-tenant SaaS (1MB - 50GB)
- Social Network (1MB - 50GB)
- IoT Timeseries (1MB - 50GB)

### Financial Datasets
- Ledger (chart of accounts, journals, trial balance)
- Banking (accounts, transfers, balances)
- E-commerce Payments (orders, payments, refunds, settlements)

### Analytics Datasets (OLAP)
- ClickBench (99M rows, web analytics)
- Wiktionary (~10GB, full-text search)
- Wikidata (~100GB+, knowledge graph)
- Common Crawl Host Graph (web graph)

## Running Benchmarks

```bash
# OLTP benchmarks
pnpm bench:oltp --dataset=ecommerce --size=100mb

# Analytics benchmarks
pnpm bench:analytics --dataset=clickbench

# MongoDB comparison
pnpm bench:mongo

# Financial benchmarks (TigerBeetle vs SQL)
pnpm bench:financial

# Financial benchmarks (simulated mode for CI)
pnpm bench:financial:simulated

# Full suite
pnpm bench:all
```

## TigerBeetle Setup

TigerBeetle requires either:
1. A running TigerBeetle cluster (for native performance benchmarks)
2. Simulated mode (for CI and development)

### Local Development with Docker

```bash
# Start TigerBeetle cluster
docker run -p 3000:3000 ghcr.io/tigerbeetle/tigerbeetle:latest \
  format --cluster=0 --replica=0 --replica-count=1 /data/0_0.tigerbeetle

docker run -p 3000:3000 ghcr.io/tigerbeetle/tigerbeetle:latest \
  start --addresses=0.0.0.0:3000 /data/0_0.tigerbeetle

# Run benchmarks against local cluster
pnpm bench:financial
```

### Simulated Mode for CI

The TigerBeetle adapter includes a pure TypeScript implementation with TigerBeetle semantics
that runs entirely in-memory. This is useful for:
- CI pipelines without TigerBeetle cluster access
- Development and testing
- Understanding the API without infrastructure setup

```bash
# Run with simulated mode
TIGERBEETLE_SIMULATED=true pnpm bench:financial

# Or use the npm script
pnpm bench:financial:simulated
```

### WASM Port for Cloudflare Workers

This project uses the WASM port from `@dotdo/poc-tigerbeetle-do` which provides:
- Pure TypeScript LedgerState implementation
- TigerBeetle semantics (double-entry, ACID)
- Suitable for Cloudflare Workers and Durable Objects
- No native dependencies

## Architecture Recommendation

Default bundle (zero cold start):
- db4 (+ @db4/mongo for MongoDB API)
- evodb

Lazy load on demand:
- sqlite (smallest WASM)
- postgres (full SQL + DocumentDB)
- duckdb (analytics only)
- tigerbeetle (financial accounting)
