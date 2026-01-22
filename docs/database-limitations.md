# Database Limitations and Known Issues

This document describes the known limitations, caveats, and best practices for each database in the benchmark suite. All databases are designed to run in Cloudflare Workers / Durable Objects environments.

## Table of Contents

1. [db4 - Pure TypeScript Document Store](#db4---pure-typescript-document-store)
2. [evodb - Schema Evolution Database](#evodb---schema-evolution-database)
3. [postgres (PGLite) - PostgreSQL WASM](#postgres-pglite---postgresql-wasm)
4. [sqlite (libsql) - SQLite WASM](#sqlite-libsql---sqlite-wasm)
5. [duckdb - DuckDB WASM](#duckdb---duckdb-wasm)
6. [tigerbeetle - Financial Ledger](#tigerbeetle---financial-ledger-wasm-port)
7. [graphdb - Triple Store](#graphdb---triple-store-with-websocket-hibernation)
8. [sdb - Document/Graph Database](#sdb---documentgraph-database)
9. [@db4/mongo - MongoDB API on db4](#db4mongo---mongodb-api-on-db4)
10. [@dotdo/mongodb - MongoDB API on PostgreSQL](#dotdomongodb---mongodb-api-on-postgresql)

---

## db4 - Pure TypeScript Document Store

### Overview
Pure TypeScript document store with no WASM dependencies. Optimized for cold start performance and minimal bundle size.

### WASM Size
- **Bundle Impact**: None (pure TypeScript)
- **Total Size**: ~10-15KB gzipped (estimated)

### Cold Start
- **Typical Time**: <5ms
- **Best in class** for cold start due to zero WASM loading

### Query Limitations
- Simple query builder with basic `where`, `limit`, `offset` support
- SQL-like queries via `query()` method support basic joins
- Join syntax: `{ from: 'table', join: { table: 'other', on: 'condition' }, limit: N }`
- No complex SQL features (subqueries, CTEs, window functions)
- No full-text search
- Limited aggregation support

### Memory Usage
| Data Size | Approximate Memory |
|-----------|-------------------|
| 1,000 docs | ~1-2 MB |
| 10,000 docs | ~10-20 MB |
| 100,000 docs | ~100-200 MB |

### Concurrency
- Single-writer model (typical for DO environment)
- No built-in transaction isolation
- Concurrent writes may cause race conditions without external coordination

### Missing Features
- No built-in indexing (full table scan for queries)
- No schema validation
- No change data capture (CDC)
- No cursor-based pagination

### Known Bugs
- None documented in codebase

### Best For
- Applications requiring minimal cold start latency
- Small to medium datasets (<10,000 documents)
- Simple document storage with basic querying
- Prototyping and development
- Cost-sensitive applications (no WASM = smaller bundle)

### Avoid For
- Complex relational queries with multiple joins
- Large datasets requiring indexed queries
- Applications needing full SQL support
- High-throughput write scenarios requiring transactions

---

## evodb - Schema Evolution Database

### Overview
Pure TypeScript event-sourced document store with a fluent query builder API. Designed for schema evolution scenarios.

### WASM Size
- **Bundle Impact**: None (pure TypeScript)
- **Total Size**: ~15-20KB gzipped (estimated)

### Cold Start
- **Typical Time**: <5ms
- Event replay may add latency on hibernation wake depending on event log size

### Query Limitations
- Fluent query builder: `query().where().join().limit().orderBy()`
- Supported operators: `=`, `!=`, `>`, `<`, `>=`, `<=`
- Single-level joins supported
- No subqueries or complex SQL features

### Memory Usage
| Data Size | Approximate Memory |
|-----------|-------------------|
| 1,000 docs | ~2-4 MB (includes event log) |
| 10,000 docs | ~20-40 MB |
| 100,000 docs | ~200-400 MB |

Note: Event-sourced architecture stores both current state and event history, increasing memory usage.

### Concurrency
- Event-sourced model provides eventual consistency
- Conflict resolution via event ordering
- No ACID transactions

### Missing Features
- No event compaction/snapshotting (event log grows unbounded)
- No schema validation at runtime
- No built-in event subscriptions
- Limited projection support

### Known Bugs
- None documented in codebase

### Best For
- Applications requiring audit trails
- Schema evolution scenarios
- Event-driven architectures
- Applications needing point-in-time recovery

### Avoid For
- High-write-volume applications (event log growth)
- Applications requiring immediate consistency
- Large datasets (event replay can be slow)
- Memory-constrained environments

---

## postgres (PGLite) - PostgreSQL WASM

### Overview
PostgreSQL-compatible store using PGLite WASM. Full SQL support with lazy-loaded WASM for optimal cold start.

### WASM Size
- **Bundle Impact**: ~2-3 MB gzipped
- **Uncompressed**: ~8-10 MB
- WASM is lazy-loaded on first instantiation

### Cold Start
- **Typical Time**: 100-300ms
- First instantiation includes WASM loading and database initialization
- Subsequent requests use cached WASM instance

### Query Limitations
- Full PostgreSQL SQL support
- Parameter binding with `$1, $2, ...` syntax
- **Nested transactions not supported** (explicit error thrown)
- Some PostgreSQL extensions may not be available in WASM
- No stored procedures (WASM limitation)
- No `LISTEN/NOTIFY` (no persistent connections)

### Memory Usage
| Data Size | Approximate Memory |
|-----------|-------------------|
| 1,000 rows | ~10-15 MB |
| 10,000 rows | ~30-50 MB |
| 100,000 rows | ~200-400 MB |

Note: In-memory database by default. Memory usage scales with data and index size.

### Concurrency
- Transaction support via `transaction()` method
- ACID compliant within single DO
- No multi-DO transactions
- Row-level locking within transactions

### Missing Features
- No database persistence (in-memory only by default)
- No `LISTEN/NOTIFY`
- No replication
- Limited extension support
- Hibernation restore requires re-seeding data (snapshot restore not implemented)

### Known Bugs
- `restoreFromStorage()` falls back to re-seeding rather than true snapshot restore

### Best For
- Applications requiring full SQL support
- Complex queries with joins, aggregations, CTEs
- Migration from PostgreSQL-based applications
- Applications needing ACID transactions

### Avoid For
- Cold start sensitive applications (<100ms requirement)
- Bundle size sensitive applications
- Very large datasets (>1GB)
- Scenarios requiring database persistence across restarts

---

## sqlite (libsql) - SQLite WASM

### Overview
SQLite-compatible store using libsql WASM. Lighter weight than PostgreSQL with good SQL support.

### WASM Size
- **Bundle Impact**: ~500KB-1MB gzipped
- **Uncompressed**: ~2-3 MB
- Smaller than PGLite

### Cold Start
- **Typical Time**: 50-150ms
- Faster than PGLite due to smaller WASM size
- Cached instance reuse for warm requests

### Query Limitations
- Standard SQLite SQL support
- Parameter binding with `?` placeholders
- **Nested transactions not supported** (explicit error thrown)
- No window functions in older SQLite versions
- No JSON1 extension functions (may vary by libsql version)
- Date/time handling requires explicit formatting (SQLite has no native DATE type)

### Memory Usage
| Data Size | Approximate Memory |
|-----------|-------------------|
| 1,000 rows | ~5-10 MB |
| 10,000 rows | ~20-30 MB |
| 100,000 rows | ~100-200 MB |

### Concurrency
- Transaction support via `transaction()` method
- Batch operations via `batch()` method
- SERIALIZABLE isolation by default
- Single-writer, multiple-reader model

### Missing Features
- No native `TIMESTAMP WITH TIME ZONE` (use TEXT with ISO format)
- Hibernation restore requires re-seeding (no binary dump restore)
- No foreign key enforcement by default (must enable with `PRAGMA foreign_keys = ON`)

### Known Bugs
- None documented in codebase

### Best For
- Applications needing SQL with lower cold start than PostgreSQL
- Mobile/edge scenarios where bundle size matters
- Applications with moderate SQL complexity
- Read-heavy workloads

### Avoid For
- Applications requiring PostgreSQL-specific features
- Complex date/time operations
- Large binary data storage
- Scenarios requiring foreign key cascade operations

---

## duckdb - DuckDB WASM

### Overview
DuckDB WASM optimized for analytical queries and columnar storage. Best for OLAP workloads.

### WASM Size
- **Bundle Impact**: ~3-5 MB gzipped
- **Uncompressed**: ~15-20 MB
- Largest WASM bundle in the suite
- Worker thread required for async operations

### Cold Start
- **Typical Time**: 200-500ms
- Requires Worker instantiation + WASM loading
- Bundle selection (MVP vs EH) adds complexity

### Query Limitations
- Excellent SQL support for analytical queries
- Parameter binding with `$1, $2, ...` syntax (converted from `?` internally)
- **No transaction support** (analytical database)
- `INSERT OR IGNORE` syntax supported
- Native Parquet/CSV file reading (requires virtual filesystem)
- Column metadata not returned in query results (`columns: []` always empty)

### Memory Usage
| Data Size | Approximate Memory |
|-----------|-------------------|
| 1,000 rows | ~20-30 MB |
| 10,000 rows | ~50-80 MB |
| 100,000 rows | ~200-400 MB |
| 1,000,000 rows | ~1-2 GB |

Note: Columnar storage is more efficient for analytical queries but has higher base memory.

### Concurrency
- Read-optimized, single-writer
- No ACID transactions
- Prepared statements for repeated queries

### Missing Features
- No transaction support
- Virtual filesystem integration not implemented for DO storage
- Parquet import from R2 not implemented in adapter
- Column type information not exposed in results

### Known Bugs
- Query result `columns` array is always empty (metadata extraction not implemented)

### Best For
- Analytical/OLAP workloads
- Large dataset aggregations
- Data warehouse style queries
- Complex analytical SQL (window functions, CTEs)

### Avoid For
- OLTP workloads (frequent small writes)
- Cold start sensitive applications
- Bundle size sensitive applications
- Applications requiring transactions
- Simple key-value operations

---

## tigerbeetle - Financial Ledger (WASM Port)

### Overview
High-performance financial accounting database using pure TypeScript `LedgerState` implementation with TigerBeetle semantics. Designed for double-entry bookkeeping.

### WASM Size
- **Bundle Impact**: None (pure TypeScript implementation)
- Uses `@dotdo/poc-tigerbeetle-do` package
- ~20-30KB gzipped (estimated)

### Cold Start
- **Typical Time**: <10ms
- Pure TypeScript, no WASM loading
- Memory allocation scales with account/transfer count

### Query Limitations
- **Domain-specific API only** (no SQL)
- Operations: `createAccounts`, `createTransfers`, `lookupAccounts`, `lookupTransfers`
- Filter by account ID and timestamp range
- **Maximum batch size: 8,190** (TigerBeetle protocol limit)
- Query by transfer requires knowing transfer IDs or account IDs
- No arbitrary field filtering

### Memory Usage
| Data Size | Approximate Memory |
|-----------|-------------------|
| 1,000 accounts | ~2-5 MB |
| 10,000 accounts | ~20-50 MB |
| 100,000 accounts | ~200-500 MB |

Memory scales with both accounts and transfers stored.

### Concurrency
- **Exactly-once processing semantics**
- Idempotent operations (duplicate creates with same fields succeed)
- Strict ACID guarantees
- Linked operations for atomic batches

### Missing Features
- No arbitrary SQL queries
- No full-text search
- No aggregation queries (must compute client-side)
- Account history query combines debit/credit queries (no unified query)
- **Persistence not implemented** (`restoreFromStorage` is a no-op)

### Known Bugs
- `getAccountTransfers` returns combined debit+credit transfers but deduplication is done client-side

### Best For
- Financial/accounting applications
- Double-entry bookkeeping systems
- High-throughput payment processing
- Applications requiring exactly-once semantics
- Audit-compliant financial records

### Avoid For
- General-purpose document storage
- Applications needing SQL queries
- Non-financial data models
- Scenarios requiring ad-hoc queries

---

## graphdb - Triple Store with WebSocket Hibernation

### Overview
Graph database with triple store (subject, predicate, object), designed for WebSocket connections with hibernation support. Production uses SQLite-backed DOs with R2 lakehouse storage.

### WASM Size
- **Bundle Impact**: None for in-memory benchmark adapter
- Production version uses SQLite (see sqlite limitations)

### Cold Start
- **Typical Time**: <10ms (in-memory adapter)
- Production: Depends on WebSocket connection establishment

### Query Limitations
- Triple-based queries: `getTriples(subject)`, `getTriplesByPredicate(predicate)`
- Simple query syntax: `type:TypeName` or `predicate:value`
- Graph traversal: `traverse()`, `reverseTraverse()`, `pathTraverse()`
- **No SPARQL support** (custom query format only)
- **Entity references detected by string prefix** (`entity:`, `thing-`, `user:`)
- Limited query optimization (full scan for complex patterns)

### Memory Usage
| Data Size | Approximate Memory |
|-----------|-------------------|
| 1,000 entities | ~5-10 MB |
| 10,000 entities | ~50-100 MB |
| 100,000 entities | ~500MB-1GB |

Memory includes triple store, predicate index, and reverse index.

### Concurrency
- Single-writer model
- No transactions
- Eventual consistency via WebSocket subscriptions (production)

### Missing Features
- No SPARQL query language
- No graph algorithms (shortest path, PageRank, etc.)
- `getTriplesByPredicate` not fully implemented in WebSocket client
- Statistics endpoints return zeros in WebSocket client mode
- No sharding in benchmark adapter (production uses ShardDO)

### Known Bugs
- WebSocket client `getTriplesByPredicate` returns empty array
- WebSocket client `count()` and `getStats()` return placeholder values

### Best For
- Knowledge graphs
- Entity-relationship modeling
- Applications needing graph traversal
- WebSocket-based real-time updates
- Linked data applications

### Avoid For
- Complex analytical queries
- Applications requiring SPARQL
- Large-scale graph analytics
- Scenarios needing graph algorithms

---

## sdb - Document/Graph Database

### Overview
Document/graph hybrid database with schema-based documents, graph relationships (`->` syntax), WebSocket subscriptions, and React hooks support.

### WASM Size
- **Bundle Impact**: None (pure TypeScript)
- ~25-35KB gzipped (estimated)

### Cold Start
- **Typical Time**: <5ms
- Pure TypeScript with schema definition

### Query Limitations
- Schema-defined field types: `string`, `text`, `number`, `boolean`, `date`, `json`
- Relationship syntax: `-> Type` (forward), `<- Type` (reverse)
- List options: `limit`, `offset`, `cursor`, `orderBy`, `order`
- Filter by equality only (no `$gt`, `$lt` operators)
- Server-side transforms: `.map()`, `.filter()`, `.slice()`
- **Pluralization is automatic** (e.g., `Thing` -> `things`)

### Memory Usage
| Data Size | Approximate Memory |
|-----------|-------------------|
| 1,000 docs | ~3-6 MB |
| 10,000 docs | ~30-60 MB |
| 100,000 docs | ~300-600 MB |

Includes documents, relationships, and reverse relationship indexes.

### Concurrency
- Batch operations via `batch().commit()`
- No ACID transactions (batch is not atomic)
- WebSocket subscriptions for real-time updates (production)

### Missing Features
- No complex query operators ($gt, $lt, $in, etc.)
- No aggregation pipeline
- Batch commit is not atomic (partial failures possible)
- WebSocket client needs full implementation
- No cursor-based pagination in adapter

### Known Bugs
- None documented in codebase

### Best For
- React/TypeScript applications
- Document stores with relationships
- Real-time applications (via WebSocket)
- Schema-first development
- Applications needing type-safe queries

### Avoid For
- Complex query filtering
- Aggregation-heavy workloads
- Applications requiring atomic batch operations
- Large-scale analytical queries

---

## @db4/mongo - MongoDB API on db4

### Overview
MongoDB-compatible API implemented on db4 backend. Pure TypeScript, zero WASM - optimized for cold start and bundle size.

### WASM Size
- **Bundle Impact**: None (pure TypeScript)
- Inherits db4's minimal footprint
- ~20-30KB gzipped (estimated)

### Cold Start
- **Typical Time**: <10ms
- No WASM loading
- Client connection is instant (`db4://memory`)

### Query Limitations
- MongoDB CRUD: `findOne`, `find`, `insertOne`, `insertMany`, `updateOne`, `updateMany`, `deleteOne`, `deleteMany`
- Query operators: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`
- Update operators: `$set`, `$inc`, `$push`, `$pull`
- Aggregation pipeline: `$match`, `$group`, `$sort`, `$limit`, `$lookup`, `$addFields`
- Projections (inclusion and exclusion)
- `distinct()` and `estimatedDocumentCount()` supported
- **Index creation supported** but performance depends on underlying db4 implementation

### Memory Usage
| Data Size | Approximate Memory |
|-----------|-------------------|
| 1,000 docs | ~2-4 MB |
| 10,000 docs | ~20-40 MB |
| 100,000 docs | ~200-400 MB |

### Concurrency
- Inherits db4 concurrency model
- No transactions (MongoDB sessions not implemented)
- `bulkWrite` operations are not atomic

### Missing Features
- No change streams
- No transactions/sessions
- No text search ($text operator)
- No geospatial queries ($near, $geoWithin)
- No `$expr` for aggregation expressions in queries
- Index performance may not match real MongoDB

### Known Bugs
- None documented in codebase

### Best For
- MongoDB-familiar developers
- Applications migrating from MongoDB
- Scenarios requiring MongoDB API compatibility
- Cold start sensitive MongoDB workloads

### Avoid For
- Applications needing MongoDB transactions
- Geospatial queries
- Full-text search
- Change stream requirements

---

## @dotdo/mongodb - MongoDB API on PostgreSQL

### Overview
MongoDB-compatible API implemented on PostgreSQL/DocumentDB backend using PGLite WASM. Combines MongoDB API with PostgreSQL's reliability.

### WASM Size
- **Bundle Impact**: ~2-3 MB gzipped (PGLite WASM)
- Same as postgres adapter

### Cold Start
- **Typical Time**: 100-300ms
- Inherits PGLite WASM loading time
- Connection string: `documentdb://pglite:memory`

### Query Limitations
- Same MongoDB API as @db4/mongo
- Indexes translated to PostgreSQL indexes
- **Better index performance** than @db4/mongo (PostgreSQL query planner)
- JSONB storage for documents
- Some MongoDB operators may have different performance characteristics

### Memory Usage
| Data Size | Approximate Memory |
|-----------|-------------------|
| 1,000 docs | ~15-25 MB |
| 10,000 docs | ~50-80 MB |
| 100,000 docs | ~300-500 MB |

Higher base memory due to PostgreSQL overhead.

### Concurrency
- PostgreSQL-backed transactions possible (not exposed in MongoDB API)
- ACID compliance at storage layer
- MongoDB API operations are not transactional

### Missing Features
- Same as @db4/mongo (no change streams, transactions, text search, geospatial)
- Higher cold start than @db4/mongo
- Larger bundle size

### Known Bugs
- None documented in codebase

### Best For
- MongoDB API with PostgreSQL reliability
- Applications needing better query performance for complex operations
- Scenarios where cold start is acceptable (200-300ms)
- Applications benefiting from JSONB indexing

### Avoid For
- Cold start sensitive applications
- Bundle size sensitive applications
- Simple document operations (db4/mongo is faster)
- Very large datasets

---

## Comparison Summary

| Database | WASM Size | Cold Start | SQL Support | Transaction Support | Best Use Case |
|----------|-----------|------------|-------------|---------------------|---------------|
| db4 | None | <5ms | Basic | No | Simple documents, fast cold start |
| evodb | None | <5ms | Basic | No (event-sourced) | Audit trails, schema evolution |
| postgres | 2-3MB | 100-300ms | Full | Yes | Complex SQL queries |
| sqlite | 500KB-1MB | 50-150ms | Full | Yes | Balanced SQL with smaller bundle |
| duckdb | 3-5MB | 200-500ms | Full (OLAP) | No | Analytics, large aggregations |
| tigerbeetle | None | <10ms | None (domain API) | Yes (batch) | Financial accounting |
| graphdb | None* | <10ms | None (graph) | No | Knowledge graphs, traversals |
| sdb | None | <5ms | None (schema) | No | Document/graph hybrid, React |
| @db4/mongo | None | <10ms | MongoDB API | No | MongoDB compatibility, fast |
| @dotdo/mongodb | 2-3MB | 100-300ms | MongoDB API | No | MongoDB API with PostgreSQL |

*Production graphdb uses SQLite

---

## General Recommendations

### For Minimal Cold Start (<20ms)
1. db4
2. evodb
3. sdb
4. tigerbeetle
5. @db4/mongo

### For Full SQL Support
1. postgres (PGLite) - Full PostgreSQL
2. sqlite (libsql) - Standard SQL with smaller bundle
3. duckdb - Best for analytics (no transactions)

### For Graph Operations
1. graphdb - Native triple store
2. sdb - Document/graph hybrid

### For Financial Applications
1. tigerbeetle - Purpose-built, exactly-once semantics

### For MongoDB Compatibility
1. @db4/mongo - Fast cold start, pure TypeScript
2. @dotdo/mongodb - Better query performance, higher cold start
