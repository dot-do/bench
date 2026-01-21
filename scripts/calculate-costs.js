#!/usr/bin/env node

/**
 * Cost Analysis Script
 *
 * Calculates $/1M operations for each database backend.
 *
 * Cloudflare Pricing (2026):
 * - DO SQLite rows read:    $0.001 per 1M rows
 * - DO SQLite rows written: $1.00 per 1M rows
 * - DO duration:            $0.001 per 1M GB-seconds
 * - Workers requests:       $0.30 per 1M requests
 * - R2 Class A (write):     $4.50 per 1M requests
 * - R2 Class B (read):      $0.36 per 1M requests
 * - R2 Storage:             $0.015 per GB/month
 *
 * Key insight: Row cost is per-row, not per-byte!
 * 1KB row = same cost as 2MB row
 * => Optimal: Pack data into 2MB blobs
 */

import { readFileSync, writeFileSync } from 'fs'
import { join, dirname } from 'path'
import { fileURLToPath } from 'url'

const __dirname = dirname(fileURLToPath(import.meta.url))
const resultsDir = join(__dirname, '..', 'results')

// Cloudflare pricing (per 1M)
const PRICING = {
  doRowsRead: 0.001,      // $ per 1M rows read
  doRowsWritten: 1.00,    // $ per 1M rows written
  doDuration: 0.001,      // $ per 1M GB-seconds
  workersRequests: 0.30,  // $ per 1M requests
  r2ClassA: 4.50,         // $ per 1M writes
  r2ClassB: 0.36,         // $ per 1M reads
  r2StorageGB: 0.015,     // $ per GB/month
}

// Scenarios
const scenarios = [
  {
    name: 'Point Lookup (1 row)',
    description: 'Read single row by primary key',
    operations: {
      naive: { rowsRead: 1 },      // 1 row
      optimized: { rowsRead: 1 },  // Same (can't optimize single row)
    }
  },
  {
    name: 'Range Scan (100 rows)',
    description: 'Read 100 rows matching filter',
    operations: {
      naive: { rowsRead: 100 },         // 100 individual rows
      optimized: { rowsRead: 1 },       // 1 blob containing 100 rows (2MB packed)
    }
  },
  {
    name: 'Write 100 rows',
    description: 'Insert 100 new rows',
    operations: {
      naive: { rowsWritten: 100 },      // 100 individual rows
      optimized: { rowsWritten: 1 },    // 1 blob containing 100 rows
    }
  },
  {
    name: 'Write 10,000 rows',
    description: 'Bulk insert 10,000 rows',
    operations: {
      naive: { rowsWritten: 10000 },    // 10,000 individual rows
      optimized: { rowsWritten: 5 },    // 5 blobs × 2,000 rows each (2MB max)
    }
  },
  {
    name: 'Analytics Query (1M rows)',
    description: 'Aggregate over 1M rows',
    operations: {
      naive: { rowsRead: 1000000 },     // 1M individual rows (ouch!)
      optimized: { rowsRead: 500 },     // 500 columnar blocks × 2,000 rows each
    }
  },
  {
    name: 'CDC to Lakehouse (10K events)',
    description: 'Stream 10K events to R2/Iceberg',
    operations: {
      naive: { rowsRead: 10000, r2Writes: 10000 },  // Per-event R2 writes
      optimized: { rowsRead: 5, r2Writes: 1 },      // Batched to 1 Parquet file
    }
  },
]

function calculateCost(operations) {
  let cost = 0

  if (operations.rowsRead) {
    cost += (operations.rowsRead / 1_000_000) * PRICING.doRowsRead
  }
  if (operations.rowsWritten) {
    cost += (operations.rowsWritten / 1_000_000) * PRICING.doRowsWritten
  }
  if (operations.r2Writes) {
    cost += (operations.r2Writes / 1_000_000) * PRICING.r2ClassA
  }
  if (operations.r2Reads) {
    cost += (operations.r2Reads / 1_000_000) * PRICING.r2ClassB
  }

  return cost
}

function main() {
  console.log('Cost Analysis: Naive vs 2MB Blob Optimization\n')
  console.log('=' .repeat(80))

  const results = {
    timestamp: new Date().toISOString(),
    pricing: PRICING,
    scenarios: [],
  }

  for (const scenario of scenarios) {
    const naiveCost = calculateCost(scenario.operations.naive)
    const optimizedCost = calculateCost(scenario.operations.optimized)
    const savings = ((naiveCost - optimizedCost) / naiveCost) * 100

    const result = {
      name: scenario.name,
      description: scenario.description,
      naive: {
        operations: scenario.operations.naive,
        costPer1M: naiveCost.toFixed(6),
      },
      optimized: {
        operations: scenario.operations.optimized,
        costPer1M: optimizedCost.toFixed(6),
      },
      savingsPercent: savings.toFixed(1),
      multiplier: (naiveCost / optimizedCost).toFixed(1),
    }

    results.scenarios.push(result)

    console.log(`\n${scenario.name}`)
    console.log(`  ${scenario.description}`)
    console.log(`  Naive:     $${naiveCost.toFixed(6)} per 1M operations`)
    console.log(`  Optimized: $${optimizedCost.toFixed(6)} per 1M operations`)
    console.log(`  Savings:   ${savings.toFixed(1)}% (${result.multiplier}x cheaper)`)
  }

  // Summary: Which database uses which strategy?
  console.log('\n\n' + '='.repeat(80))
  console.log('Database Optimization Strategies\n')

  const strategies = {
    db4: {
      strategy: 'Vortex columnar blocks (2MB)',
      blobPacking: true,
      description: 'Packs columnar data into 2MB DO SQLite blobs',
    },
    evodb: {
      strategy: 'Columnar shredding (2MB blocks)',
      blobPacking: true,
      description: 'Shreds JSON to columns, packs into 2MB blocks',
    },
    postgres: {
      strategy: 'VFS with 2MB chunks (512 × 8KB pages)',
      blobPacking: true,
      description: 'Packs 512 PostgreSQL pages into 2MB DO blobs',
    },
    sqlite: {
      strategy: 'VFS with 2MB chunks (512 × 4KB pages)',
      blobPacking: true,
      description: 'Packs 512 SQLite pages into 2MB DO blobs',
    },
    duckdb: {
      strategy: 'Buffer registration (2MB chunks)',
      blobPacking: true,
      description: 'Registers buffers as 2MB blobs, columnar format',
    },
  }

  console.log('| Database | Blob Packing | Strategy |')
  console.log('|----------|--------------|----------|')
  for (const [name, info] of Object.entries(strategies)) {
    const packing = info.blobPacking === true ? 'Yes (2MB)' :
                    info.blobPacking === 'partial' ? 'Partial' : 'No'
    console.log(`| ${name.padEnd(8)} | ${packing.padEnd(12)} | ${info.strategy} |`)
  }

  // Monthly cost projections
  console.log('\n\nMonthly Cost Projections (Active SaaS App)\n')
  console.log('Assumptions: 10K users, 1K queries/day each, 100 writes/day each\n')

  const monthlyQueries = 10000 * 1000 * 30  // 300M reads/month
  const monthlyWrites = 10000 * 100 * 30    // 30M writes/month

  const monthlyNaive = {
    reads: (monthlyQueries / 1_000_000) * PRICING.doRowsRead,
    writes: (monthlyWrites / 1_000_000) * PRICING.doRowsWritten,
  }
  monthlyNaive.total = monthlyNaive.reads + monthlyNaive.writes

  const monthlyOptimized = {
    reads: (monthlyQueries / 100 / 1_000_000) * PRICING.doRowsRead,  // 100x fewer rows (blob packed)
    writes: (monthlyWrites / 100 / 1_000_000) * PRICING.doRowsWritten, // 100x fewer rows
  }
  monthlyOptimized.total = monthlyOptimized.reads + monthlyOptimized.writes

  console.log(`Naive (row-per-row):      $${monthlyNaive.total.toFixed(2)}/month`)
  console.log(`Optimized (2MB blobs):    $${monthlyOptimized.total.toFixed(2)}/month`)
  console.log(`Savings:                  $${(monthlyNaive.total - monthlyOptimized.total).toFixed(2)}/month`)

  results.monthlyProjection = {
    assumptions: '10K users, 1K queries/day, 100 writes/day',
    naive: monthlyNaive,
    optimized: monthlyOptimized,
  }

  // Write results
  writeFileSync(
    join(resultsDir, 'cost-analysis.json'),
    JSON.stringify(results, null, 2)
  )
  console.log(`\nResults written to results/cost-analysis.json`)
}

main()
