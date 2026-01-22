/**
 * Financial/Accounting Benchmarks
 *
 * Compares TigerBeetle against traditional databases for financial workloads:
 * - Account creation (single and batch)
 * - Transfer processing (double-entry bookkeeping)
 * - Balance lookups
 * - Transaction throughput
 * - ACID compliance under load
 *
 * TigerBeetle is purpose-built for financial accounting with:
 * - Strict double-entry bookkeeping
 * - ACID guarantees
 * - Exactly-once processing
 * - Up to 1M+ TPS
 *
 * Running these benchmarks:
 *
 * With TigerBeetle cluster (recommended):
 *   # Start TigerBeetle cluster first (see README)
 *   pnpm bench:financial
 *
 * Without TigerBeetle (simulated mode for CI):
 *   TIGERBEETLE_SIMULATED=true pnpm bench:financial
 *
 * @see https://tigerbeetle.com/
 */

import { bench, describe, beforeAll, afterAll } from 'vitest'
import type { TigerBeetleStore } from '../databases/tigerbeetle'
import type { PostgresStore } from '../databases/postgres'
import type { SQLiteStore } from '../databases/sqlite'
import type { DB4Store } from '../databases/db4'

// ============================================================================
// Benchmark Configuration
// ============================================================================

const ITERATIONS = {
  single: 100,
  batch: 10,
  throughput: 5,
}

const BATCH_SIZES = {
  small: 100,
  medium: 1000,
  large: 8190, // TigerBeetle max batch size
}

// ============================================================================
// Store Instances (lazy-loaded and cached)
// ============================================================================

let tigerbeetleStore: TigerBeetleStore | null = null
let tigerbeetleAccountIds: bigint[] = []

let postgresStore: PostgresStore | null = null
let postgresAccountIds: string[] = []

let sqliteStore: SQLiteStore | null = null
let sqliteAccountIds: string[] = []

let db4Store: DB4Store | null = null
let db4AccountIds: string[] = []

// ============================================================================
// Store Initialization Helpers
// ============================================================================

async function getTigerBeetleStore(): Promise<TigerBeetleStore> {
  if (!tigerbeetleStore) {
    const { createTigerBeetleStore, seedTestData } = await import('../databases/tigerbeetle')
    tigerbeetleStore = await createTigerBeetleStore()
    const result = await seedTestData(tigerbeetleStore, '10mb')
    tigerbeetleAccountIds = result.accountIds
  }
  return tigerbeetleStore
}

async function getPostgresStore(): Promise<PostgresStore> {
  if (!postgresStore) {
    const { createPostgresStore } = await import('../databases/postgres')
    postgresStore = await createPostgresStore()

    // Create accounts table for financial benchmarks
    await postgresStore.query(`
      CREATE TABLE IF NOT EXISTS accounts (
        id TEXT PRIMARY KEY,
        ledger INTEGER NOT NULL,
        code INTEGER NOT NULL,
        debits_pending BIGINT NOT NULL DEFAULT 0,
        debits_posted BIGINT NOT NULL DEFAULT 0,
        credits_pending BIGINT NOT NULL DEFAULT 0,
        credits_posted BIGINT NOT NULL DEFAULT 0,
        user_data TEXT,
        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
      )
    `)

    await postgresStore.query(`
      CREATE TABLE IF NOT EXISTS transfers (
        id TEXT PRIMARY KEY,
        debit_account_id TEXT NOT NULL REFERENCES accounts(id),
        credit_account_id TEXT NOT NULL REFERENCES accounts(id),
        amount BIGINT NOT NULL,
        ledger INTEGER NOT NULL,
        code INTEGER NOT NULL,
        user_data TEXT,
        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
      )
    `)

    await postgresStore.query(`CREATE INDEX IF NOT EXISTS idx_transfers_debit ON transfers(debit_account_id)`)
    await postgresStore.query(`CREATE INDEX IF NOT EXISTS idx_transfers_credit ON transfers(credit_account_id)`)

    // Seed accounts
    postgresAccountIds = []
    await postgresStore.transaction(async (tx) => {
      for (let i = 0; i < 1000; i++) {
        const id = `account-${String(i).padStart(6, '0')}`
        postgresAccountIds.push(id)
        await tx.query(
          'INSERT INTO accounts (id, ledger, code, credits_posted) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING',
          [id, 1, 1001, 10000 * 100] // $10,000 initial balance
        )
      }
    })
  }
  return postgresStore
}

async function getSQLiteStore(): Promise<SQLiteStore> {
  if (!sqliteStore) {
    const { createSQLiteStore } = await import('../databases/sqlite')
    sqliteStore = await createSQLiteStore()

    // Create accounts table for financial benchmarks
    await sqliteStore.query(`
      CREATE TABLE IF NOT EXISTS accounts (
        id TEXT PRIMARY KEY,
        ledger INTEGER NOT NULL,
        code INTEGER NOT NULL,
        debits_pending INTEGER NOT NULL DEFAULT 0,
        debits_posted INTEGER NOT NULL DEFAULT 0,
        credits_pending INTEGER NOT NULL DEFAULT 0,
        credits_posted INTEGER NOT NULL DEFAULT 0,
        user_data TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      )
    `)

    await sqliteStore.query(`
      CREATE TABLE IF NOT EXISTS transfers (
        id TEXT PRIMARY KEY,
        debit_account_id TEXT NOT NULL,
        credit_account_id TEXT NOT NULL,
        amount INTEGER NOT NULL,
        ledger INTEGER NOT NULL,
        code INTEGER NOT NULL,
        user_data TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (debit_account_id) REFERENCES accounts(id),
        FOREIGN KEY (credit_account_id) REFERENCES accounts(id)
      )
    `)

    await sqliteStore.query(`CREATE INDEX IF NOT EXISTS idx_transfers_debit ON transfers(debit_account_id)`)
    await sqliteStore.query(`CREATE INDEX IF NOT EXISTS idx_transfers_credit ON transfers(credit_account_id)`)

    // Seed accounts
    sqliteAccountIds = []
    await sqliteStore.transaction(async (tx) => {
      for (let i = 0; i < 1000; i++) {
        const id = `account-${String(i).padStart(6, '0')}`
        sqliteAccountIds.push(id)
        await tx.query('INSERT OR IGNORE INTO accounts (id, ledger, code, credits_posted) VALUES (?, ?, ?, ?)', [
          id,
          1,
          1001,
          10000 * 100, // $10,000 initial balance
        ])
      }
    })
  }
  return sqliteStore
}

async function getDB4Store(): Promise<DB4Store> {
  if (!db4Store) {
    const { createDB4Store } = await import('../databases/db4')
    db4Store = await createDB4Store()

    // Seed accounts
    db4AccountIds = []
    for (let i = 0; i < 1000; i++) {
      const id = `account-${String(i).padStart(6, '0')}`
      db4AccountIds.push(id)
      await db4Store.set('accounts', id, {
        name: `Account ${i}`,
        status: 'active',
        created_at: new Date().toISOString(),
      } as Omit<import('../databases/db4').Thing, 'id'>)
    }
  }
  return db4Store
}

// ============================================================================
// UUID/ID Generation Helpers
// ============================================================================

let idCounter = 0
function generateUUID(): string {
  idCounter++
  return `id-${Date.now()}-${idCounter}-${Math.random().toString(36).slice(2)}`
}

// ============================================================================
// Benchmarks: Account Operations
// ============================================================================

describe('Financial - Account Creation (Single)', () => {
  bench(
    'tigerbeetle create account',
    async () => {
      const store = await getTigerBeetleStore()
      const { generateId } = await import('../databases/tigerbeetle')
      await store.createAccounts([
        {
          id: generateId(),
          ledger: 1,
          code: 1001,
        },
      ])
    },
    { iterations: ITERATIONS.single }
  )

  bench(
    'postgres create account',
    async () => {
      const store = await getPostgresStore()
      const id = generateUUID()
      await store.query('INSERT INTO accounts (id, ledger, code) VALUES ($1, $2, $3)', [id, 1, 1001])
    },
    { iterations: ITERATIONS.single }
  )

  bench(
    'sqlite create account',
    async () => {
      const store = await getSQLiteStore()
      const id = generateUUID()
      await store.query('INSERT INTO accounts (id, ledger, code) VALUES (?, ?, ?)', [id, 1, 1001])
    },
    { iterations: ITERATIONS.single }
  )

  bench(
    'db4 create account',
    async () => {
      const store = await getDB4Store()
      const id = generateUUID()
      await store.set('accounts', id, {
        name: `Account ${id}`,
        status: 'active',
        created_at: new Date().toISOString(),
      } as Omit<import('../databases/db4').Thing, 'id'>)
    },
    { iterations: ITERATIONS.single }
  )
})

describe('Financial - Account Creation (Batch 1000)', () => {
  bench(
    'tigerbeetle batch create 1000 accounts',
    async () => {
      const store = await getTigerBeetleStore()
      const { generateId } = await import('../databases/tigerbeetle')
      const accounts = Array.from({ length: BATCH_SIZES.medium }, () => ({
        id: generateId(),
        ledger: 1,
        code: 1001,
      }))
      await store.createAccountsBatch(accounts)
    },
    { iterations: ITERATIONS.batch }
  )

  bench(
    'postgres batch create 1000 accounts',
    async () => {
      const store = await getPostgresStore()
      await store.transaction(async (tx) => {
        for (let i = 0; i < BATCH_SIZES.medium; i++) {
          const id = generateUUID()
          await tx.query('INSERT INTO accounts (id, ledger, code) VALUES ($1, $2, $3)', [id, 1, 1001])
        }
      })
    },
    { iterations: ITERATIONS.batch }
  )

  bench(
    'sqlite batch create 1000 accounts',
    async () => {
      const store = await getSQLiteStore()
      await store.transaction(async (tx) => {
        for (let i = 0; i < BATCH_SIZES.medium; i++) {
          const id = generateUUID()
          await tx.query('INSERT INTO accounts (id, ledger, code) VALUES (?, ?, ?)', [id, 1, 1001])
        }
      })
    },
    { iterations: ITERATIONS.batch }
  )

  bench(
    'db4 batch create 1000 accounts',
    async () => {
      const store = await getDB4Store()
      const promises: Promise<void>[] = []
      for (let i = 0; i < BATCH_SIZES.medium; i++) {
        const id = generateUUID()
        promises.push(
          store.set('accounts', id, {
            name: `Account ${id}`,
            status: 'active',
            created_at: new Date().toISOString(),
          } as Omit<import('../databases/db4').Thing, 'id'>)
        )
      }
      await Promise.all(promises)
    },
    { iterations: ITERATIONS.batch }
  )
})

// ============================================================================
// Benchmarks: Balance Lookups
// ============================================================================

describe('Financial - Balance Lookup', () => {
  bench(
    'tigerbeetle balance lookup',
    async () => {
      const store = await getTigerBeetleStore()
      const accountId = tigerbeetleAccountIds[Math.floor(Math.random() * tigerbeetleAccountIds.length)]
      await store.lookupAccounts([accountId])
    },
    { iterations: ITERATIONS.single }
  )

  bench(
    'postgres balance lookup',
    async () => {
      const store = await getPostgresStore()
      const accountId = postgresAccountIds[Math.floor(Math.random() * postgresAccountIds.length)]
      await store.query(
        'SELECT credits_posted - debits_posted as balance FROM accounts WHERE id = $1',
        [accountId]
      )
    },
    { iterations: ITERATIONS.single }
  )

  bench(
    'sqlite balance lookup',
    async () => {
      const store = await getSQLiteStore()
      const accountId = sqliteAccountIds[Math.floor(Math.random() * sqliteAccountIds.length)]
      await store.query('SELECT credits_posted - debits_posted as balance FROM accounts WHERE id = ?', [
        accountId,
      ])
    },
    { iterations: ITERATIONS.single }
  )

  bench(
    'db4 balance lookup',
    async () => {
      const store = await getDB4Store()
      const accountId = db4AccountIds[Math.floor(Math.random() * db4AccountIds.length)]
      const account = await store.get('accounts', accountId)
      // Calculate balance (would be done by the caller)
      const _balance = account ? (account as any).credits_posted - (account as any).debits_posted : 0
    },
    { iterations: ITERATIONS.single }
  )
})

describe('Financial - Batch Balance Lookup (100 accounts)', () => {
  bench(
    'tigerbeetle batch balance lookup',
    async () => {
      const store = await getTigerBeetleStore()
      const ids = tigerbeetleAccountIds.slice(0, 100)
      await store.lookupAccounts(ids)
    },
    { iterations: ITERATIONS.single }
  )

  bench(
    'postgres batch balance lookup',
    async () => {
      const store = await getPostgresStore()
      const ids = postgresAccountIds.slice(0, 100)
      const placeholders = ids.map((_, i) => `$${i + 1}`).join(',')
      await store.query(
        `SELECT id, credits_posted - debits_posted as balance FROM accounts WHERE id IN (${placeholders})`,
        ids
      )
    },
    { iterations: ITERATIONS.single }
  )

  bench(
    'sqlite batch balance lookup',
    async () => {
      const store = await getSQLiteStore()
      const ids = sqliteAccountIds.slice(0, 100)
      const placeholders = ids.map(() => '?').join(',')
      await store.query(
        `SELECT id, credits_posted - debits_posted as balance FROM accounts WHERE id IN (${placeholders})`,
        ids
      )
    },
    { iterations: ITERATIONS.single }
  )

  bench(
    'db4 batch balance lookup',
    async () => {
      const store = await getDB4Store()
      const ids = db4AccountIds.slice(0, 100)
      await Promise.all(ids.map((id) => store.get('accounts', id)))
    },
    { iterations: ITERATIONS.single }
  )
})

// ============================================================================
// Benchmarks: Transfer Processing (Double-Entry)
// ============================================================================

describe('Financial - Transfer Processing (Double-Entry)', () => {
  bench(
    'tigerbeetle transfer (double-entry)',
    async () => {
      const store = await getTigerBeetleStore()
      const { generateId } = await import('../databases/tigerbeetle')
      const fromIdx = Math.floor(Math.random() * tigerbeetleAccountIds.length)
      let toIdx = Math.floor(Math.random() * tigerbeetleAccountIds.length)
      while (toIdx === fromIdx) toIdx = Math.floor(Math.random() * tigerbeetleAccountIds.length)

      await store.createTransfers([
        {
          id: generateId(),
          debit_account_id: tigerbeetleAccountIds[fromIdx],
          credit_account_id: tigerbeetleAccountIds[toIdx],
          amount: 100n, // $1.00
          ledger: 1,
          code: 1,
        },
      ])
    },
    { iterations: ITERATIONS.single }
  )

  bench(
    'postgres transfer (transaction)',
    async () => {
      const store = await getPostgresStore()
      const fromIdx = Math.floor(Math.random() * postgresAccountIds.length)
      let toIdx = Math.floor(Math.random() * postgresAccountIds.length)
      while (toIdx === fromIdx) toIdx = Math.floor(Math.random() * postgresAccountIds.length)

      const fromId = postgresAccountIds[fromIdx]
      const toId = postgresAccountIds[toIdx]
      const amount = 100 // $1.00

      await store.transaction(async (tx) => {
        // Debit source account
        await tx.query('UPDATE accounts SET debits_posted = debits_posted + $1 WHERE id = $2', [amount, fromId])
        // Credit destination account
        await tx.query('UPDATE accounts SET credits_posted = credits_posted + $1 WHERE id = $2', [amount, toId])
        // Record transfer
        await tx.query(
          'INSERT INTO transfers (id, debit_account_id, credit_account_id, amount, ledger, code) VALUES ($1, $2, $3, $4, $5, $6)',
          [generateUUID(), fromId, toId, amount, 1, 1]
        )
      })
    },
    { iterations: ITERATIONS.single }
  )

  bench(
    'sqlite transfer (transaction)',
    async () => {
      const store = await getSQLiteStore()
      const fromIdx = Math.floor(Math.random() * sqliteAccountIds.length)
      let toIdx = Math.floor(Math.random() * sqliteAccountIds.length)
      while (toIdx === fromIdx) toIdx = Math.floor(Math.random() * sqliteAccountIds.length)

      const fromId = sqliteAccountIds[fromIdx]
      const toId = sqliteAccountIds[toIdx]
      const amount = 100 // $1.00

      await store.transaction(async (tx) => {
        // Debit source account
        await tx.query('UPDATE accounts SET debits_posted = debits_posted + ? WHERE id = ?', [amount, fromId])
        // Credit destination account
        await tx.query('UPDATE accounts SET credits_posted = credits_posted + ? WHERE id = ?', [amount, toId])
        // Record transfer
        await tx.query(
          'INSERT INTO transfers (id, debit_account_id, credit_account_id, amount, ledger, code) VALUES (?, ?, ?, ?, ?, ?)',
          [generateUUID(), fromId, toId, amount, 1, 1]
        )
      })
    },
    { iterations: ITERATIONS.single }
  )
})

// ============================================================================
// Benchmarks: Throughput (Batch Transfers)
// ============================================================================

describe('Financial - Throughput (1000 transfers)', () => {
  bench(
    'tigerbeetle 1000 transfers',
    async () => {
      const store = await getTigerBeetleStore()
      const { generateId } = await import('../databases/tigerbeetle')

      const transfers = Array.from({ length: 1000 }, () => {
        const fromIdx = Math.floor(Math.random() * tigerbeetleAccountIds.length)
        let toIdx = Math.floor(Math.random() * tigerbeetleAccountIds.length)
        while (toIdx === fromIdx) toIdx = Math.floor(Math.random() * tigerbeetleAccountIds.length)

        return {
          id: generateId(),
          debit_account_id: tigerbeetleAccountIds[fromIdx],
          credit_account_id: tigerbeetleAccountIds[toIdx],
          amount: BigInt(Math.floor(Math.random() * 1000) + 1),
          ledger: 1,
          code: 1,
        }
      })

      await store.createTransfersBatch(transfers)
    },
    { iterations: ITERATIONS.throughput }
  )

  bench(
    'postgres 1000 transfers',
    async () => {
      const store = await getPostgresStore()

      await store.transaction(async (tx) => {
        for (let i = 0; i < 1000; i++) {
          const fromIdx = Math.floor(Math.random() * postgresAccountIds.length)
          let toIdx = Math.floor(Math.random() * postgresAccountIds.length)
          while (toIdx === fromIdx) toIdx = Math.floor(Math.random() * postgresAccountIds.length)

          const fromId = postgresAccountIds[fromIdx]
          const toId = postgresAccountIds[toIdx]
          const amount = Math.floor(Math.random() * 1000) + 1

          await tx.query('UPDATE accounts SET debits_posted = debits_posted + $1 WHERE id = $2', [amount, fromId])
          await tx.query('UPDATE accounts SET credits_posted = credits_posted + $1 WHERE id = $2', [amount, toId])
          await tx.query(
            'INSERT INTO transfers (id, debit_account_id, credit_account_id, amount, ledger, code) VALUES ($1, $2, $3, $4, $5, $6)',
            [generateUUID(), fromId, toId, amount, 1, 1]
          )
        }
      })
    },
    { iterations: ITERATIONS.throughput }
  )

  bench(
    'sqlite 1000 transfers',
    async () => {
      const store = await getSQLiteStore()

      await store.transaction(async (tx) => {
        for (let i = 0; i < 1000; i++) {
          const fromIdx = Math.floor(Math.random() * sqliteAccountIds.length)
          let toIdx = Math.floor(Math.random() * sqliteAccountIds.length)
          while (toIdx === fromIdx) toIdx = Math.floor(Math.random() * sqliteAccountIds.length)

          const fromId = sqliteAccountIds[fromIdx]
          const toId = sqliteAccountIds[toIdx]
          const amount = Math.floor(Math.random() * 1000) + 1

          await tx.query('UPDATE accounts SET debits_posted = debits_posted + ? WHERE id = ?', [amount, fromId])
          await tx.query('UPDATE accounts SET credits_posted = credits_posted + ? WHERE id = ?', [amount, toId])
          await tx.query(
            'INSERT INTO transfers (id, debit_account_id, credit_account_id, amount, ledger, code) VALUES (?, ?, ?, ?, ?, ?)',
            [generateUUID(), fromId, toId, amount, 1, 1]
          )
        }
      })
    },
    { iterations: ITERATIONS.throughput }
  )
})

describe('Financial - High Throughput (10000 transfers)', () => {
  bench(
    'tigerbeetle 10000 transfers',
    async () => {
      const store = await getTigerBeetleStore()
      const { generateId } = await import('../databases/tigerbeetle')

      const transfers = Array.from({ length: 10000 }, () => {
        const fromIdx = Math.floor(Math.random() * tigerbeetleAccountIds.length)
        let toIdx = Math.floor(Math.random() * tigerbeetleAccountIds.length)
        while (toIdx === fromIdx) toIdx = Math.floor(Math.random() * tigerbeetleAccountIds.length)

        return {
          id: generateId(),
          debit_account_id: tigerbeetleAccountIds[fromIdx],
          credit_account_id: tigerbeetleAccountIds[toIdx],
          amount: BigInt(Math.floor(Math.random() * 1000) + 1),
          ledger: 1,
          code: 1,
        }
      })

      await store.createTransfersBatch(transfers)
    },
    { iterations: ITERATIONS.throughput }
  )

  bench(
    'postgres 10000 transfers',
    async () => {
      const store = await getPostgresStore()

      await store.transaction(async (tx) => {
        for (let i = 0; i < 10000; i++) {
          const fromIdx = Math.floor(Math.random() * postgresAccountIds.length)
          let toIdx = Math.floor(Math.random() * postgresAccountIds.length)
          while (toIdx === fromIdx) toIdx = Math.floor(Math.random() * postgresAccountIds.length)

          const fromId = postgresAccountIds[fromIdx]
          const toId = postgresAccountIds[toIdx]
          const amount = Math.floor(Math.random() * 1000) + 1

          await tx.query('UPDATE accounts SET debits_posted = debits_posted + $1 WHERE id = $2', [amount, fromId])
          await tx.query('UPDATE accounts SET credits_posted = credits_posted + $1 WHERE id = $2', [amount, toId])
          await tx.query(
            'INSERT INTO transfers (id, debit_account_id, credit_account_id, amount, ledger, code) VALUES ($1, $2, $3, $4, $5, $6)',
            [generateUUID(), fromId, toId, amount, 1, 1]
          )
        }
      })
    },
    { iterations: ITERATIONS.throughput }
  )
})

// ============================================================================
// Benchmarks: Account History
// ============================================================================

describe('Financial - Account History', () => {
  bench(
    'tigerbeetle get account transfers (last 100)',
    async () => {
      const store = await getTigerBeetleStore()
      const accountId = tigerbeetleAccountIds[Math.floor(Math.random() * tigerbeetleAccountIds.length)]
      await store.getAccountTransfers({
        account_id: accountId,
        timestamp_min: 0n,
        timestamp_max: BigInt(Number.MAX_SAFE_INTEGER),
        limit: 100,
        flags: 0,
      })
    },
    { iterations: ITERATIONS.single }
  )

  bench(
    'postgres get account transfers (last 100)',
    async () => {
      const store = await getPostgresStore()
      const accountId = postgresAccountIds[Math.floor(Math.random() * postgresAccountIds.length)]
      await store.query(
        `SELECT * FROM transfers
         WHERE debit_account_id = $1 OR credit_account_id = $1
         ORDER BY created_at DESC LIMIT 100`,
        [accountId]
      )
    },
    { iterations: ITERATIONS.single }
  )

  bench(
    'sqlite get account transfers (last 100)',
    async () => {
      const store = await getSQLiteStore()
      const accountId = sqliteAccountIds[Math.floor(Math.random() * sqliteAccountIds.length)]
      await store.query(
        `SELECT * FROM transfers
         WHERE debit_account_id = ? OR credit_account_id = ?
         ORDER BY created_at DESC LIMIT 100`,
        [accountId, accountId]
      )
    },
    { iterations: ITERATIONS.single }
  )
})

// ============================================================================
// Cleanup
// ============================================================================

afterAll(async () => {
  if (tigerbeetleStore) {
    await tigerbeetleStore.close()
  }
  if (postgresStore) {
    await postgresStore.close()
  }
  if (sqliteStore) {
    await sqliteStore.close()
  }
  if (db4Store) {
    await db4Store.close()
  }
})
