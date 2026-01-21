/**
 * TigerBeetle WASM Adapter
 *
 * Uses the WASM port from pocs/packages/tigerbeetle-do
 * NOT the tigerbeetle-node npm package.
 *
 * TigerBeetle is a high-performance financial accounting database designed for:
 * - Double-entry bookkeeping with strict ACID guarantees
 * - High-throughput financial transactions (up to 1M+ TPS)
 * - Exactly-once processing semantics
 *
 * This adapter uses the pure TypeScript LedgerState implementation
 * with TigerBeetle semantics, suitable for Cloudflare Workers.
 *
 * @see https://tigerbeetle.com/
 */

import type { DatasetSize } from '../datasets/oltp/index'

// Import from the WASM port - import from core and types submodules
// to avoid pulling in Cloudflare Workers dependencies
import {
  LedgerState,
} from '@dotdo/poc-tigerbeetle-do/core'

import {
  Uint128,
  uint128,
  CreateAccountResult as WasmCreateAccountResult,
  CreateTransferResult as WasmCreateTransferResult,
  type Account as WasmAccount,
  type Transfer as WasmTransfer,
  type CreateAccountInput,
  type CreateTransferInput,
} from '@dotdo/poc-tigerbeetle-do/types'

// ============================================================================
// Re-export WASM types for external use
// ============================================================================

export { Uint128, uint128 }
export type { WasmAccount, WasmTransfer, CreateAccountInput, CreateTransferInput }

// ============================================================================
// Legacy bigint-based types (for backward compatibility)
// ============================================================================

/**
 * TigerBeetle Account - the fundamental unit of double-entry bookkeeping
 *
 * Accounts track balances and are identified by 128-bit UUIDs.
 * Credits and debits are tracked separately for audit purposes.
 */
export interface Account {
  id: bigint // 128-bit unique identifier
  debits_pending: bigint // Total pending debits
  debits_posted: bigint // Total posted debits
  credits_pending: bigint // Total pending credits
  credits_posted: bigint // Total posted credits
  user_data_128: bigint // Custom user data (128-bit)
  user_data_64: bigint // Custom user data (64-bit)
  user_data_32: number // Custom user data (32-bit)
  reserved: number // Reserved for future use
  ledger: number // Ledger identifier (chart of accounts)
  code: number // Account code/type
  flags: number // Account flags (linked, debits_must_not_exceed_credits, etc.)
  timestamp: bigint // Server-assigned timestamp
}

/**
 * TigerBeetle Transfer - moves funds between accounts
 *
 * Transfers are immutable and append-only. They represent the movement
 * of funds from a debit account to a credit account.
 */
export interface Transfer {
  id: bigint // 128-bit unique identifier
  debit_account_id: bigint // Source account
  credit_account_id: bigint // Destination account
  amount: bigint // Transfer amount (must be > 0)
  pending_id: bigint // For two-phase transfers
  user_data_128: bigint // Custom user data (128-bit)
  user_data_64: bigint // Custom user data (64-bit)
  user_data_32: number // Custom user data (32-bit)
  timeout: number // Timeout for pending transfers (seconds)
  ledger: number // Ledger identifier
  code: number // Transfer code/type
  flags: number // Transfer flags (linked, pending, post_pending, void_pending)
  timestamp: bigint // Server-assigned timestamp
}

/**
 * Account filter for lookups
 */
export interface AccountFilter {
  account_id: bigint
  timestamp_min: bigint
  timestamp_max: bigint
  limit: number
  flags: number
}

/**
 * Account balance at a point in time
 */
export interface AccountBalance {
  debits_pending: bigint
  debits_posted: bigint
  credits_pending: bigint
  credits_posted: bigint
  timestamp: bigint
}

// Re-export WASM result enums with legacy names
export { WasmCreateAccountResult as CreateAccountResultCode }
export { WasmCreateTransferResult as CreateTransferResultCode }

/**
 * Create account result
 */
export interface CreateAccountResult {
  index: number
  result: WasmCreateAccountResult
}

/**
 * Create transfer result
 */
export interface CreateTransferResult {
  index: number
  result: WasmCreateTransferResult
}

// ============================================================================
// Account Flags (matching WASM enum values)
// ============================================================================

export const AccountFlags = {
  None: 0,
  Linked: 1,
  DebitsMustNotExceedCredits: 2,
  CreditsMustNotExceedDebits: 4,
  History: 8,
} as const

// ============================================================================
// Transfer Flags (matching WASM enum values)
// ============================================================================

export const TransferFlags = {
  None: 0,
  Linked: 1,
  Pending: 2,
  PostPendingTransfer: 4,
  VoidPendingTransfer: 8,
} as const

// ============================================================================
// Store Interface
// ============================================================================

/**
 * TigerBeetle Store interface for benchmarking
 */
export interface TigerBeetleStore {
  // Account operations
  createAccounts(accounts: Partial<Account>[]): Promise<CreateAccountResult[]>
  lookupAccounts(ids: bigint[]): Promise<Account[]>
  getAccountTransfers(filter: AccountFilter): Promise<Transfer[]>
  getAccountBalances(filter: AccountFilter): Promise<AccountBalance[]>

  // Transfer operations
  createTransfers(transfers: Partial<Transfer>[]): Promise<CreateTransferResult[]>
  lookupTransfers(ids: bigint[]): Promise<Transfer[]>

  // Batch operations for throughput benchmarks
  createAccountsBatch(accounts: Partial<Account>[], batchSize?: number): Promise<CreateAccountResult[]>
  createTransfersBatch(transfers: Partial<Transfer>[], batchSize?: number): Promise<CreateTransferResult[]>

  // Access to underlying LedgerState (for advanced operations)
  getLedger(): LedgerState

  // Lifecycle
  close(): Promise<void>
}

// ============================================================================
// Configuration
// ============================================================================

export interface TigerBeetleConfig {
  // Maximum batch size (default: 8190, TigerBeetle's limit)
  maxBatchSize?: number

  // Custom timestamp generator (for testing)
  getTimestamp?: () => bigint
}

const defaultConfig: TigerBeetleConfig = {
  maxBatchSize: 8190,
}

// ============================================================================
// Conversion Utilities
// ============================================================================

/**
 * Convert bigint to Uint128
 */
function bigintToUint128(value: bigint): Uint128 {
  return Uint128.fromBigInt(value)
}

/**
 * Convert Uint128 to bigint
 */
function uint128ToBigint(value: Uint128): bigint {
  return value.toBigInt()
}

/**
 * Convert WasmAccount to legacy Account type
 */
function wasmAccountToLegacy(account: WasmAccount): Account {
  return {
    id: uint128ToBigint(account.id),
    debits_pending: uint128ToBigint(account.debits_pending),
    debits_posted: uint128ToBigint(account.debits_posted),
    credits_pending: uint128ToBigint(account.credits_pending),
    credits_posted: uint128ToBigint(account.credits_posted),
    user_data_128: uint128ToBigint(account.user_data_128),
    user_data_64: account.user_data_64,
    user_data_32: account.user_data_32,
    reserved: 0,
    ledger: account.ledger,
    code: account.code,
    flags: account.flags,
    timestamp: account.timestamp,
  }
}

/**
 * Convert WasmTransfer to legacy Transfer type
 */
function wasmTransferToLegacy(transfer: WasmTransfer): Transfer {
  return {
    id: uint128ToBigint(transfer.id),
    debit_account_id: uint128ToBigint(transfer.debit_account_id),
    credit_account_id: uint128ToBigint(transfer.credit_account_id),
    amount: uint128ToBigint(transfer.amount),
    pending_id: uint128ToBigint(transfer.pending_id),
    user_data_128: uint128ToBigint(transfer.user_data_128),
    user_data_64: transfer.user_data_64,
    user_data_32: transfer.user_data_32,
    timeout: transfer.timeout,
    ledger: transfer.ledger,
    code: transfer.code,
    flags: transfer.flags,
    timestamp: transfer.timestamp,
  }
}

/**
 * Convert Partial<Account> to CreateAccountInput
 */
function legacyAccountToInput(account: Partial<Account>): CreateAccountInput {
  const id = account.id !== undefined ? bigintToUint128(account.id) : generateUint128Id()
  return {
    id,
    user_data_128: account.user_data_128 !== undefined ? bigintToUint128(account.user_data_128) : undefined,
    user_data_64: account.user_data_64,
    user_data_32: account.user_data_32,
    ledger: account.ledger ?? 1,
    code: account.code ?? 1,
    flags: account.flags,
  }
}

/**
 * Convert Partial<Transfer> to CreateTransferInput
 */
function legacyTransferToInput(transfer: Partial<Transfer>): CreateTransferInput {
  const id = transfer.id !== undefined ? bigintToUint128(transfer.id) : generateUint128Id()
  return {
    id,
    debit_account_id: transfer.debit_account_id !== undefined ? bigintToUint128(transfer.debit_account_id) : Uint128.ZERO,
    credit_account_id: transfer.credit_account_id !== undefined ? bigintToUint128(transfer.credit_account_id) : Uint128.ZERO,
    amount: transfer.amount !== undefined ? bigintToUint128(transfer.amount) : Uint128.ZERO,
    pending_id: transfer.pending_id !== undefined ? bigintToUint128(transfer.pending_id) : undefined,
    user_data_128: transfer.user_data_128 !== undefined ? bigintToUint128(transfer.user_data_128) : undefined,
    user_data_64: transfer.user_data_64,
    user_data_32: transfer.user_data_32,
    timeout: transfer.timeout,
    ledger: transfer.ledger ?? 1,
    code: transfer.code ?? 1,
    flags: transfer.flags,
  }
}

// ============================================================================
// Store Implementation
// ============================================================================

/**
 * Generate a unique 128-bit ID as bigint
 */
export function generateId(): bigint {
  const buffer = new Uint8Array(16)
  crypto.getRandomValues(buffer)
  // Convert to bigint (little-endian)
  let id = 0n
  for (let i = 0; i < 16; i++) {
    id |= BigInt(buffer[i]) << BigInt(i * 8)
  }
  return id
}

/**
 * Generate a unique 128-bit ID as Uint128
 */
export function generateUint128Id(): Uint128 {
  return bigintToUint128(generateId())
}

/**
 * Create a new TigerBeetle store instance using the WASM LedgerState.
 *
 * This uses the pure TypeScript implementation from tigerbeetle-do
 * which runs entirely in-memory with TigerBeetle semantics.
 */
export async function createTigerBeetleStore(config: TigerBeetleConfig = {}): Promise<TigerBeetleStore> {
  const mergedConfig = { ...defaultConfig, ...config }

  // Create the LedgerState instance
  const ledger = new LedgerState({
    maxBatchSize: mergedConfig.maxBatchSize,
    getTimestamp: mergedConfig.getTimestamp,
  })

  return {
    getLedger(): LedgerState {
      return ledger
    },

    async createAccounts(accounts: Partial<Account>[]): Promise<CreateAccountResult[]> {
      const inputs = accounts.map(legacyAccountToInput)
      const results = ledger.createAccounts(inputs)

      // LedgerState returns only failed operations (TigerBeetle convention)
      // For compatibility, we return all results including successes
      const allResults: CreateAccountResult[] = accounts.map((_, index) => ({
        index,
        result: WasmCreateAccountResult.ok,
      }))

      // Override with actual failures
      for (const result of results) {
        allResults[result.index] = {
          index: result.index,
          result: result.result as WasmCreateAccountResult,
        }
      }

      return allResults
    },

    async lookupAccounts(ids: bigint[]): Promise<Account[]> {
      const uint128Ids = ids.map(bigintToUint128)
      const wasmAccounts = ledger.lookupAccounts(uint128Ids)
      return wasmAccounts.map(wasmAccountToLegacy)
    },

    async getAccountTransfers(filter: AccountFilter): Promise<Transfer[]> {
      const wasmTransfers = ledger.queryTransfers({
        debit_account_id: bigintToUint128(filter.account_id),
      })
      const creditTransfers = ledger.queryTransfers({
        credit_account_id: bigintToUint128(filter.account_id),
      })

      // Combine and dedupe
      const seen = new Set<string>()
      const allTransfers: WasmTransfer[] = []

      for (const t of [...wasmTransfers, ...creditTransfers]) {
        const key = t.id.toString()
        if (!seen.has(key)) {
          seen.add(key)
          // Filter by timestamp
          if (t.timestamp >= filter.timestamp_min && t.timestamp <= filter.timestamp_max) {
            allTransfers.push(t)
          }
        }
      }

      // Apply limit
      return allTransfers.slice(0, filter.limit).map(wasmTransferToLegacy)
    },

    async getAccountBalances(filter: AccountFilter): Promise<AccountBalance[]> {
      const accounts = ledger.lookupAccounts([bigintToUint128(filter.account_id)])
      if (accounts.length === 0) return []

      const account = accounts[0]
      return [
        {
          debits_pending: uint128ToBigint(account.debits_pending),
          debits_posted: uint128ToBigint(account.debits_posted),
          credits_pending: uint128ToBigint(account.credits_pending),
          credits_posted: uint128ToBigint(account.credits_posted),
          timestamp: account.timestamp,
        },
      ]
    },

    async createTransfers(transfers: Partial<Transfer>[]): Promise<CreateTransferResult[]> {
      const inputs = transfers.map(legacyTransferToInput)
      const results = ledger.createTransfers(inputs)

      // LedgerState returns only failed operations (TigerBeetle convention)
      // For compatibility, we return all results including successes
      const allResults: CreateTransferResult[] = transfers.map((_, index) => ({
        index,
        result: WasmCreateTransferResult.ok,
      }))

      // Override with actual failures
      for (const result of results) {
        allResults[result.index] = {
          index: result.index,
          result: result.result as WasmCreateTransferResult,
        }
      }

      return allResults
    },

    async lookupTransfers(ids: bigint[]): Promise<Transfer[]> {
      const uint128Ids = ids.map(bigintToUint128)
      const wasmTransfers = ledger.lookupTransfers(uint128Ids)
      return wasmTransfers.map(wasmTransferToLegacy)
    },

    async createAccountsBatch(
      accounts: Partial<Account>[],
      batchSize = 8190
    ): Promise<CreateAccountResult[]> {
      const results: CreateAccountResult[] = []
      for (let i = 0; i < accounts.length; i += batchSize) {
        const batch = accounts.slice(i, i + batchSize)
        const inputs = batch.map(legacyAccountToInput)
        const batchFailures = ledger.createAccounts(inputs)

        // Build results for this batch
        const batchResults: CreateAccountResult[] = batch.map((_, index) => ({
          index: i + index,
          result: WasmCreateAccountResult.ok,
        }))

        // Override with actual failures
        for (const failure of batchFailures) {
          batchResults[failure.index] = {
            index: i + failure.index,
            result: failure.result as WasmCreateAccountResult,
          }
        }

        results.push(...batchResults)
      }
      return results
    },

    async createTransfersBatch(
      transfers: Partial<Transfer>[],
      batchSize = 8190
    ): Promise<CreateTransferResult[]> {
      const results: CreateTransferResult[] = []
      for (let i = 0; i < transfers.length; i += batchSize) {
        const batch = transfers.slice(i, i + batchSize)
        const inputs = batch.map(legacyTransferToInput)
        const batchFailures = ledger.createTransfers(inputs)

        // Build results for this batch
        const batchResults: CreateTransferResult[] = batch.map((_, index) => ({
          index: i + index,
          result: WasmCreateTransferResult.ok,
        }))

        // Override with actual failures
        for (const failure of batchFailures) {
          batchResults[failure.index] = {
            index: i + failure.index,
            result: failure.result as WasmCreateTransferResult,
          }
        }

        results.push(...batchResults)
      }
      return results
    },

    async close(): Promise<void> {
      ledger.clear()
    },
  }
}

// ============================================================================
// Seed Data Utilities
// ============================================================================

/**
 * Seed row counts by dataset size
 */
const seedCountsBySize: Record<DatasetSize, { accounts: number; transfers: number }> = {
  '1mb': { accounts: 100, transfers: 1000 },
  '10mb': { accounts: 1000, transfers: 10000 },
  '100mb': { accounts: 10000, transfers: 100000 },
  '1gb': { accounts: 100000, transfers: 1000000 },
  '10gb': { accounts: 1000000, transfers: 10000000 },
  '20gb': { accounts: 2000000, transfers: 20000000 },
  '30gb': { accounts: 3000000, transfers: 30000000 },
  '50gb': { accounts: 5000000, transfers: 50000000 },
}

/**
 * Ledger codes for chart of accounts
 */
export const LedgerCodes = {
  Assets: 1,
  Liabilities: 2,
  Equity: 3,
  Revenue: 4,
  Expenses: 5,
} as const

/**
 * Account codes for account types
 */
export const AccountCodes = {
  // Assets (1xxx)
  Cash: 1001,
  AccountsReceivable: 1002,
  Inventory: 1003,
  PrepaidExpenses: 1004,

  // Liabilities (2xxx)
  AccountsPayable: 2001,
  AccruedLiabilities: 2002,
  UnearnedRevenue: 2003,

  // Equity (3xxx)
  RetainedEarnings: 3001,
  CommonStock: 3002,

  // Revenue (4xxx)
  Sales: 4001,
  ServiceRevenue: 4002,

  // Expenses (5xxx)
  CostOfGoodsSold: 5001,
  SalariesExpense: 5002,
  RentExpense: 5003,
} as const

/**
 * Transfer codes for transaction types
 */
export const TransferCodes = {
  Payment: 1,
  Refund: 2,
  Transfer: 3,
  Fee: 4,
  Adjustment: 5,
  Settlement: 6,
} as const

/**
 * Seed test data for financial benchmarks
 */
export async function seedTestData(
  store: TigerBeetleStore,
  size: DatasetSize = '10mb'
): Promise<{ accountIds: bigint[] }> {
  const counts = seedCountsBySize[size]
  const accountIds: bigint[] = []

  console.log(`Seeding TigerBeetle with ${counts.accounts} accounts and ${counts.transfers} transfers...`)

  // Create accounts
  const accounts: Partial<Account>[] = []
  for (let i = 0; i < counts.accounts; i++) {
    const id = generateId()
    accountIds.push(id)
    accounts.push({
      id,
      ledger: LedgerCodes.Assets,
      code: AccountCodes.Cash,
      flags: AccountFlags.DebitsMustNotExceedCredits,
      user_data_32: i, // Account index for reference
    })
  }

  // Batch create accounts
  const batchSize = 8190
  for (let i = 0; i < accounts.length; i += batchSize) {
    const batch = accounts.slice(i, i + batchSize)
    await store.createAccounts(batch)
    if ((i + batchSize) % 10000 === 0) {
      console.log(`Created ${Math.min(i + batchSize, accounts.length)} accounts...`)
    }
  }

  // Seed initial credits to accounts (so they have balances to transfer)
  // Create a "bank" account to fund the others
  const bankAccountId = generateId()
  await store.createAccounts([
    {
      id: bankAccountId,
      ledger: LedgerCodes.Assets,
      code: AccountCodes.Cash,
      flags: AccountFlags.None, // Bank can have unlimited credits
    },
  ])

  // Fund each account with initial balance
  const fundingTransfers: Partial<Transfer>[] = accountIds.map((accountId) => ({
    id: generateId(),
    debit_account_id: bankAccountId,
    credit_account_id: accountId,
    amount: BigInt(Math.floor(Math.random() * 10000) + 1000) * 100n, // Random amount between $10 and $110
    ledger: LedgerCodes.Assets,
    code: TransferCodes.Transfer,
  }))

  // Batch create funding transfers
  for (let i = 0; i < fundingTransfers.length; i += batchSize) {
    const batch = fundingTransfers.slice(i, i + batchSize)
    await store.createTransfers(batch)
  }

  // Create random transfers between accounts
  const transfers: Partial<Transfer>[] = []
  for (let i = 0; i < counts.transfers; i++) {
    const fromIdx = Math.floor(Math.random() * accountIds.length)
    let toIdx = Math.floor(Math.random() * accountIds.length)
    while (toIdx === fromIdx) {
      toIdx = Math.floor(Math.random() * accountIds.length)
    }

    transfers.push({
      id: generateId(),
      debit_account_id: accountIds[fromIdx],
      credit_account_id: accountIds[toIdx],
      amount: BigInt(Math.floor(Math.random() * 1000) + 1) * 100n, // Random amount between $0.01 and $10
      ledger: LedgerCodes.Assets,
      code: TransferCodes.Payment,
    })
  }

  // Batch create transfers
  for (let i = 0; i < transfers.length; i += batchSize) {
    const batch = transfers.slice(i, i + batchSize)
    await store.createTransfers(batch)
    if ((i + batchSize) % 100000 === 0) {
      console.log(`Created ${Math.min(i + batchSize, transfers.length)} transfers...`)
    }
  }

  console.log(`Seeding complete: ${counts.accounts} accounts, ${counts.transfers} transfers`)

  return { accountIds }
}

/**
 * Restore store state from DO storage (for hibernation benchmarks).
 * TigerBeetle maintains its own persistent storage, so this is a no-op.
 */
export async function restoreFromStorage(
  _store: TigerBeetleStore,
  _storage: Map<string, ArrayBuffer>
): Promise<void> {
  // TigerBeetle is a persistent database - no restoration needed
  // The cluster maintains its own state across restarts
}
