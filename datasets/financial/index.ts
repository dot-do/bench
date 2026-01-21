/**
 * Financial Dataset Registry and Types
 *
 * This module provides type definitions and a registry for financial benchmark datasets.
 * Financial datasets are specialized for double-entry bookkeeping, accounting, and payments.
 *
 * TigerBeetle-specific considerations:
 * - All amounts are stored as 128-bit integers (BigInt in JS)
 * - Accounts use ledger/code classification (chart of accounts)
 * - Transfers are immutable and append-only
 * - Built-in support for pending/posted amounts
 */

// ============================================================================
// Dataset Size Configuration
// ============================================================================

export type DatasetSize = '1mb' | '10mb' | '100mb' | '1gb' | '10gb' | '20gb' | '30gb' | '50gb'

// ============================================================================
// Financial Entity Types
// ============================================================================

/**
 * Account types in a chart of accounts
 */
export type AccountType = 'asset' | 'liability' | 'equity' | 'revenue' | 'expense'

/**
 * Ledger codes (industry-standard chart of accounts ranges)
 */
export const LedgerCodes = {
  Assets: 1, // 1000-1999
  Liabilities: 2, // 2000-2999
  Equity: 3, // 3000-3999
  Revenue: 4, // 4000-4999
  Expenses: 5, // 5000-5999
} as const

/**
 * Standard account codes within each ledger
 */
export const AccountCodes = {
  // Assets (1xxx)
  Cash: 1001,
  AccountsReceivable: 1002,
  Inventory: 1003,
  PrepaidExpenses: 1004,
  FixedAssets: 1005,
  Investments: 1006,

  // Liabilities (2xxx)
  AccountsPayable: 2001,
  AccruedLiabilities: 2002,
  UnearnedRevenue: 2003,
  ShortTermDebt: 2004,
  LongTermDebt: 2005,

  // Equity (3xxx)
  RetainedEarnings: 3001,
  CommonStock: 3002,
  AdditionalPaidInCapital: 3003,

  // Revenue (4xxx)
  Sales: 4001,
  ServiceRevenue: 4002,
  InterestIncome: 4003,

  // Expenses (5xxx)
  CostOfGoodsSold: 5001,
  SalariesExpense: 5002,
  RentExpense: 5003,
  UtilitiesExpense: 5004,
  MarketingExpense: 5005,
  DepreciationExpense: 5006,
} as const

/**
 * Transfer/transaction codes
 */
export const TransferCodes = {
  // Payment types
  Payment: 1,
  Refund: 2,
  Transfer: 3,
  Fee: 4,
  Adjustment: 5,

  // Settlement types
  Settlement: 10,
  Chargeback: 11,
  Reversal: 12,

  // Accounting entries
  JournalEntry: 20,
  Accrual: 21,
  Deferral: 22,

  // Payroll
  SalaryPayment: 30,
  TaxWithholding: 31,
  Benefits: 32,
} as const

// ============================================================================
// Financial Account Definition
// ============================================================================

export interface FinancialAccount {
  id: bigint
  name: string
  type: AccountType
  ledger: number
  code: number
  currency: string
  // Balance components
  debits_pending: bigint
  debits_posted: bigint
  credits_pending: bigint
  credits_posted: bigint
  // Metadata
  parent_id?: bigint
  external_id?: string
  user_data?: Record<string, unknown>
  created_at: Date
}

/**
 * Calculate available balance (credits - debits)
 */
export function getAvailableBalance(account: FinancialAccount): bigint {
  return account.credits_posted - account.debits_posted
}

/**
 * Calculate pending balance (including pending)
 */
export function getPendingBalance(account: FinancialAccount): bigint {
  return (
    account.credits_posted +
    account.credits_pending -
    account.debits_posted -
    account.debits_pending
  )
}

// ============================================================================
// Financial Transfer Definition
// ============================================================================

export interface FinancialTransfer {
  id: bigint
  debit_account_id: bigint
  credit_account_id: bigint
  amount: bigint
  currency: string
  ledger: number
  code: number
  // Two-phase commit support
  pending_id?: bigint
  timeout?: number
  // Metadata
  reference?: string
  description?: string
  user_data?: Record<string, unknown>
  created_at: Date
}

// ============================================================================
// Seed Configuration
// ============================================================================

export interface FinancialSeedConfig {
  size: DatasetSize
  accounts: number
  transfers: number
  // Distribution parameters
  accountDistribution: {
    assets: number
    liabilities: number
    equity: number
    revenue: number
    expenses: number
  }
  transferDistribution: {
    payments: number
    refunds: number
    fees: number
    settlements: number
  }
  // Amount ranges (in cents/smallest currency unit)
  amountRange: {
    min: bigint
    max: bigint
  }
}

/**
 * Seed configurations by size tier
 */
export const seedConfigs: Record<DatasetSize, FinancialSeedConfig> = {
  '1mb': {
    size: '1mb',
    accounts: 100,
    transfers: 1000,
    accountDistribution: { assets: 40, liabilities: 20, equity: 10, revenue: 15, expenses: 15 },
    transferDistribution: { payments: 70, refunds: 10, fees: 10, settlements: 10 },
    amountRange: { min: 100n, max: 100000n }, // $1 - $1,000
  },
  '10mb': {
    size: '10mb',
    accounts: 1000,
    transfers: 10000,
    accountDistribution: { assets: 40, liabilities: 20, equity: 10, revenue: 15, expenses: 15 },
    transferDistribution: { payments: 70, refunds: 10, fees: 10, settlements: 10 },
    amountRange: { min: 100n, max: 1000000n }, // $1 - $10,000
  },
  '100mb': {
    size: '100mb',
    accounts: 10000,
    transfers: 100000,
    accountDistribution: { assets: 40, liabilities: 20, equity: 10, revenue: 15, expenses: 15 },
    transferDistribution: { payments: 70, refunds: 10, fees: 10, settlements: 10 },
    amountRange: { min: 100n, max: 10000000n }, // $1 - $100,000
  },
  '1gb': {
    size: '1gb',
    accounts: 100000,
    transfers: 1000000,
    accountDistribution: { assets: 40, liabilities: 20, equity: 10, revenue: 15, expenses: 15 },
    transferDistribution: { payments: 70, refunds: 10, fees: 10, settlements: 10 },
    amountRange: { min: 100n, max: 100000000n }, // $1 - $1,000,000
  },
  '10gb': {
    size: '10gb',
    accounts: 1000000,
    transfers: 10000000,
    accountDistribution: { assets: 40, liabilities: 20, equity: 10, revenue: 15, expenses: 15 },
    transferDistribution: { payments: 70, refunds: 10, fees: 10, settlements: 10 },
    amountRange: { min: 100n, max: 100000000n },
  },
  '20gb': {
    size: '20gb',
    accounts: 2000000,
    transfers: 20000000,
    accountDistribution: { assets: 40, liabilities: 20, equity: 10, revenue: 15, expenses: 15 },
    transferDistribution: { payments: 70, refunds: 10, fees: 10, settlements: 10 },
    amountRange: { min: 100n, max: 100000000n },
  },
  '30gb': {
    size: '30gb',
    accounts: 3000000,
    transfers: 30000000,
    accountDistribution: { assets: 40, liabilities: 20, equity: 10, revenue: 15, expenses: 15 },
    transferDistribution: { payments: 70, refunds: 10, fees: 10, settlements: 10 },
    amountRange: { min: 100n, max: 100000000n },
  },
  '50gb': {
    size: '50gb',
    accounts: 5000000,
    transfers: 50000000,
    accountDistribution: { assets: 40, liabilities: 20, equity: 10, revenue: 15, expenses: 15 },
    transferDistribution: { payments: 70, refunds: 10, fees: 10, settlements: 10 },
    amountRange: { min: 100n, max: 100000000n },
  },
}

// ============================================================================
// Benchmark Query Definitions
// ============================================================================

export interface FinancialBenchmarkQuery {
  name: string
  description: string
  category: 'lookup' | 'balance' | 'transfer' | 'history' | 'aggregate' | 'report'
  // For SQL databases
  sql?: string
  // For TigerBeetle (function-based)
  tigerbeetle?: (store: unknown, params: unknown) => Promise<unknown>
  // Parameters
  parameters?: Record<string, unknown>
}

/**
 * Standard financial benchmark queries
 */
export const benchmarkQueries: FinancialBenchmarkQuery[] = [
  // Lookups
  {
    name: 'account_lookup',
    description: 'Look up a single account by ID',
    category: 'lookup',
    sql: 'SELECT * FROM accounts WHERE id = $1',
  },
  {
    name: 'batch_account_lookup',
    description: 'Look up multiple accounts by ID',
    category: 'lookup',
    sql: 'SELECT * FROM accounts WHERE id = ANY($1)',
  },

  // Balances
  {
    name: 'balance_lookup',
    description: 'Get current balance for an account',
    category: 'balance',
    sql: 'SELECT credits_posted - debits_posted as balance FROM accounts WHERE id = $1',
  },
  {
    name: 'pending_balance_lookup',
    description: 'Get balance including pending amounts',
    category: 'balance',
    sql: `SELECT
            credits_posted + credits_pending - debits_posted - debits_pending as pending_balance
          FROM accounts WHERE id = $1`,
  },

  // Transfers
  {
    name: 'single_transfer',
    description: 'Execute a single transfer between accounts',
    category: 'transfer',
  },
  {
    name: 'batch_transfers',
    description: 'Execute a batch of transfers',
    category: 'transfer',
  },

  // History
  {
    name: 'account_history',
    description: 'Get transfer history for an account',
    category: 'history',
    sql: `SELECT * FROM transfers
          WHERE debit_account_id = $1 OR credit_account_id = $1
          ORDER BY created_at DESC LIMIT $2`,
  },

  // Aggregates
  {
    name: 'total_by_ledger',
    description: 'Total balances by ledger type',
    category: 'aggregate',
    sql: `SELECT ledger, SUM(credits_posted - debits_posted) as total
          FROM accounts GROUP BY ledger`,
  },
  {
    name: 'transfer_volume',
    description: 'Total transfer volume in a time period',
    category: 'aggregate',
    sql: `SELECT COUNT(*) as count, SUM(amount) as total
          FROM transfers WHERE created_at BETWEEN $1 AND $2`,
  },

  // Reports
  {
    name: 'trial_balance',
    description: 'Generate trial balance report',
    category: 'report',
    sql: `SELECT
            ledger,
            code,
            SUM(debits_posted) as total_debits,
            SUM(credits_posted) as total_credits,
            SUM(credits_posted - debits_posted) as net_balance
          FROM accounts
          GROUP BY ledger, code
          ORDER BY ledger, code`,
  },
  {
    name: 'income_statement',
    description: 'Generate income statement',
    category: 'report',
    sql: `SELECT
            CASE ledger WHEN 4 THEN 'Revenue' WHEN 5 THEN 'Expense' END as category,
            code,
            SUM(credits_posted - debits_posted) as amount
          FROM accounts
          WHERE ledger IN (4, 5)
          GROUP BY ledger, code
          ORDER BY ledger, code`,
  },
]

// ============================================================================
// Re-exports
// ============================================================================

export * from './ledger'
export * from './banking'
export * from './ecommerce-payments'
