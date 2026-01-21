/**
 * Ledger Dataset - General Accounting
 *
 * Represents a complete chart of accounts and journal entry system
 * for general-purpose double-entry bookkeeping.
 *
 * Suitable for:
 * - Small to medium business accounting
 * - General ledger operations
 * - Financial reporting (trial balance, income statement, balance sheet)
 * - Audit trails
 */

import {
  DatasetSize,
  LedgerCodes,
  AccountCodes,
  TransferCodes,
  FinancialAccount,
  FinancialTransfer,
  seedConfigs,
} from './index'

// ============================================================================
// Ledger-Specific Types
// ============================================================================

/**
 * Journal entry - a collection of related transfers that must balance
 */
export interface JournalEntry {
  id: bigint
  entry_number: string
  date: Date
  description: string
  reference?: string
  // Line items (transfers)
  lines: JournalLine[]
  // Metadata
  posted: boolean
  posted_at?: Date
  posted_by?: string
  created_at: Date
  created_by?: string
}

/**
 * Journal entry line item
 */
export interface JournalLine {
  account_id: bigint
  debit?: bigint
  credit?: bigint
  description?: string
}

/**
 * Account with ledger-specific metadata
 */
export interface LedgerAccount extends FinancialAccount {
  account_number: string
  is_control_account: boolean
  normal_balance: 'debit' | 'credit'
  is_active: boolean
}

/**
 * Period for financial reporting
 */
export interface AccountingPeriod {
  id: string
  name: string
  start_date: Date
  end_date: Date
  is_closed: boolean
  closed_at?: Date
}

// ============================================================================
// Chart of Accounts Template
// ============================================================================

/**
 * Standard chart of accounts structure
 */
export const chartOfAccounts: Omit<LedgerAccount, 'id' | 'debits_pending' | 'debits_posted' | 'credits_pending' | 'credits_posted' | 'created_at'>[] = [
  // Assets
  { name: 'Cash', account_number: '1001', type: 'asset', ledger: LedgerCodes.Assets, code: AccountCodes.Cash, currency: 'USD', is_control_account: false, normal_balance: 'debit', is_active: true },
  { name: 'Accounts Receivable', account_number: '1002', type: 'asset', ledger: LedgerCodes.Assets, code: AccountCodes.AccountsReceivable, currency: 'USD', is_control_account: true, normal_balance: 'debit', is_active: true },
  { name: 'Inventory', account_number: '1003', type: 'asset', ledger: LedgerCodes.Assets, code: AccountCodes.Inventory, currency: 'USD', is_control_account: false, normal_balance: 'debit', is_active: true },
  { name: 'Prepaid Expenses', account_number: '1004', type: 'asset', ledger: LedgerCodes.Assets, code: AccountCodes.PrepaidExpenses, currency: 'USD', is_control_account: false, normal_balance: 'debit', is_active: true },
  { name: 'Fixed Assets', account_number: '1005', type: 'asset', ledger: LedgerCodes.Assets, code: AccountCodes.FixedAssets, currency: 'USD', is_control_account: false, normal_balance: 'debit', is_active: true },

  // Liabilities
  { name: 'Accounts Payable', account_number: '2001', type: 'liability', ledger: LedgerCodes.Liabilities, code: AccountCodes.AccountsPayable, currency: 'USD', is_control_account: true, normal_balance: 'credit', is_active: true },
  { name: 'Accrued Liabilities', account_number: '2002', type: 'liability', ledger: LedgerCodes.Liabilities, code: AccountCodes.AccruedLiabilities, currency: 'USD', is_control_account: false, normal_balance: 'credit', is_active: true },
  { name: 'Unearned Revenue', account_number: '2003', type: 'liability', ledger: LedgerCodes.Liabilities, code: AccountCodes.UnearnedRevenue, currency: 'USD', is_control_account: false, normal_balance: 'credit', is_active: true },
  { name: 'Short-Term Debt', account_number: '2004', type: 'liability', ledger: LedgerCodes.Liabilities, code: AccountCodes.ShortTermDebt, currency: 'USD', is_control_account: false, normal_balance: 'credit', is_active: true },
  { name: 'Long-Term Debt', account_number: '2005', type: 'liability', ledger: LedgerCodes.Liabilities, code: AccountCodes.LongTermDebt, currency: 'USD', is_control_account: false, normal_balance: 'credit', is_active: true },

  // Equity
  { name: 'Retained Earnings', account_number: '3001', type: 'equity', ledger: LedgerCodes.Equity, code: AccountCodes.RetainedEarnings, currency: 'USD', is_control_account: false, normal_balance: 'credit', is_active: true },
  { name: 'Common Stock', account_number: '3002', type: 'equity', ledger: LedgerCodes.Equity, code: AccountCodes.CommonStock, currency: 'USD', is_control_account: false, normal_balance: 'credit', is_active: true },

  // Revenue
  { name: 'Sales Revenue', account_number: '4001', type: 'revenue', ledger: LedgerCodes.Revenue, code: AccountCodes.Sales, currency: 'USD', is_control_account: false, normal_balance: 'credit', is_active: true },
  { name: 'Service Revenue', account_number: '4002', type: 'revenue', ledger: LedgerCodes.Revenue, code: AccountCodes.ServiceRevenue, currency: 'USD', is_control_account: false, normal_balance: 'credit', is_active: true },
  { name: 'Interest Income', account_number: '4003', type: 'revenue', ledger: LedgerCodes.Revenue, code: AccountCodes.InterestIncome, currency: 'USD', is_control_account: false, normal_balance: 'credit', is_active: true },

  // Expenses
  { name: 'Cost of Goods Sold', account_number: '5001', type: 'expense', ledger: LedgerCodes.Expenses, code: AccountCodes.CostOfGoodsSold, currency: 'USD', is_control_account: false, normal_balance: 'debit', is_active: true },
  { name: 'Salaries Expense', account_number: '5002', type: 'expense', ledger: LedgerCodes.Expenses, code: AccountCodes.SalariesExpense, currency: 'USD', is_control_account: false, normal_balance: 'debit', is_active: true },
  { name: 'Rent Expense', account_number: '5003', type: 'expense', ledger: LedgerCodes.Expenses, code: AccountCodes.RentExpense, currency: 'USD', is_control_account: false, normal_balance: 'debit', is_active: true },
  { name: 'Utilities Expense', account_number: '5004', type: 'expense', ledger: LedgerCodes.Expenses, code: AccountCodes.UtilitiesExpense, currency: 'USD', is_control_account: false, normal_balance: 'debit', is_active: true },
  { name: 'Marketing Expense', account_number: '5005', type: 'expense', ledger: LedgerCodes.Expenses, code: AccountCodes.MarketingExpense, currency: 'USD', is_control_account: false, normal_balance: 'debit', is_active: true },
]

// ============================================================================
// Data Generation
// ============================================================================

let idCounter = 1n

/**
 * Generate a unique 128-bit ID
 */
function generateId(): bigint {
  return idCounter++
}

/**
 * Generate random amount within range
 */
function randomAmount(min: bigint, max: bigint): bigint {
  const range = max - min
  return min + BigInt(Math.floor(Math.random() * Number(range)))
}

/**
 * Generate random date within range
 */
function randomDate(start: Date, end: Date): Date {
  const startTime = start.getTime()
  const endTime = end.getTime()
  return new Date(startTime + Math.random() * (endTime - startTime))
}

/**
 * Generate a balanced journal entry
 */
export function generateJournalEntry(
  accounts: LedgerAccount[],
  amountRange: { min: bigint; max: bigint }
): JournalEntry {
  const entryId = generateId()
  const amount = randomAmount(amountRange.min, amountRange.max)

  // Pick random debit and credit accounts
  const debitAccounts = accounts.filter(a => a.normal_balance === 'debit')
  const creditAccounts = accounts.filter(a => a.normal_balance === 'credit')

  const debitAccount = debitAccounts[Math.floor(Math.random() * debitAccounts.length)]
  const creditAccount = creditAccounts[Math.floor(Math.random() * creditAccounts.length)]

  return {
    id: entryId,
    entry_number: `JE-${String(entryId).padStart(8, '0')}`,
    date: randomDate(new Date('2023-01-01'), new Date('2024-12-31')),
    description: `Journal entry ${entryId}`,
    lines: [
      { account_id: debitAccount.id, debit: amount },
      { account_id: creditAccount.id, credit: amount },
    ],
    posted: true,
    posted_at: new Date(),
    created_at: new Date(),
  }
}

/**
 * Generate seed data for ledger benchmarks
 */
export async function seedLedgerData(
  size: DatasetSize,
  createAccount: (account: Partial<LedgerAccount>) => Promise<{ id: bigint }>,
  createTransfer: (transfer: Partial<FinancialTransfer>) => Promise<void>
): Promise<{ accounts: LedgerAccount[]; journalEntries: JournalEntry[] }> {
  const config = seedConfigs[size]
  const accounts: LedgerAccount[] = []
  const journalEntries: JournalEntry[] = []

  // Create chart of accounts
  for (const template of chartOfAccounts) {
    const id = generateId()
    const account: LedgerAccount = {
      ...template,
      id,
      debits_pending: 0n,
      debits_posted: 0n,
      credits_pending: 0n,
      credits_posted: 0n,
      created_at: new Date(),
    }
    await createAccount(account)
    accounts.push(account)
  }

  // Create additional subsidiary accounts based on size
  const additionalAccounts = config.accounts - chartOfAccounts.length
  for (let i = 0; i < additionalAccounts; i++) {
    const baseAccount = chartOfAccounts[i % chartOfAccounts.length]
    const id = generateId()
    const account: LedgerAccount = {
      ...baseAccount,
      id,
      name: `${baseAccount.name} - Sub ${i + 1}`,
      account_number: `${baseAccount.account_number}-${String(i + 1).padStart(4, '0')}`,
      is_control_account: false,
      debits_pending: 0n,
      debits_posted: 0n,
      credits_pending: 0n,
      credits_posted: 0n,
      created_at: new Date(),
    }
    await createAccount(account)
    accounts.push(account)
  }

  // Generate journal entries as transfers
  for (let i = 0; i < config.transfers; i++) {
    const entry = generateJournalEntry(accounts, config.amountRange)
    journalEntries.push(entry)

    // Create transfer for each line
    for (const line of entry.lines) {
      const fromAccount = line.debit ? accounts.find(a => a.id === line.account_id) : undefined
      const toAccount = line.credit ? accounts.find(a => a.id === line.account_id) : undefined

      if (line.debit && fromAccount) {
        // For a debit entry, we need a corresponding credit somewhere
        const creditLine = entry.lines.find(l => l.credit)
        if (creditLine) {
          await createTransfer({
            id: generateId(),
            debit_account_id: line.account_id,
            credit_account_id: creditLine.account_id,
            amount: line.debit,
            ledger: fromAccount.ledger,
            code: TransferCodes.JournalEntry,
            currency: 'USD',
            reference: entry.entry_number,
            description: entry.description,
            created_at: entry.date,
          })
        }
      }
    }
  }

  return { accounts, journalEntries }
}

// ============================================================================
// Benchmark Queries
// ============================================================================

/**
 * Ledger-specific benchmark queries
 */
export const ledgerQueries = {
  /**
   * Trial Balance - verify debits = credits
   */
  trialBalance: `
    SELECT
      account_number,
      name,
      SUM(debits_posted) as total_debits,
      SUM(credits_posted) as total_credits
    FROM accounts
    WHERE is_active = true
    GROUP BY account_number, name
    ORDER BY account_number
  `,

  /**
   * Balance Sheet
   */
  balanceSheet: `
    SELECT
      CASE ledger
        WHEN 1 THEN 'Assets'
        WHEN 2 THEN 'Liabilities'
        WHEN 3 THEN 'Equity'
      END as category,
      name,
      credits_posted - debits_posted as balance
    FROM accounts
    WHERE ledger IN (1, 2, 3) AND is_active = true
    ORDER BY ledger, code
  `,

  /**
   * Income Statement
   */
  incomeStatement: `
    SELECT
      CASE ledger
        WHEN 4 THEN 'Revenue'
        WHEN 5 THEN 'Expense'
      END as category,
      name,
      CASE ledger
        WHEN 4 THEN credits_posted - debits_posted
        WHEN 5 THEN debits_posted - credits_posted
      END as amount
    FROM accounts
    WHERE ledger IN (4, 5) AND is_active = true
    ORDER BY ledger, code
  `,

  /**
   * Account Activity for a period
   */
  accountActivity: `
    SELECT
      t.created_at,
      t.reference,
      t.description,
      CASE WHEN t.debit_account_id = $1 THEN t.amount ELSE 0 END as debit,
      CASE WHEN t.credit_account_id = $1 THEN t.amount ELSE 0 END as credit
    FROM transfers t
    WHERE (t.debit_account_id = $1 OR t.credit_account_id = $1)
      AND t.created_at BETWEEN $2 AND $3
    ORDER BY t.created_at
  `,

  /**
   * Journal entry list
   */
  journalEntries: `
    SELECT
      reference as entry_number,
      created_at as date,
      description,
      SUM(amount) as total
    FROM transfers
    WHERE code = ${TransferCodes.JournalEntry}
      AND created_at BETWEEN $1 AND $2
    GROUP BY reference, created_at, description
    ORDER BY created_at DESC
    LIMIT $3
  `,
}
