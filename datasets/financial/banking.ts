/**
 * Banking Dataset - Financial Institution Operations
 *
 * Represents a retail/commercial banking system with:
 * - Customer accounts (checking, savings, loans)
 * - Transfers between accounts
 * - Balance tracking with pending/posted amounts
 * - Transaction limits and controls
 *
 * Optimized for TigerBeetle's double-entry model:
 * - All transfers are two-sided (debit/credit)
 * - Pending amounts for authorization holds
 * - Account flags for overdraft protection
 */

import {
  DatasetSize,
  LedgerCodes,
  TransferCodes,
  FinancialAccount,
  FinancialTransfer,
  seedConfigs,
} from './index'

// ============================================================================
// Banking-Specific Types
// ============================================================================

/**
 * Bank account types
 */
export type BankAccountType = 'checking' | 'savings' | 'money_market' | 'cd' | 'loan' | 'credit_card'

/**
 * Transaction status
 */
export type TransactionStatus = 'pending' | 'posted' | 'declined' | 'reversed' | 'expired'

/**
 * Bank account with banking-specific metadata
 */
export interface BankAccount extends FinancialAccount {
  account_number: string
  routing_number: string
  account_type: BankAccountType
  customer_id: bigint
  // Limits
  daily_limit: bigint
  transaction_limit: bigint
  overdraft_limit: bigint
  // Interest
  interest_rate: number // APY in basis points (e.g., 425 = 4.25%)
  interest_accrued: bigint
  // Status
  is_active: boolean
  is_frozen: boolean
  opened_at: Date
  closed_at?: Date
}

/**
 * Customer profile
 */
export interface BankCustomer {
  id: bigint
  customer_number: string
  first_name: string
  last_name: string
  email: string
  phone: string
  // KYC status
  kyc_verified: boolean
  kyc_verified_at?: Date
  // Risk profile
  risk_score: number
  // Metadata
  created_at: Date
}

/**
 * Bank transfer with banking-specific metadata
 */
export interface BankTransfer extends FinancialTransfer {
  transaction_type: 'ach' | 'wire' | 'internal' | 'atm' | 'pos' | 'fee' | 'interest'
  status: TransactionStatus
  // Authorization
  authorization_code?: string
  authorized_at?: Date
  // Settlement
  settlement_date?: Date
  settled_at?: Date
  // Metadata
  merchant_name?: string
  merchant_category_code?: string
  location?: string
}

// ============================================================================
// Banking Codes
// ============================================================================

/**
 * Bank-specific account codes
 */
export const BankAccountCodes = {
  // Customer accounts
  Checking: 1101,
  Savings: 1102,
  MoneyMarket: 1103,
  CD: 1104,

  // Liability accounts (customer deposits are bank liabilities)
  CustomerDeposits: 2101,
  AccruedInterest: 2102,

  // Asset accounts (loans are bank assets)
  ConsumerLoans: 1201,
  MortgageLoans: 1202,
  CreditCardReceivables: 1203,

  // Internal accounts
  FeeIncome: 4101,
  InterestExpense: 5101,
  OperatingAccount: 1001,
} as const

/**
 * Bank-specific transfer codes
 */
export const BankTransferCodes = {
  // Deposits
  CashDeposit: 101,
  CheckDeposit: 102,
  ACHCredit: 103,
  WireCredit: 104,

  // Withdrawals
  CashWithdrawal: 201,
  CheckCashed: 202,
  ACHDebit: 203,
  WireDebit: 204,

  // Card transactions
  POSDebit: 301,
  ATMWithdrawal: 302,
  OnlinePayment: 303,

  // Fees
  MonthlyFee: 401,
  OverdraftFee: 402,
  WireFee: 403,
  ATMFee: 404,

  // Interest
  InterestCredit: 501,
  InterestDebit: 502,

  // Internal
  InternalTransfer: 601,
  LoanPayment: 602,
  LoanDisbursement: 603,
} as const

// ============================================================================
// Data Generation
// ============================================================================

let idCounter = 1n

function generateId(): bigint {
  return idCounter++
}

function randomAmount(min: bigint, max: bigint): bigint {
  const range = max - min
  return min + BigInt(Math.floor(Math.random() * Number(range)))
}

function randomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min
}

function randomDate(start: Date, end: Date): Date {
  const startTime = start.getTime()
  const endTime = end.getTime()
  return new Date(startTime + Math.random() * (endTime - startTime))
}

function randomElement<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)]
}

/**
 * Generate a random bank account number
 */
function generateAccountNumber(): string {
  return String(Math.floor(Math.random() * 9000000000) + 1000000000)
}

/**
 * Generate a random routing number (ABA format)
 */
function generateRoutingNumber(): string {
  // Simplified - real routing numbers have checksum validation
  return String(Math.floor(Math.random() * 900000000) + 100000000)
}

/**
 * Generate a random customer
 */
function generateCustomer(): BankCustomer {
  const id = generateId()
  const firstNames = ['James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda', 'David', 'Elizabeth']
  const lastNames = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez']

  return {
    id,
    customer_number: `CUST-${String(id).padStart(10, '0')}`,
    first_name: randomElement(firstNames),
    last_name: randomElement(lastNames),
    email: `customer${id}@example.com`,
    phone: `+1${String(Math.floor(Math.random() * 9000000000) + 1000000000)}`,
    kyc_verified: Math.random() > 0.1, // 90% verified
    kyc_verified_at: Math.random() > 0.1 ? randomDate(new Date('2020-01-01'), new Date()) : undefined,
    risk_score: randomInt(1, 100),
    created_at: randomDate(new Date('2015-01-01'), new Date()),
  }
}

/**
 * Generate a bank account for a customer
 */
function generateBankAccount(customer: BankCustomer, accountType: BankAccountType): BankAccount {
  const id = generateId()

  const accountTypeCodes: Record<BankAccountType, number> = {
    checking: BankAccountCodes.Checking,
    savings: BankAccountCodes.Savings,
    money_market: BankAccountCodes.MoneyMarket,
    cd: BankAccountCodes.CD,
    loan: BankAccountCodes.ConsumerLoans,
    credit_card: BankAccountCodes.CreditCardReceivables,
  }

  const interestRates: Record<BankAccountType, number> = {
    checking: 10, // 0.10%
    savings: 425, // 4.25%
    money_market: 475, // 4.75%
    cd: 500, // 5.00%
    loan: 850, // 8.50% (paid by customer)
    credit_card: 1999, // 19.99% (paid by customer)
  }

  const dailyLimits: Record<BankAccountType, bigint> = {
    checking: 10000_00n, // $10,000
    savings: 5000_00n, // $5,000
    money_market: 25000_00n, // $25,000
    cd: 0n, // No daily transfers
    loan: 0n, // N/A
    credit_card: 5000_00n, // $5,000
  }

  return {
    id,
    name: `${customer.first_name} ${customer.last_name} - ${accountType}`,
    account_number: generateAccountNumber(),
    routing_number: generateRoutingNumber(),
    account_type: accountType,
    customer_id: customer.id,
    type: accountType === 'loan' || accountType === 'credit_card' ? 'asset' : 'liability',
    ledger: accountType === 'loan' || accountType === 'credit_card' ? LedgerCodes.Assets : LedgerCodes.Liabilities,
    code: accountTypeCodes[accountType],
    currency: 'USD',
    daily_limit: dailyLimits[accountType],
    transaction_limit: dailyLimits[accountType] / 2n,
    overdraft_limit: accountType === 'checking' ? 500_00n : 0n, // $500 overdraft for checking
    interest_rate: interestRates[accountType],
    interest_accrued: 0n,
    debits_pending: 0n,
    debits_posted: 0n,
    credits_pending: 0n,
    credits_posted: randomAmount(1000_00n, 50000_00n), // Initial balance $10 - $500
    is_active: true,
    is_frozen: false,
    opened_at: randomDate(customer.created_at, new Date()),
    created_at: new Date(),
  }
}

/**
 * Generate a bank transfer between accounts
 */
function generateBankTransfer(
  fromAccount: BankAccount,
  toAccount: BankAccount,
  amountRange: { min: bigint; max: bigint }
): BankTransfer {
  const id = generateId()
  const amount = randomAmount(amountRange.min, amountRange.max)

  const transactionTypes: BankTransfer['transaction_type'][] = ['ach', 'wire', 'internal', 'pos']
  const transactionType = randomElement(transactionTypes)

  const codeMap: Record<BankTransfer['transaction_type'], number> = {
    ach: BankTransferCodes.ACHCredit,
    wire: BankTransferCodes.WireCredit,
    internal: BankTransferCodes.InternalTransfer,
    atm: BankTransferCodes.ATMWithdrawal,
    pos: BankTransferCodes.POSDebit,
    fee: BankTransferCodes.MonthlyFee,
    interest: BankTransferCodes.InterestCredit,
  }

  const createdAt = randomDate(new Date('2024-01-01'), new Date())

  return {
    id,
    debit_account_id: fromAccount.id,
    credit_account_id: toAccount.id,
    amount,
    currency: 'USD',
    ledger: fromAccount.ledger,
    code: codeMap[transactionType],
    transaction_type: transactionType,
    status: 'posted',
    authorization_code: Math.random().toString(36).slice(2, 10).toUpperCase(),
    authorized_at: createdAt,
    settlement_date: new Date(createdAt.getTime() + 24 * 60 * 60 * 1000), // T+1
    settled_at: new Date(createdAt.getTime() + 24 * 60 * 60 * 1000),
    created_at: createdAt,
  }
}

// ============================================================================
// Seed Functions
// ============================================================================

/**
 * Generate seed data for banking benchmarks
 */
export async function seedBankingData(
  size: DatasetSize,
  createAccount: (account: Partial<BankAccount>) => Promise<{ id: bigint }>,
  createTransfer: (transfer: Partial<BankTransfer>) => Promise<void>
): Promise<{ customers: BankCustomer[]; accounts: BankAccount[]; transfers: BankTransfer[] }> {
  const config = seedConfigs[size]
  const customers: BankCustomer[] = []
  const accounts: BankAccount[] = []
  const transfers: BankTransfer[] = []

  // Calculate customer count (assume 2 accounts per customer on average)
  const customerCount = Math.ceil(config.accounts / 2)

  // Generate customers
  for (let i = 0; i < customerCount; i++) {
    customers.push(generateCustomer())
  }

  // Create operating account for the bank
  const operatingAccountId = generateId()
  const operatingAccount: BankAccount = {
    id: operatingAccountId,
    name: 'Bank Operating Account',
    account_number: '0000000001',
    routing_number: generateRoutingNumber(),
    account_type: 'checking',
    customer_id: 0n,
    type: 'asset',
    ledger: LedgerCodes.Assets,
    code: BankAccountCodes.OperatingAccount,
    currency: 'USD',
    daily_limit: 0n,
    transaction_limit: 0n,
    overdraft_limit: 0n,
    interest_rate: 0,
    interest_accrued: 0n,
    debits_pending: 0n,
    debits_posted: 0n,
    credits_pending: 0n,
    credits_posted: BigInt(config.accounts) * 100000_00n, // Fund with plenty of capital
    is_active: true,
    is_frozen: false,
    opened_at: new Date('2000-01-01'),
    created_at: new Date(),
  }
  await createAccount(operatingAccount)
  accounts.push(operatingAccount)

  // Generate customer accounts
  const accountTypes: BankAccountType[] = ['checking', 'savings', 'checking', 'savings', 'money_market']
  for (const customer of customers) {
    // Each customer gets 1-3 accounts
    const numAccounts = randomInt(1, 3)
    for (let i = 0; i < numAccounts && accounts.length < config.accounts; i++) {
      const accountType = randomElement(accountTypes)
      const account = generateBankAccount(customer, accountType)
      await createAccount(account)
      accounts.push(account)
    }
  }

  // Fund customer accounts from operating account
  for (const account of accounts) {
    if (account.id !== operatingAccountId && account.credits_posted > 0n) {
      await createTransfer({
        id: generateId(),
        debit_account_id: operatingAccountId,
        credit_account_id: account.id,
        amount: account.credits_posted,
        currency: 'USD',
        ledger: account.ledger,
        code: BankTransferCodes.InternalTransfer,
        created_at: account.opened_at,
      })
    }
  }

  // Generate transfers between accounts
  const customerAccounts = accounts.filter(a => a.customer_id !== 0n)
  for (let i = 0; i < config.transfers; i++) {
    const fromAccount = randomElement(customerAccounts)
    let toAccount = randomElement(customerAccounts)
    // Ensure different accounts
    while (toAccount.id === fromAccount.id) {
      toAccount = randomElement(customerAccounts)
    }

    const transfer = generateBankTransfer(fromAccount, toAccount, config.amountRange)
    await createTransfer(transfer)
    transfers.push(transfer)
  }

  return { customers, accounts, transfers }
}

// ============================================================================
// Benchmark Queries
// ============================================================================

/**
 * Banking-specific benchmark queries
 */
export const bankingQueries = {
  /**
   * Customer account summary
   */
  customerAccountSummary: `
    SELECT
      a.account_number,
      a.account_type,
      a.credits_posted - a.debits_posted as balance,
      a.credits_pending - a.debits_pending as pending_balance
    FROM accounts a
    WHERE a.customer_id = $1 AND a.is_active = true
    ORDER BY a.account_type
  `,

  /**
   * Recent transactions for an account
   */
  recentTransactions: `
    SELECT
      t.created_at,
      t.transaction_type,
      t.status,
      CASE WHEN t.debit_account_id = $1 THEN -t.amount ELSE t.amount END as amount,
      t.merchant_name,
      t.authorization_code
    FROM transfers t
    WHERE (t.debit_account_id = $1 OR t.credit_account_id = $1)
    ORDER BY t.created_at DESC
    LIMIT $2
  `,

  /**
   * Daily transaction total
   */
  dailyTransactionTotal: `
    SELECT SUM(amount) as daily_total
    FROM transfers
    WHERE debit_account_id = $1
      AND DATE(created_at) = $2
      AND status = 'posted'
  `,

  /**
   * Account balance at a point in time
   */
  historicalBalance: `
    SELECT
      credits_posted - debits_posted as balance
    FROM accounts a
    WHERE a.id = $1
    -- For historical, would need to compute from transfers
  `,

  /**
   * Pending authorizations
   */
  pendingAuthorizations: `
    SELECT
      t.id,
      t.amount,
      t.merchant_name,
      t.authorized_at,
      t.authorization_code
    FROM transfers t
    WHERE t.debit_account_id = $1
      AND t.status = 'pending'
    ORDER BY t.authorized_at DESC
  `,

  /**
   * Monthly statement summary
   */
  monthlyStatement: `
    SELECT
      DATE(created_at) as date,
      COUNT(*) as transaction_count,
      SUM(CASE WHEN debit_account_id = $1 THEN -amount ELSE amount END) as net_change
    FROM transfers
    WHERE (debit_account_id = $1 OR credit_account_id = $1)
      AND created_at BETWEEN $2 AND $3
    GROUP BY DATE(created_at)
    ORDER BY date
  `,
}
