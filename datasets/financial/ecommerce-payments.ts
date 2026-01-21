/**
 * E-commerce Payments Dataset
 *
 * Represents a payment processing system for e-commerce with:
 * - Merchant accounts
 * - Customer payment methods
 * - Orders and payments
 * - Refunds and chargebacks
 * - Settlement and disbursement
 *
 * Models real-world payment flows:
 * - Authorization (pending)
 * - Capture (post pending)
 * - Settlement to merchant
 * - Refund processing
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
// E-commerce Payment Types
// ============================================================================

/**
 * Payment method types
 */
export type PaymentMethodType = 'credit_card' | 'debit_card' | 'bank_account' | 'wallet' | 'bnpl'

/**
 * Card networks
 */
export type CardNetwork = 'visa' | 'mastercard' | 'amex' | 'discover'

/**
 * Payment status
 */
export type PaymentStatus =
  | 'pending'
  | 'authorized'
  | 'captured'
  | 'settled'
  | 'refunded'
  | 'partially_refunded'
  | 'disputed'
  | 'failed'
  | 'cancelled'

/**
 * Merchant account
 */
export interface MerchantAccount extends FinancialAccount {
  merchant_id: string
  business_name: string
  mcc: string // Merchant Category Code
  processing_rate: number // Basis points (e.g., 275 = 2.75%)
  settlement_delay_days: number
  // Balance types
  available_balance: bigint
  pending_balance: bigint
  reserved_balance: bigint
  // Status
  is_active: boolean
  is_high_risk: boolean
}

/**
 * Customer wallet/payment source
 */
export interface CustomerWallet extends FinancialAccount {
  customer_id: string
  email: string
  // Default payment method
  default_payment_method_id?: string
}

/**
 * Payment method (stored card, bank account, etc.)
 */
export interface PaymentMethod {
  id: string
  customer_id: string
  type: PaymentMethodType
  // Card details (masked)
  card_last4?: string
  card_brand?: CardNetwork
  card_exp_month?: number
  card_exp_year?: number
  // Bank details (masked)
  bank_last4?: string
  bank_name?: string
  // Status
  is_default: boolean
  is_verified: boolean
  created_at: Date
}

/**
 * Order record
 */
export interface Order {
  id: string
  merchant_id: string
  customer_id: string
  // Amounts
  subtotal: bigint
  tax: bigint
  shipping: bigint
  discount: bigint
  total: bigint
  currency: string
  // Status
  status: 'pending' | 'paid' | 'fulfilled' | 'cancelled' | 'refunded'
  // Timestamps
  created_at: Date
  paid_at?: Date
  fulfilled_at?: Date
}

/**
 * Payment record
 */
export interface Payment extends FinancialTransfer {
  order_id: string
  merchant_id: string
  customer_id: string
  payment_method_id: string
  // Status
  status: PaymentStatus
  // Authorization
  authorization_id?: string
  authorized_amount?: bigint
  authorized_at?: Date
  // Capture
  captured_amount?: bigint
  captured_at?: Date
  // Settlement
  settlement_id?: string
  settlement_amount?: bigint
  settled_at?: Date
  // Fees
  processing_fee: bigint
  platform_fee: bigint
  // Metadata
  failure_reason?: string
  risk_score?: number
}

/**
 * Refund record
 */
export interface Refund extends FinancialTransfer {
  payment_id: string
  order_id: string
  reason: 'requested_by_customer' | 'duplicate' | 'fraudulent' | 'order_change' | 'other'
  status: 'pending' | 'succeeded' | 'failed'
}

// ============================================================================
// Payment Processing Codes
// ============================================================================

/**
 * E-commerce account codes
 */
export const PaymentAccountCodes = {
  // Platform accounts
  PlatformOperating: 1001,
  PlatformFees: 4001,
  ProcessingFees: 5001,
  ChargebackReserve: 2001,

  // Merchant accounts
  MerchantSettlement: 1101,
  MerchantPending: 1102,
  MerchantReserve: 1103,

  // Customer wallets
  CustomerWallet: 1201,
  CustomerCredits: 2201,
} as const

/**
 * Payment transfer codes
 */
export const PaymentTransferCodes = {
  // Customer to Platform
  Authorization: 101,
  Capture: 102,
  Purchase: 103,

  // Platform to Merchant
  Settlement: 201,
  Adjustment: 202,

  // Refunds
  RefundToCustomer: 301,
  RefundFromMerchant: 302,

  // Fees
  ProcessingFee: 401,
  PlatformFee: 402,
  ChargebackFee: 403,

  // Disputes
  DisputeHold: 501,
  DisputeRelease: 502,
  ChargebackDebit: 503,
} as const

// ============================================================================
// Data Generation
// ============================================================================

let idCounter = 1n

function generateId(): bigint {
  return idCounter++
}

function generateStringId(): string {
  return `id_${Date.now()}_${Math.random().toString(36).slice(2, 11)}`
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
 * Generate a merchant account
 */
function generateMerchant(): MerchantAccount {
  const id = generateId()
  const merchantId = `merch_${generateStringId()}`

  const businessNames = [
    'Tech Gadgets Plus',
    'Fashion Forward',
    'Home Essentials Co',
    'Sports & Outdoors Depot',
    'Beauty & Wellness Store',
    'Digital Downloads Hub',
    'Gourmet Foods Market',
    'Pet Supplies Direct',
    'Office Solutions Inc',
    'Kids & Baby World',
  ]

  const mccs = ['5411', '5651', '5732', '5941', '5977', '5999', '7299', '5691', '5045', '5641']

  return {
    id,
    merchant_id: merchantId,
    business_name: randomElement(businessNames),
    mcc: randomElement(mccs),
    name: `Merchant ${merchantId}`,
    type: 'asset',
    ledger: LedgerCodes.Assets,
    code: PaymentAccountCodes.MerchantSettlement,
    currency: 'USD',
    processing_rate: randomInt(200, 350), // 2.00% - 3.50%
    settlement_delay_days: randomInt(1, 3),
    available_balance: 0n,
    pending_balance: 0n,
    reserved_balance: 0n,
    debits_pending: 0n,
    debits_posted: 0n,
    credits_pending: 0n,
    credits_posted: 0n,
    is_active: true,
    is_high_risk: Math.random() < 0.1, // 10% high risk
    created_at: randomDate(new Date('2020-01-01'), new Date('2024-01-01')),
  }
}

/**
 * Generate a customer wallet
 */
function generateCustomer(): CustomerWallet {
  const id = generateId()
  const customerId = `cus_${generateStringId()}`

  return {
    id,
    customer_id: customerId,
    email: `customer${id}@example.com`,
    name: `Customer ${customerId}`,
    type: 'liability', // Customer funds held by platform
    ledger: LedgerCodes.Liabilities,
    code: PaymentAccountCodes.CustomerWallet,
    currency: 'USD',
    debits_pending: 0n,
    debits_posted: 0n,
    credits_pending: 0n,
    credits_posted: randomAmount(0n, 1000_00n), // $0 - $10 store credit
    created_at: randomDate(new Date('2022-01-01'), new Date()),
  }
}

/**
 * Generate a payment method
 */
function generatePaymentMethod(customer: CustomerWallet): PaymentMethod {
  const types: PaymentMethodType[] = ['credit_card', 'credit_card', 'debit_card', 'bank_account', 'wallet']
  const type = randomElement(types)

  const brands: CardNetwork[] = ['visa', 'mastercard', 'amex', 'discover']

  return {
    id: `pm_${generateStringId()}`,
    customer_id: customer.customer_id,
    type,
    card_last4: type.includes('card') ? String(randomInt(1000, 9999)) : undefined,
    card_brand: type.includes('card') ? randomElement(brands) : undefined,
    card_exp_month: type.includes('card') ? randomInt(1, 12) : undefined,
    card_exp_year: type.includes('card') ? randomInt(2025, 2030) : undefined,
    bank_last4: type === 'bank_account' ? String(randomInt(1000, 9999)) : undefined,
    bank_name: type === 'bank_account' ? 'Chase Bank' : undefined,
    is_default: true,
    is_verified: true,
    created_at: randomDate(customer.created_at, new Date()),
  }
}

/**
 * Generate an order
 */
function generateOrder(merchant: MerchantAccount, customer: CustomerWallet): Order {
  const subtotal = randomAmount(10_00n, 500_00n) // $10 - $500
  const tax = (subtotal * 8n) / 100n // 8% tax
  const shipping = randomAmount(0n, 15_00n) // $0 - $15 shipping
  const discount = Math.random() < 0.3 ? randomAmount(0n, subtotal / 5n) : 0n // 30% chance of discount

  return {
    id: `ord_${generateStringId()}`,
    merchant_id: merchant.merchant_id,
    customer_id: customer.customer_id,
    subtotal,
    tax,
    shipping,
    discount,
    total: subtotal + tax + shipping - discount,
    currency: 'USD',
    status: 'pending',
    created_at: randomDate(new Date('2024-01-01'), new Date()),
  }
}

/**
 * Generate a payment for an order
 */
function generatePayment(
  order: Order,
  merchant: MerchantAccount,
  customer: CustomerWallet,
  paymentMethod: PaymentMethod,
  platformAccountId: bigint
): Payment {
  const id = generateId()

  // Calculate fees
  const processingFee = (order.total * BigInt(merchant.processing_rate)) / 10000n
  const platformFee = (order.total * 50n) / 10000n // 0.50% platform fee

  const createdAt = new Date(order.created_at.getTime() + randomInt(60000, 3600000)) // 1 min - 1 hour after order

  return {
    id,
    order_id: order.id,
    merchant_id: merchant.merchant_id,
    customer_id: customer.customer_id,
    payment_method_id: paymentMethod.id,
    debit_account_id: customer.id, // Debit customer
    credit_account_id: platformAccountId, // Credit platform (will later settle to merchant)
    amount: order.total,
    currency: 'USD',
    ledger: LedgerCodes.Assets,
    code: PaymentTransferCodes.Capture,
    status: 'captured',
    authorization_id: `auth_${generateStringId()}`,
    authorized_amount: order.total,
    authorized_at: new Date(createdAt.getTime() - 60000),
    captured_amount: order.total,
    captured_at: createdAt,
    processing_fee: processingFee,
    platform_fee: platformFee,
    risk_score: randomInt(1, 100),
    created_at: createdAt,
  }
}

// ============================================================================
// Seed Functions
// ============================================================================

/**
 * Generate seed data for e-commerce payment benchmarks
 */
export async function seedEcommercePaymentsData(
  size: DatasetSize,
  createAccount: (account: Partial<MerchantAccount | CustomerWallet>) => Promise<{ id: bigint }>,
  createTransfer: (transfer: Partial<Payment | Refund>) => Promise<void>
): Promise<{
  merchants: MerchantAccount[]
  customers: CustomerWallet[]
  paymentMethods: PaymentMethod[]
  orders: Order[]
  payments: Payment[]
}> {
  const config = seedConfigs[size]
  const merchants: MerchantAccount[] = []
  const customers: CustomerWallet[] = []
  const paymentMethods: PaymentMethod[] = []
  const orders: Order[] = []
  const payments: Payment[] = []

  // Create platform operating account
  const platformAccountId = generateId()
  const platformAccount: FinancialAccount = {
    id: platformAccountId,
    name: 'Platform Operating Account',
    type: 'asset',
    ledger: LedgerCodes.Assets,
    code: PaymentAccountCodes.PlatformOperating,
    currency: 'USD',
    debits_pending: 0n,
    debits_posted: 0n,
    credits_pending: 0n,
    credits_posted: BigInt(config.accounts) * 1000_00n, // Fund platform
    created_at: new Date('2020-01-01'),
  }
  await createAccount(platformAccount)

  // Calculate distribution
  const merchantCount = Math.ceil(config.accounts * 0.1) // 10% merchants
  const customerCount = config.accounts - merchantCount

  // Generate merchants
  for (let i = 0; i < merchantCount; i++) {
    const merchant = generateMerchant()
    await createAccount(merchant)
    merchants.push(merchant)
  }

  // Generate customers with payment methods
  for (let i = 0; i < customerCount; i++) {
    const customer = generateCustomer()
    await createAccount(customer)
    customers.push(customer)

    // Each customer has 1-3 payment methods
    const numMethods = randomInt(1, 3)
    for (let j = 0; j < numMethods; j++) {
      const method = generatePaymentMethod(customer)
      if (j > 0) method.is_default = false
      paymentMethods.push(method)
    }
  }

  // Fund customer accounts for payments
  for (const customer of customers) {
    if (customer.credits_posted > 0n) {
      await createTransfer({
        id: generateId(),
        debit_account_id: platformAccountId,
        credit_account_id: customer.id,
        amount: customer.credits_posted,
        currency: 'USD',
        ledger: customer.ledger,
        code: PaymentTransferCodes.Purchase,
        created_at: customer.created_at,
      })
    }
  }

  // Generate orders and payments
  for (let i = 0; i < config.transfers; i++) {
    const merchant = randomElement(merchants)
    const customer = randomElement(customers)
    const customerMethods = paymentMethods.filter((m) => m.customer_id === customer.customer_id)
    const paymentMethod = customerMethods.length > 0 ? randomElement(customerMethods) : paymentMethods[0]

    const order = generateOrder(merchant, customer)
    orders.push(order)

    // Ensure customer has sufficient balance
    customer.credits_posted = customer.credits_posted + order.total * 2n

    const payment = generatePayment(order, merchant, customer, paymentMethod, platformAccountId)
    await createTransfer(payment)
    payments.push(payment)

    // 5% of payments result in refunds
    if (Math.random() < 0.05) {
      const refundAmount = Math.random() < 0.7 ? payment.amount : payment.amount / 2n // 70% full refund
      const refund: Refund = {
        id: generateId(),
        payment_id: String(payment.id),
        order_id: order.id,
        debit_account_id: platformAccountId,
        credit_account_id: customer.id,
        amount: refundAmount,
        currency: 'USD',
        ledger: LedgerCodes.Assets,
        code: PaymentTransferCodes.RefundToCustomer,
        reason: randomElement(['requested_by_customer', 'duplicate', 'order_change']),
        status: 'succeeded',
        created_at: new Date(payment.created_at.getTime() + randomInt(86400000, 604800000)), // 1-7 days later
      }
      await createTransfer(refund)
    }
  }

  return { merchants, customers, paymentMethods, orders, payments }
}

// ============================================================================
// Benchmark Queries
// ============================================================================

/**
 * E-commerce payment benchmark queries
 */
export const ecommercePaymentQueries = {
  /**
   * Merchant daily sales summary
   */
  merchantDailySales: `
    SELECT
      DATE(created_at) as date,
      COUNT(*) as transaction_count,
      SUM(amount) as gross_volume,
      SUM(processing_fee) as total_fees,
      SUM(amount - processing_fee - platform_fee) as net_volume
    FROM payments
    WHERE merchant_id = $1
      AND status = 'captured'
      AND created_at BETWEEN $2 AND $3
    GROUP BY DATE(created_at)
    ORDER BY date DESC
  `,

  /**
   * Customer payment history
   */
  customerPaymentHistory: `
    SELECT
      p.id,
      p.created_at,
      p.amount,
      p.status,
      o.id as order_id,
      m.business_name as merchant_name
    FROM payments p
    JOIN orders o ON p.order_id = o.id
    JOIN merchants m ON p.merchant_id = m.merchant_id
    WHERE p.customer_id = $1
    ORDER BY p.created_at DESC
    LIMIT $2
  `,

  /**
   * Pending settlements for merchant
   */
  pendingSettlements: `
    SELECT
      SUM(amount - processing_fee - platform_fee) as pending_amount,
      COUNT(*) as pending_count,
      MIN(created_at) as oldest_payment
    FROM payments
    WHERE merchant_id = $1
      AND status = 'captured'
      AND settled_at IS NULL
  `,

  /**
   * Refund rate by merchant
   */
  merchantRefundRate: `
    SELECT
      m.merchant_id,
      m.business_name,
      COUNT(DISTINCT p.id) as total_payments,
      COUNT(DISTINCT r.id) as total_refunds,
      COALESCE(SUM(r.amount), 0) as refund_amount,
      SUM(p.amount) as gross_volume,
      ROUND(COUNT(DISTINCT r.id)::numeric / COUNT(DISTINCT p.id) * 100, 2) as refund_rate
    FROM merchants m
    LEFT JOIN payments p ON m.merchant_id = p.merchant_id
    LEFT JOIN refunds r ON p.id = r.payment_id
    WHERE p.created_at BETWEEN $1 AND $2
    GROUP BY m.merchant_id, m.business_name
    ORDER BY refund_rate DESC
  `,

  /**
   * Platform revenue summary
   */
  platformRevenue: `
    SELECT
      DATE(created_at) as date,
      SUM(platform_fee) as platform_revenue,
      SUM(processing_fee) as processing_revenue,
      SUM(amount) as gross_volume,
      COUNT(*) as transaction_count
    FROM payments
    WHERE status IN ('captured', 'settled')
      AND created_at BETWEEN $1 AND $2
    GROUP BY DATE(created_at)
    ORDER BY date
  `,

  /**
   * High-value transactions (for fraud monitoring)
   */
  highValueTransactions: `
    SELECT
      p.id,
      p.amount,
      p.risk_score,
      p.customer_id,
      p.merchant_id,
      m.is_high_risk,
      p.created_at
    FROM payments p
    JOIN merchants m ON p.merchant_id = m.merchant_id
    WHERE p.amount > $1
      AND p.created_at > NOW() - INTERVAL '24 hours'
    ORDER BY p.risk_score DESC, p.amount DESC
    LIMIT $2
  `,
}
