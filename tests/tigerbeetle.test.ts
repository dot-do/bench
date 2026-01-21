/**
 * TigerBeetle WASM Adapter Tests
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import {
  createTigerBeetleStore,
  generateId,
  AccountFlags,
  TransferFlags,
  CreateAccountResultCode,
  CreateTransferResultCode,
  type TigerBeetleStore,
} from '../databases/tigerbeetle'

describe('TigerBeetle WASM Adapter', () => {
  let store: TigerBeetleStore

  beforeEach(async () => {
    store = await createTigerBeetleStore()
  })

  afterEach(async () => {
    await store.close()
  })

  describe('Account Operations', () => {
    it('should create accounts', async () => {
      const accountId = generateId()
      const results = await store.createAccounts([
        {
          id: accountId,
          ledger: 1,
          code: 1,
          flags: AccountFlags.None,
        },
      ])

      expect(results).toHaveLength(1)
      expect(results[0].result).toBe(CreateAccountResultCode.ok)
    })

    it('should lookup accounts', async () => {
      const accountId = generateId()
      await store.createAccounts([
        {
          id: accountId,
          ledger: 1,
          code: 1,
          flags: AccountFlags.None,
        },
      ])

      const accounts = await store.lookupAccounts([accountId])
      expect(accounts).toHaveLength(1)
      expect(accounts[0].id).toBe(accountId)
      expect(accounts[0].ledger).toBe(1)
      expect(accounts[0].code).toBe(1)
    })

    it('should handle duplicate account creation (idempotent)', async () => {
      const accountId = generateId()

      // First creation should succeed
      const results1 = await store.createAccounts([
        { id: accountId, ledger: 1, code: 1 },
      ])
      expect(results1[0].result).toBe(CreateAccountResultCode.ok)

      // Second creation with identical fields is idempotent - also succeeds
      // TigerBeetle semantics: idempotent creates return success, not "exists"
      const results2 = await store.createAccounts([
        { id: accountId, ledger: 1, code: 1 },
      ])
      // The underlying LedgerState returns empty array for idempotent success
      // Our adapter maps this to ok (0), not exists (21)
      expect(results2[0].result).toBe(CreateAccountResultCode.ok)
    })

    it('should detect duplicate account creation with different fields', async () => {
      const accountId = generateId()

      // First creation
      await store.createAccounts([
        { id: accountId, ledger: 1, code: 1 },
      ])

      // Second creation with different code should fail
      const results = await store.createAccounts([
        { id: accountId, ledger: 1, code: 2 }, // Different code
      ])
      expect(results[0].result).toBe(CreateAccountResultCode.exists_with_different_code)
    })
  })

  describe('Transfer Operations', () => {
    let account1Id: bigint
    let account2Id: bigint

    beforeEach(async () => {
      account1Id = generateId()
      account2Id = generateId()
      await store.createAccounts([
        { id: account1Id, ledger: 1, code: 1, flags: AccountFlags.None },
        { id: account2Id, ledger: 1, code: 1, flags: AccountFlags.None },
      ])
    })

    it('should create transfers', async () => {
      const transferId = generateId()
      const results = await store.createTransfers([
        {
          id: transferId,
          debit_account_id: account1Id,
          credit_account_id: account2Id,
          amount: 1000n,
          ledger: 1,
          code: 1,
          flags: TransferFlags.None,
        },
      ])

      expect(results).toHaveLength(1)
      expect(results[0].result).toBe(CreateTransferResultCode.ok)
    })

    it('should update account balances after transfer', async () => {
      await store.createTransfers([
        {
          id: generateId(),
          debit_account_id: account1Id,
          credit_account_id: account2Id,
          amount: 1000n,
          ledger: 1,
          code: 1,
          flags: TransferFlags.None,
        },
      ])

      const accounts = await store.lookupAccounts([account1Id, account2Id])
      const debitAccount = accounts.find((a) => a.id === account1Id)!
      const creditAccount = accounts.find((a) => a.id === account2Id)!

      expect(debitAccount.debits_posted).toBe(1000n)
      expect(debitAccount.credits_posted).toBe(0n)
      expect(creditAccount.debits_posted).toBe(0n)
      expect(creditAccount.credits_posted).toBe(1000n)
    })

    it('should lookup transfers', async () => {
      const transferId = generateId()
      await store.createTransfers([
        {
          id: transferId,
          debit_account_id: account1Id,
          credit_account_id: account2Id,
          amount: 500n,
          ledger: 1,
          code: 1,
        },
      ])

      const transfers = await store.lookupTransfers([transferId])
      expect(transfers).toHaveLength(1)
      expect(transfers[0].id).toBe(transferId)
      expect(transfers[0].amount).toBe(500n)
    })

    it('should reject transfer to non-existent account', async () => {
      const nonExistentId = generateId()
      const results = await store.createTransfers([
        {
          id: generateId(),
          debit_account_id: account1Id,
          credit_account_id: nonExistentId,
          amount: 100n,
          ledger: 1,
          code: 1,
        },
      ])

      expect(results[0].result).toBe(CreateTransferResultCode.credit_account_not_found)
    })
  })

  describe('Batch Operations', () => {
    it('should create accounts in batches', async () => {
      const accounts = Array.from({ length: 100 }, () => ({
        id: generateId(),
        ledger: 1,
        code: 1,
      }))

      const results = await store.createAccountsBatch(accounts, 25)
      expect(results).toHaveLength(100)

      const successCount = results.filter((r) => r.result === CreateAccountResultCode.ok).length
      expect(successCount).toBe(100)
    })

    it('should create transfers in batches', async () => {
      // Create accounts first
      const accountIds = Array.from({ length: 10 }, () => generateId())
      await store.createAccountsBatch(
        accountIds.map((id) => ({ id, ledger: 1, code: 1 }))
      )

      // Create transfers between accounts
      const transfers = Array.from({ length: 50 }, (_, i) => ({
        id: generateId(),
        debit_account_id: accountIds[i % 10],
        credit_account_id: accountIds[(i + 1) % 10],
        amount: BigInt(100 + i),
        ledger: 1,
        code: 1,
      }))

      const results = await store.createTransfersBatch(transfers, 10)
      expect(results).toHaveLength(50)

      const successCount = results.filter((r) => r.result === CreateTransferResultCode.ok).length
      expect(successCount).toBe(50)
    })
  })

  describe('getLedger()', () => {
    it('should provide access to underlying LedgerState', async () => {
      const ledger = store.getLedger()
      expect(ledger).toBeDefined()
      expect(ledger.accountCount).toBe(0)

      await store.createAccounts([{ id: generateId(), ledger: 1, code: 1 }])
      expect(ledger.accountCount).toBe(1)
    })
  })
})
