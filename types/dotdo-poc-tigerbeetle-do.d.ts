/**
 * Type declarations for @dotdo/poc-tigerbeetle-do package
 *
 * These are stub declarations to allow typecheck to pass while the
 * package source is not built. The actual implementation is in
 * packages/pocs/packages/tigerbeetle-do/
 */

declare module '@dotdo/poc-tigerbeetle-do' {
  /**
   * 128-bit unsigned integer class for TigerBeetle IDs and amounts
   */
  export class Uint128 {
    static ZERO: Uint128;
    static fromBigInt(value: bigint): Uint128;
    static fromBytes(bytes: Uint8Array): Uint128;
    static fromNumber(value: number): Uint128;
    toBigInt(): bigint;
    toString(): string;
  }

  /**
   * Account flags for TigerBeetle accounts
   */
  export const AccountFlags: {
    NONE: number;
    LINKED: number;
    DEBITS_MUST_NOT_EXCEED_CREDITS: number;
    CREDITS_MUST_NOT_EXCEED_DEBITS: number;
    HISTORY: number;
  };

  /**
   * Transfer flags for TigerBeetle transfers
   */
  export const TransferFlags: {
    NONE: number;
    LINKED: number;
    PENDING: number;
    POST_PENDING_TRANSFER: number;
    VOID_PENDING_TRANSFER: number;
  };

  /**
   * Input for creating an account
   */
  export interface CreateAccountInput {
    id: Uint128;
    user_data_128?: Uint128;
    user_data_64?: bigint;
    user_data_32?: number;
    ledger: number;
    code: number;
    flags?: number;
  }

  /**
   * Input for creating a transfer
   */
  export interface CreateTransferInput {
    id: Uint128;
    debit_account_id: Uint128;
    credit_account_id: Uint128;
    amount: Uint128;
    pending_id?: Uint128;
    user_data_128?: Uint128;
    user_data_64?: bigint;
    user_data_32?: number;
    timeout?: number;
    ledger: number;
    code: number;
    flags?: number;
  }

  /**
   * Account structure
   */
  export interface Account {
    id: Uint128;
    debits_pending: Uint128;
    debits_posted: Uint128;
    credits_pending: Uint128;
    credits_posted: Uint128;
    user_data_128: Uint128;
    user_data_64: bigint;
    user_data_32: number;
    ledger: number;
    code: number;
    flags: number;
    timestamp: bigint;
  }

  /**
   * Transfer structure
   */
  export interface Transfer {
    id: Uint128;
    debit_account_id: Uint128;
    credit_account_id: Uint128;
    amount: Uint128;
    pending_id: Uint128;
    user_data_128: Uint128;
    user_data_64: bigint;
    user_data_32: number;
    timeout: number;
    ledger: number;
    code: number;
    flags: number;
    timestamp: bigint;
  }

  /**
   * Result of account creation
   */
  export enum CreateAccountResult {
    ok = 0,
    linked_event_failed = 1,
    linked_event_chain_open = 2,
    exists = 3,
    // ... other error codes
  }

  /**
   * Result of transfer creation
   */
  export enum CreateTransferResult {
    ok = 0,
    linked_event_failed = 1,
    linked_event_chain_open = 2,
    exists = 3,
    // ... other error codes
  }

  /**
   * Result entry for batch operations
   */
  export interface CreateAccountResultEntry {
    index: number;
    result: CreateAccountResult;
  }

  export interface CreateTransferResultEntry {
    index: number;
    result: CreateTransferResult;
  }

  /**
   * Query filter for transfers
   */
  export interface TransferQuery {
    debit_account_id?: Uint128;
    credit_account_id?: Uint128;
  }

  /**
   * LedgerState configuration
   */
  export interface LedgerStateConfig {
    maxBatchSize?: number;
    getTimestamp?: () => bigint;
  }

  /**
   * Pure TypeScript implementation of TigerBeetle ledger state.
   * Provides in-memory double-entry bookkeeping with TigerBeetle semantics.
   */
  export class LedgerState {
    constructor(config?: LedgerStateConfig);

    /** Create accounts and return failed operations (TigerBeetle convention) */
    createAccounts(accounts: CreateAccountInput[]): CreateAccountResultEntry[];

    /** Look up accounts by ID */
    lookupAccounts(ids: Uint128[]): Account[];

    /** Create transfers and return failed operations (TigerBeetle convention) */
    createTransfers(transfers: CreateTransferInput[]): CreateTransferResultEntry[];

    /** Look up transfers by ID */
    lookupTransfers(ids: Uint128[]): Transfer[];

    /** Query transfers by account */
    queryTransfers(query: TransferQuery): Transfer[];

    /** Clear all state */
    clear(): void;

    /** Export state for serialization */
    exportState(): unknown;

    /** Import state from serialization */
    importState(state: unknown): void;

    /** Number of accounts in the ledger */
    get accountCount(): number;

    /** Number of transfers in the ledger */
    get transferCount(): number;

    /** Number of pending transfers in the ledger */
    get pendingTransferCount(): number;
  }
}

declare module '@dotdo/poc-tigerbeetle-do/core' {
  export { LedgerState, LedgerStateConfig } from '@dotdo/poc-tigerbeetle-do';
}

declare module '@dotdo/poc-tigerbeetle-do/types' {
  export {
    Uint128,
    Account,
    Transfer,
    CreateAccountInput,
    CreateTransferInput,
    CreateAccountResult,
    CreateTransferResult,
    CreateAccountResultEntry,
    CreateTransferResultEntry,
    AccountFlags,
    TransferFlags,
  } from '@dotdo/poc-tigerbeetle-do';

  /**
   * Helper function to create a Uint128 from various inputs
   */
  export function uint128(value: bigint | number | string): import('@dotdo/poc-tigerbeetle-do').Uint128;
}
