// relevant native action parameter type specs
export interface EosioEvmRaw {
    ram_payer: string,
    tx: string,
    estimate_gas: boolean,
    sender: null | string
}

export interface EosioEvmDeposit {
    from: string,
    to: string,
    quantity: string,
    memo: string
}

export interface EosioEvmWithdraw {
    to: string,
    quantity: string
}

// generic representations of data as it goes through the translator from source
// connector to target.

export interface IndexedInternalTx {
    callType: string
    from: string
    gas: bigint
    input: string
    to: string
    value: bigint
    gasUsed: bigint
    output: string
    subTraces: string
    traceAddress: string[]
    type: string
    depth: string
}

export interface IndexedTxLog {
    address?: string
    topics?: string[]
    data?: string
}

export interface IndexedTx {
    trxId: string
    trxIndex: number
    actionOrdinal: number
    blockNum: bigint
    blockHash: string

    hash: string
    raw: Uint8Array

    // tx params
    from?: string
    to?: string
    inputData: Uint8Array
    value: bigint
    nonce: bigint
    gasPrice: bigint
    gasLimit: bigint
    v: bigint
    r: bigint
    s: bigint

    // receipt
    status: 0 | 1
    itxs: IndexedInternalTx[]
    epoch: number
    createAddr?: string
    gasUsed: bigint
    gasUsedBlock: bigint
    chargedGasPrice: bigint
    output: string
    logs: IndexedTxLog[]
    logsBloom: Uint8Array
    errors: string[]
}

export interface IndexedAccountDelta {
    timestamp: bigint
    blockNum: bigint
    ordinal: number
    index: number
    address: string
    account: string
    nonce: number
    code: string
    balance: string
}

export interface IndexedAccountStateDelta {
    timestamp: bigint
    blockNum: bigint
    ordinal: number
    scope: string
    index: number
    key: string
    value: string
}

export interface IndexedBlockHeader {
    timestamp: bigint

    blockNum: bigint
    blockHash: Uint8Array

    evmBlockNum: bigint
    evmBlockHash: Uint8Array
    evmPrevHash: Uint8Array

    receiptsRoot: Uint8Array
    transactionsRoot: Uint8Array

    gasUsed: bigint
    gasLimit: bigint

    size: bigint

    transactionAmount: number
}

export interface IndexedBlock extends IndexedBlockHeader {

    transactions: IndexedTx[]
    logsBloom: Uint8Array

    deltas: {
        account: IndexedAccountDelta[];
        accountstate: IndexedAccountStateDelta[];
    }
}

export enum IndexerState {
    SYNC = 0,
    HEAD = 1
}

export type StartBlockInfo = {
    startBlock: bigint;
    startEvmBlock?: bigint;
    prevHash: Uint8Array;
}