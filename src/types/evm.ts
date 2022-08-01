import {Transaction} from "@ethereumjs/tx"

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

export interface EvmBigNumber {
    _hex: string
}

export interface EvmTransaction {
    from: string,
    nonce: number,
    gasPrice: string,
    gasLimit: string,
    to: string,
    value: string,
    data: string,
    v: number | string,
    r: string,
    s: string
}


// Hyperion plugin compat

export interface InteralEvmTransaction {
    callType: string,
    from: string,
    gas: string,
    input: string,
    input_trimmed: string,
    to: string,
    value: string,
    gasUsed: string,
    output: string,
    subtraces: number,
    traceAddress: string,
    type: string,
    depth: number,
    extra: any
}

export interface StorageEvmTransaction {
    hash: string,
    from?: string,
    trx_index: number,
    block: number,
    block_hash: string,
    to: string,
    input_data: string,
    input_trimmed: string,
    value: string,
    nonce: string,
    gas_price: string,
    gas_limit: string,
    status: number,
    itxs: InteralEvmTransaction[],
    epoch: number,
    createdaddr: string,
    gasused: number,
    gasusedblock: number,
    charged_gas_price: number,
    output: string,
    logs?: {
        address: string,
        topics: string[],
        data: string
    }[],
    logsBloom?: string,
    errors?: string[],
    value_d?: number,
    raw?: Buffer
}

export interface StorageEosioAction {
    "@timestamp": string,
    "trx_id": string,
    "action_ordinal": number,
    "signatures": string[],
    "@raw": StorageEvmTransaction
}

export interface StorageEosioDelta {
    "@timestamp": string,
    "block_num": number,
    "@global": {
        "block_num": number
    },
    "@evmBlockHash": string
}
