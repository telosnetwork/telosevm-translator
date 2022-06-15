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
        topics: string[]
    }[],
    logsBloom?: string,
    errors?: string[],
    value_d?: number 
}

export interface StorageEosioAction {
    "@timestamp": string,
    "@raw": StorageEvmTransaction
}

export interface StorageEosioDelta {
    "@timestamp": string,
    "@global": {
        "block_num": number
    },
    "@evmBlockHash": string
}
