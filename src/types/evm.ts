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
    gasPrice: EvmBigNumber,
    gasLimit: EvmBigNumber,
    to: string,
    value: EvmBigNumber,
    data: string,
    v: number,
    r: string,
    s: string
}
