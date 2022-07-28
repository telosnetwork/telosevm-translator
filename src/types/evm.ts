import Bloom from "../utils/evm";

const BN = require('bn.js');
const createKeccakHash = require('keccak');


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
    from: string,
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
    bloom?: Bloom,
    logsBloom?: string,
    errors?: string[],
    value_d?: number 
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

export interface EthGenesisParams {
    Config: {
        ChainID: number
    },
    Nonce: string,
    Timestamp: string,
    ExtraData: string,
    GasLimit: string,
    Difficulty: string,
    Mixhash: string,
    Coinbase: string,
    Alloc: {[key: string]: {balance: string}},
}

// 1,000,000,000
export const BLOCK_GAS_LIMIT = '0x3b9aca00'

export const ZERO_ADDRESS = '0x0000000000000000000000000000000000000000';
export const ZERO_HASH32 = '0x0000000000000000000000000000000000000000000000000000000000000000';

export const EMPTY_UNCLE_HASH = '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347';
export const EMPTY_SHA3 = '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421';
export const EMPTY_ROOT_HASH = '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421';

export class HexString {
    protected str: string;

    constructor(hexStr: string) {
        if (hexStr.startsWith('0x'))
            hexStr = hexStr.slice(2);
        else
            hexStr = hexStr;

        this.str = hexStr;
    }

    toString(): string {
        return this.str;
    }

    toPrefixedString(): string {
        return `0x${this.str}`; 
    }
};

export class BigIntHex extends HexString {
    constructor(val: string | number) {
        let hexStr = '';
        if (typeof val === 'string')
            hexStr = val;

        else
            hexStr = '0x' + new BN(val).toString(16);
        super(hexStr);
    }
};

export class Hash32 extends HexString {
    constructor(hexStr: string) {
        super(hexStr);

        if (this.str.length != (32 * 2))
            throw new TypeError('Hex string of wrong size!');
    }
};

export class TreeRoot extends Hash32 {};

export class EthAddress extends HexString {
    constructor(hexStr: string) {
        super(hexStr);

        if (this.str.length != (20 * 2))
            throw new TypeError('Hex string of wrong size!');
    }
};

export class BloomHex extends HexString {
    constructor(hexStr?: string) {
        if (!hexStr)
            hexStr = '00'.repeat(256);

        super(hexStr);

        if (this.str.length != (256 * 2))
            throw new TypeError('Hex string of wrong size!');
    }
};

export class NonceHex extends HexString {
    constructor(val: string | number) {
        let hexStr = '';
        if (typeof val === 'string')
            hexStr = val;

        else
            hexStr = '0x' + new BN(val).toString(16, 16);
        super(hexStr);

        if (this.str.length != (8 * 2))
            throw new TypeError('Hex string of wrong size!');
    }
};

type EthObjectValue = EthOrderedObject | HexString | Array<EthObjectValue>;

export function isEthOrderedObject(obj: any): obj is EthOrderedObject {
    return obj instanceof EthOrderedObject;
}

export function isEthObjectArray(obj: any): obj is Array<EthObjectValue> {
    if (obj instanceof Array) {
        for (const val of obj) {
            if (!isEthOrderedObject(obj) &&
                !(val instanceof HexString) &&
                !isEthObjectArray(val))
                return false;
        }
        return true;
    } else
        return false;
}

function applyToArray(
    array: Array<EthObjectValue>,
    ethObjFn: (val: EthOrderedObject, i: number) => any,
    hexStrFn: (val: HexString, i: number) => any
) {
    let i = 0;
    for (const value of array) {
        if (isEthOrderedObject(value))
            ethObjFn(value, i);

        else if (isEthObjectArray(value))
            applyToArray(value, ethObjFn, hexStrFn); 

        else if (value instanceof HexString)
            hexStrFn(value, i);
        i++;
    }
}

export class EthOrderedObject {
    data: Map<
        string,
        EthObjectValue
    >;

    constructor(attrs: Iterable<[string, EthObjectValue]>) {
         this.data = new Map(attrs);
    }

    get(key: string): EthObjectValue {
        return this.data.get(key);
    }

    has(key: string): boolean {
        return this.data.has(key);
    }

    set(key: string, val: EthObjectValue) {
        this.data.set(key, val);
    }

    hash(): Hash32 {
        const hash = createKeccakHash('keccak256');

        const ethObjFn = (obj: EthOrderedObject, i: number) => {
            hash.update(obj.hash());
        };

        const hexStrFn = (val: HexString, i: number) => {
            hash.update(val.toPrefixedString());
        };

        const hashArray: EthObjectValue[] = [];
        for (const val of this.data.values())
            hashArray.push(val);

        applyToArray(hashArray, ethObjFn, hexStrFn);

        return new Hash32(hash.digest('hex'));
    }

    toJSON() {

        const obj = Object.fromEntries(this.data);
        const newObj: {[k: string]: any} = {};
        const keysArray: string[] = [];

        const ethObjFn = (obj: EthOrderedObject, i: number) => {
            newObj[keysArray[i]] = obj.toJSON();
        };

        const hexStrFn = (val: HexString, i: number) => {
            newObj[keysArray[i]] = val.toPrefixedString();
        };

        const hashArray: EthObjectValue[] = [];
        for (const [key, val] of Object.entries(obj)) {
            keysArray.push(key);
            hashArray.push(val);
        }

        applyToArray(hashArray, ethObjFn, hexStrFn);

        return newObj;
    }
}

export function ethConfigParams(chainId: number) {
    return new EthOrderedObject([
        ['chainId', new BigIntHex(chainId)]
    ]);
}

export function ethGenesisParams(
    timestamp: number,
    chainId: number,
    gasLimit: number,
    extraData: string
) {
    return new EthOrderedObject([
        ['config', ethConfigParams(chainId)],
        ['nonce', new NonceHex(0)],
        ['timestamp', new BigIntHex(timestamp)],
        ['extraData', new HexString(extraData)],
        ['gasLimit', new BigIntHex(gasLimit)],
        ['difficulty', new BigIntHex(0)],
        ['mixHash', new Hash32(ZERO_HASH32)],
        ['coinbase', new EthAddress(ZERO_ADDRESS)],
        ['alloc', new EthOrderedObject([])]
    ]);
}

export function ethBlockHeader() {
    return new EthOrderedObject([
        ['parentHash', new Hash32(ZERO_HASH32)],
        ['unclesHash', new Hash32(EMPTY_UNCLE_HASH)],
        ['coinbase', new EthAddress(ZERO_ADDRESS)],
        ['stateRoot', new TreeRoot(EMPTY_ROOT_HASH)],
        ['transactionRoot', new TreeRoot(EMPTY_ROOT_HASH)],
        ['receiptRoot', new TreeRoot(EMPTY_ROOT_HASH)],
        ['bloom', new BloomHex()],
        ['difficulty', new BigIntHex(0)],
        ['blockNumber', new BigIntHex(0)],
        ['gasLimit', new BigIntHex(0)],
        ['gasUsed', new BigIntHex(0)],
        ['timestamp', new BigIntHex(0)],
        ['extraData', new HexString('')],
        ['mixHash', new Hash32(ZERO_HASH32)],
        ['nonce', new NonceHex(0)]
    ]);
}

export function ethReceipt() {
    return new EthOrderedObject([
        ['postState', new BigIntHex(0)],
        ['gasUsed', new BigIntHex(0)],
        ['bloom', new BloomHex()],
        ['logs', []]
    ]);
}
