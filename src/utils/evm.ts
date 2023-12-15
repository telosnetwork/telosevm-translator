import {BlockHeader, BlockOptions, HeaderData} from "@ethereumjs/block";
import {addHexPrefix} from '@ethereumjs/util';
import {StorageEvmTransaction} from "../types/evm.js";
import {Trie} from "@ethereumjs/trie";
import RLP from "rlp";
import {Bloom, encodeReceipt, TxReceipt} from "@ethereumjs/vm";
import type {Log} from "@ethereumjs/evm";
import {TransactionType} from "@ethereumjs/tx";
import {JsonRpc} from "eosjs";
import {Logger} from "winston";
import {nameToUint64} from "./eosio.js";
import {sleep} from "./indexer.js";
import moment from "moment";

export function numberToHex(num: number): string {
    let hex = num.toString(16);
    if (hex.length % 2 !== 0) {
        hex = '0' + hex;
    }
    return '0x' + hex;
}

export function arrayToHex(array: Uint8Array) {
    if (!array)
        return '';
    return Array.from(array, byte => byte.toString(16).padStart(2, '0')).join('').toLowerCase();
}

export function removeHexPrefix(str: string) {
    if (str.startsWith('0x')) {
        return str.slice(2);
    } else {
        return str;
    }
}

export function hexStringToUint8Array(hexString: string): Uint8Array {
    hexString = removeHexPrefix(hexString);

    // Ensure the hex string length is even
    if (hexString.length % 2 !== 0)
        throw new Error('Invalid hex string');

    // Convert the hex string to a byte array
    const byteArray = new Uint8Array(hexString.length / 2);
    for (let i = 0, len = hexString.length; i < len; i += 2) {
        byteArray[i / 2] = parseInt(hexString.substring(i, i + 2), 16);
    }

    return byteArray;
}

export const ZERO_ADDR = '0x0000000000000000000000000000000000000000';
export const NULL_HASH = '0x0000000000000000000000000000000000000000000000000000000000000000';
export const EMPTY_TRIE_BUF = new Trie().EMPTY_TRIE_ROOT;
export const EMPTY_TRIE = arrayToHex(EMPTY_TRIE_BUF);

// 1,000,000,000
export const BLOCK_GAS_LIMIT = '0x3b9aca00'

export const NEW_HEADS_TEMPLATE = {
    difficulty: "0x0",
    extraData: NULL_HASH,
    gasLimit: BLOCK_GAS_LIMIT,
    miner: ZERO_ADDR,
    nonce: "0x0000000000000000",
    parentHash: NULL_HASH,
    receiptsRoot: EMPTY_TRIE,
    sha3Uncles: NULL_HASH,
    stateRoot: EMPTY_TRIE,
    transactionsRoot: EMPTY_TRIE,
};


export function numToHex(input: number | string) {
    if (typeof input === 'number') {
        return '0x' + input.toString(16);
    } else {
        return '0x' + BigInt(input).toString(16);
    }
}

export function generateUniqueVRS(
    blockHash: string,
    sender: string,
    trx_index: number
): [bigint, bigint, bigint] {
    const v = BigInt(42);  // why is v 42? well cause its the anwser to life

    const trxIndexBI = BigInt(trx_index);
    const blockHashBI = BigInt(addHexPrefix(blockHash.toLowerCase()));

    const r = blockHashBI + trxIndexBI;
    const s = BigInt(
        addHexPrefix(
            removeHexPrefix(sender.toLowerCase()).padEnd(64, '0')
        )
    );

    return [v, r, s];
}


/*
 * Custom BlockHeader without dao-hard-fork validation
 */

export class TEVMBlockHeader extends BlockHeader {

    /**
     * Static constructor to create a block header from a header data dictionary
     *
     * @param headerData
     * @param opts
     */
    public static fromHeaderData(headerData: HeaderData = {}, opts: BlockOptions = {}) {
        return new TEVMBlockHeader(headerData, opts)
    }

    /**
     * Validates extra data is DAO_ExtraData for DAO_ForceExtraDataRange blocks after DAO
     * activation block (see: https://blog.slock.it/hard-fork-specification-24b889e70703)
     */
    protected _validateDAOExtraData() {}

}

export interface EVMTxWrapper {
    trx_id: string,
    action_ordinal: number,
    signatures: string[],
    evmTx: StorageEvmTransaction
};


export const DS_TYPES = [
    'transaction',
    'code_id',
    'account_v0',
    'account_metadata_v0',
    'code_v0',
    'contract_table_v0',
    'contract_row_v0',
    'contract_index64_v0',
    'contract_index128_v0',
    'contract_index256_v0',
    'contract_index_double_v0',
    'contract_index_long_double_v0',
    'account',
    'account_metadata',
    'code',
    'contract_table',
    'contract_row',
    'contract_index64',
    'contract_index128',
    'contract_index256',
    'contract_index_double',
    'contract_index_long_double'
];


export class StorageEosioDelta {
    "@timestamp": string;
    block_num: number;
    "@global": {
        block_num: number
    };
    "@blockHash": string;
    "@evmBlockHash": string;
    "@evmPrevBlockHash": string;
    "@receiptsRootHash": string;
    "@transactionsRoot": string;
    gasUsed: string;
    gasLimit: string;
    size: string;

    code: string;
    table: string;

    constructor(obj: Partial<StorageEosioDelta>) {
        Object.assign(this, obj);
    }
}

export class ProcessedBlock {
    nativeBlockHash: string;
    nativeBlockNumber: number;
    evmBlockNumber: number;
    blockTimestamp: string;
    evmTxs: Array<EVMTxWrapper>;
    errors: Array<TxDeserializationError>;

    constructor(obj: Partial<ProcessedBlock>) {
        Object.assign(this, obj);
    }
}

export async function generateBlockApplyInfo(evmTxs: Array<EVMTxWrapper>) {
    let gasUsed = BigInt(0);
    let gasLimit = BigInt(0);
    let size = BigInt(0);
    const txsRootHash = new Trie();
    const receiptsTrie = new Trie();
    const blockBloom = new Bloom();
    for (const [i, tx] of evmTxs.entries()) {
        gasUsed += BigInt(tx.evmTx.gasused);
        gasLimit += BigInt(tx.evmTx.gas_limit);
        size += BigInt(tx.evmTx.raw.length);

        const logs: Log[] = [];

        await txsRootHash.put(RLP.encode(i), tx.evmTx.raw);

        let bloom = new Bloom();
        if (tx.evmTx.logsBloom)
            bloom = new Bloom(hexStringToUint8Array(tx.evmTx.logsBloom));

        blockBloom.or(bloom);

        if (tx.evmTx.logs) {
            for (const hexLogs of tx.evmTx.logs) {
                const topics: Uint8Array[] = [];

                for (const topic of hexLogs.topics)
                    topics.push(hexStringToUint8Array(topic.padStart(64, '0')))

                logs.push([
                    hexStringToUint8Array(hexLogs.address.padStart(40, '0')),
                    topics,
                    hexStringToUint8Array(hexLogs.data )
                ]);
            }
        }

        const receipt: TxReceipt = {
            cumulativeBlockGasUsed: BigInt(tx.evmTx.gasusedblock),
            bitvector: bloom.bitvector,
            logs: logs,
            // @ts-ignore
            status: tx.evmTx.status
        };
        const encodedReceipt = encodeReceipt(receipt, TransactionType.Legacy);
        await receiptsTrie.put(RLP.encode(i), encodedReceipt);
    }
    return {
        gasUsed, gasLimit, size,
        txsRootHash, receiptsTrie, blockBloom
    };
}

export const KEYWORD_STRING_TRIM_SIZE = 32000;
export const RECEIPT_LOG_START = "RCPT{{";
export const RECEIPT_LOG_END = "}}RCPT";

export const stdGasPrice = BigInt(0);
export const stdGasLimit = BigInt(21000);

export class TxDeserializationError {
    info: { [key: string]: string };
    timestamp: string;
    stack: string;
    message: string;

    constructor(
        message: string,
        info: { stack?: any, [key: string]: any }
    ) {
        this.info = info;
        this.stack = info.stack ? info.stack : new Error().stack;
        this.timestamp = moment.utc().format();
        this.message = message;
    }
}

export function isTxDeserializationError(obj: any): obj is TxDeserializationError {
    return (
        obj.info !== undefined &&
        obj.timestamp !== undefined &&
        obj.stack !== undefined &&
        obj.message !== undefined
    );
}

export async function queryAddress(
    accountName: string,
    rpc: JsonRpc,
    logger: Logger
) {
    const acctInt = nameToUint64(accountName);
    let retry = 5;
    let result = null;
    while (retry > 0) {
        retry--;
        try {
            result = await rpc.get_table_rows({
                code: 'eosio.evm',
                scope: 'eosio.evm',
                table: 'account',
                key_type: 'i64',
                index_position: 3,
                lower_bound: acctInt,
                upper_bound: acctInt,
                limit: 1
            });
            if (result.rows.length == 1)
                return result.rows[0].address;

            if (result.rows.length > 1)
                throw new Error('multi address for one account, shouldn\'t happen.');

        } catch (error) {
            logger.error(`queryAddress failed for account ${accountName}, int: ${acctInt}`);
            logger.error(error);
            if (retry == 0)
                throw error;
        }

        logger.warn(`queryAddress returned null for account ${accountName}, int: ${acctInt}, retrying...`);
        await sleep(500);
    }

    throw new Error(`failed to get eth addr for ${accountName}, int: ${acctInt}`);
}
