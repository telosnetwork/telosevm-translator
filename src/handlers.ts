import {
    EosioEvmRaw,
    EosioEvmDeposit,
    EosioEvmWithdraw,
    StorageEvmTransaction
} from './types/evm';

import {TEVMTransaction} from './utils/evm-tx';

import { removeHexPrefix } from './utils/evm';

import {nameToUint64, parseAsset} from './utils/eosio';

import logger from './utils/winston';

// ethereum tools
const BN = require('bn.js');
import Common from '@ethereumjs/common'
import { Chain, Hardfork } from '@ethereumjs/common'
import {JsonRpc} from 'eosjs';
import {StaticPool} from 'node-worker-threads-pool';
import {isValidAddress} from '@ethereumjs/util';

import {generateUniqueVRS, ZERO_ADDR} from './utils/evm';
import moment from 'moment';

const sleep = (ms: number) => new Promise( res => setTimeout(res, ms));


const KEYWORD_STRING_TRIM_SIZE = 32000;

let common: Common = null;
let deseralizationPool: StaticPool<(x: any) => any> = null;

export class TxDeserializationError {
    info: {[key: string]: string};
    timestamp: string;
    stack: string;
    message: string;

    constructor(
        message: string,
        info: {[key: string]: any}
    ) {
        this.info = info;
        this.stack = (<any> new Error()).stack;
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

export function setCommon(chainId: number) {
    common = Common.custom({
        chainId: chainId,
        defaultHardfork: Hardfork.Istanbul
    }, {
        baseChain: Chain.Mainnet
    });
    deseralizationPool = new StaticPool({
        size: 8,
        task: './build/workers/evm.js',
        workerData: {
            chainId: chainId
        }
    });
}

export async function handleEvmTx(
    nativeBlockHash: string,
    trx_index: number,
    blockNum: number,
    tx: EosioEvmRaw,
    consoleLog: string
) : Promise<StorageEvmTransaction | TxDeserializationError> {
    const result = await deseralizationPool.exec([{
        nativeBlockHash, trx_index, blockNum, tx, consoleLog
    }]);

    if (result.success)
        return result.tx;
    else
        return new TxDeserializationError(
            'Raw EVM deserialization error',
            {
                'nativeBlockHash': nativeBlockHash,
                'tx': tx,
                'block_num': blockNum,
                'error': result.message
            }
        );
}

const stdGasPrice = "0x0";
const stdGasLimit = `0x${(21000).toString(16)}`;

async function queryAddress(accountName: string, rpc: JsonRpc) {
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

        } catch (error) {
            logger.warn(`queryAddress failed for account ${accountName}, int: ${acctInt}`);
            logger.warn(error);
            if (retry > 0) {
                await sleep(1000);
                continue;
            } else
                throw error;
        }
    }

    if (result.rows.length == 1) {
        return result.rows[0].address;
    } else if (result.rows.length > 1) {
        throw new Error('multi address for one account, shouldn\'t happen.');
    } else {
        return null;
    }
}

export async function handleEvmDeposit(
    nativeBlockHash: string,
    trx_index: number,
    blockNum: number,
    tx: EosioEvmDeposit,
    rpc: JsonRpc,
    gasUsedBlock: number
) : Promise<StorageEvmTransaction | TxDeserializationError> {
    const quantity = parseAsset(tx.quantity);

    let toAddr = null;
    if (!tx.memo.startsWith('0x')) {
        const address = await queryAddress(tx.from, rpc);

        if (address) {
            toAddr = `0x${address}`;
        } else {
            return new TxDeserializationError(
                "User deposited without registering",
                {
                    'nativeBlockHash': nativeBlockHash,
                    'tx': tx,
                    'block_num': blockNum
                });
        }
    } else {
        if(isValidAddress(tx.memo))
            toAddr = tx.memo;

        else {
            const address = await queryAddress(tx.from, rpc);

            if (!address) {
                return new TxDeserializationError(
                    "User deposited to an invalid address",
                    {
                        'nativeBlockHash': nativeBlockHash,
                        'tx': tx,
                        'block_num': blockNum
                    });
            }

            toAddr = `0x${address}`;
        }
    }

    const [v, r, s] = generateUniqueVRS(
        nativeBlockHash, ZERO_ADDR, trx_index);

    const txParams = {
        nonce: 0,
        gasPrice: stdGasPrice,
        gasLimit: stdGasLimit,
        to: toAddr,
        value: (new BN(quantity.amount)).mul(new BN('100000000000000')),
        data: "0x",
        v: v,
        r: r,
        s: s
    };

    try {
        const evmTx = TEVMTransaction.fromTxData(
            txParams, {common: common});

        const inputData = '0x' + evmTx.data?.toString('hex');
        const txBody: StorageEvmTransaction = {
            hash: '0x' + evmTx.hash()?.toString('hex'),
            from: ZERO_ADDR,
            trx_index: trx_index,
            block: blockNum,
            block_hash: "",
            to: evmTx.to?.toString(),
            input_data: inputData,
            input_trimmed: inputData.substring(0, KEYWORD_STRING_TRIM_SIZE),
            value: evmTx.value?.toString(16),
            value_d: new BN(evmTx.value?.toString()) / new BN('1000000000000000000'),
            nonce: evmTx.nonce?.toString(),
            gas_price: evmTx.gasPrice?.toString(),
            gas_limit: evmTx.gasLimit.toString(),
            status: 1,
            itxs: new Array(),
            epoch: 0,
            createdaddr: "",
            gasused: new BN(removeHexPrefix(stdGasLimit), 16),
            gasusedblock: new BN(gasUsedBlock),
            charged_gas_price: 0,
            output: "",
            raw: evmTx.serialize(),
            v: v,
            r: r,
            s: s
        };

        return txBody;

    } catch (error) {
        return new TxDeserializationError(
            error.message,
            {
                'nativeBlockHash': nativeBlockHash,
                'tx': tx,
                'block_num': blockNum
            });
    }
}

export async function handleEvmWithdraw(
    nativeBlockHash: string,
    trx_index: number,
    blockNum: number,
    tx: EosioEvmWithdraw,
    rpc: JsonRpc,
    gasUsedBlock: number
) : Promise<StorageEvmTransaction | TxDeserializationError> {
    const address = await queryAddress(tx.to, rpc);

    const quantity = parseAsset(tx.quantity);

    const [v, r, s] = generateUniqueVRS(
        nativeBlockHash, address, trx_index);

    const txParams = {
        nonce: 0,
        gasPrice: stdGasPrice,
        gasLimit: stdGasLimit,
        to: ZERO_ADDR,
        value: (new BN(quantity.amount)).mul(new BN('100000000000000')),
        data: "0x",
        v: v,
        r: r,
        s: s
    };
    try {
        const evmTx = new TEVMTransaction(txParams, {common: common});

        const inputData = '0x' + evmTx.data?.toString('hex');
        const txBody: StorageEvmTransaction = {
            hash: '0x' + evmTx.hash()?.toString('hex'),
            from: '0x' + address.toLowerCase(), 
            trx_index: trx_index,
            block: blockNum,
            block_hash: "",
            to: evmTx.to?.toString(),
            input_data: inputData,
            input_trimmed: inputData.substring(0, KEYWORD_STRING_TRIM_SIZE),
            value: evmTx.value?.toString(16),
            value_d: new BN(evmTx.value?.toString()) / new BN('1000000000000000000'),
            nonce: evmTx.nonce?.toString(),
            gas_price: evmTx.gasPrice?.toString(),
            gas_limit: evmTx.gasLimit?.toString(),
            status: 1,
            itxs: new Array(),
            epoch: 0,
            createdaddr: "",
            gasused: new BN(removeHexPrefix(stdGasLimit), 16),
            gasusedblock: new BN(gasUsedBlock),
            charged_gas_price: 0,
            output: "",
            raw: evmTx.serialize(),
            v: v,
            r: r,
            s: s
        };

        return txBody;

    } catch (error) {
        return new TxDeserializationError(
            error.message,
            {
                'nativeBlockHash': nativeBlockHash,
                'tx': tx,
                'block_num': blockNum
            });
    }
}
