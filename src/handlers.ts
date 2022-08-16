import {
    EosioEvmRaw,
    EosioEvmDeposit,
    EosioEvmWithdraw,
    StorageEvmTransaction
} from './types/evm';

import {TEVMTransaction} from './utils/evm';

import {nameToUint64, parseAsset} from './utils/eosio';
import logger from './utils/winston';


// ethereum tools
var Units = require('ethereumjs-units');
const BN = require('bn.js');
import Common from '@ethereumjs/common'
import { Chain, Hardfork } from '@ethereumjs/common'
import {JsonRpc} from 'eosjs';
import {StaticPool} from 'node-worker-threads-pool';
import {isValidAddress} from '@ethereumjs/util';

import {generateUniqueVRS} from './utils/evm';


const KEYWORD_STRING_TRIM_SIZE = 32000;

let common: Common = null;
let deseralizationPool: StaticPool<(x: any) => any> = null;

class TxDeserializationError extends Error {
    info: any;

    constructor(public message: string, info: any) {
        super(message);
        this.name = "TxDeserializationError";
        this.stack = (<any> new Error()).stack;
        this.info = info;
    }
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
) : Promise<StorageEvmTransaction> {
    const result = await deseralizationPool.exec([{
        nativeBlockHash, trx_index, blockNum, tx, consoleLog
    }]);

    if (result.success)
        return result.tx;
    else {
        logger.error(result.message['stack']);
        throw new TxDeserializationError('EVM worker crashed.', result.message);
    }
}

const stdGasPrice = "0x7a307efa80";
const stdGasLimit = `0x${(21000).toString(16)}`;

async function queryAddress(accountName: string, rpc: JsonRpc) {
    const acctInt = nameToUint64(accountName)
    const result = await rpc.get_table_rows({
        code: 'eosio.evm',
        scope: 'eosio.evm',
        table: 'account',
        key_type: 'i64',
        index_position: 3,
        lower_bound: acctInt,
        upper_bound: acctInt,
        limit: 1
    });

    if (result.rows.length == 1) {
        return result.rows[0].address;
    } else if (result.rows.length > 1) {
        throw new Error("multiple address for one account? shouldn\'t happen");
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
) : Promise<StorageEvmTransaction> {
    const quantity = parseAsset(tx.quantity);

    let toAddr = null;
    if (!tx.memo.startsWith('0x')) {
        const address = await queryAddress(tx.from, rpc);

        if (address) {
            toAddr = `0x${address}`;
        } else {
            logger.error('seems user deposited without registering!');
            return null;
        }
    } else {
        if(isValidAddress(tx.memo))
            toAddr = tx.memo;

        else {
            const address = await queryAddress(tx.from, rpc);

            if (!address) {
                logger.error(JSON.stringify(tx));
                logger.error('seems user deposited to an invalid address!');
                return null;
            }

            toAddr = `0x${address}`;
        }
    }

    const [v, r, s] = generateUniqueVRS(nativeBlockHash, trx_index);

    const txParams = {
        from: "0x0000000000000000000000000000000000000000",
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
    const evmTx = TEVMTransaction.fromTxData(txParams);

    const inputData = '0x' + evmTx.data?.toString('hex');
    const txBody: StorageEvmTransaction = {
        hash: '0x' + evmTx.hash()?.toString('hex'),
        from: "0x0000000000000000000000000000000000000000",
        trx_index: trx_index,
        block: blockNum,
        block_hash: "",
        to: evmTx.to?.toString(),
        input_data: inputData,
        input_trimmed: inputData.substring(0, KEYWORD_STRING_TRIM_SIZE),
        value: '0x' + evmTx.value?.toString(16),
        value_d: new BN(evmTx.value?.toString()) / new BN('1000000000000000000'),
        nonce: evmTx.nonce?.toString(),
        gas_price: evmTx.gasPrice?.toString(),
        gas_limit: evmTx.gasLimit?.toString(),
        status: 1,
        itxs: new Array(),
        epoch: 0,
        createdaddr: "",
        gasused: 0,
        gasusedblock: gasUsedBlock,
        charged_gas_price: 0,
        output: "",
        raw: evmTx.serialize()
    };

    return txBody;
}

export async function handleEvmWithdraw(
    nativeBlockHash: string,
    trx_index: number,
    blockNum: number,
    tx: EosioEvmWithdraw,
    rpc: JsonRpc,
    gasUsedBlock: number
) : Promise<StorageEvmTransaction> {
    const address = await queryAddress(tx.to, rpc);

    const quantity = parseAsset(tx.quantity);

    const [v, r, s] = generateUniqueVRS(nativeBlockHash, trx_index);
    const txParams = {
        from: address.toLowerCase(), 
        nonce: 0,
        gasPrice: stdGasPrice,
        gasLimit: stdGasLimit,
        to: "0x0000000000000000000000000000000000000000",
        value: (new BN(quantity.amount)).mul(new BN('100000000000000')),
        data: "0x",
        v: v,
        r: r,
        s: s 
    };
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
        value: '0x' + evmTx.value?.toString(16),
        value_d: new BN(evmTx.value?.toString()) / new BN('1000000000000000000'),
        nonce: evmTx.nonce?.toString(),
        gas_price: evmTx.gasPrice?.toString(),
        gas_limit: evmTx.gasLimit?.toString(),
        status: 1,
        itxs: new Array(),
        epoch: 0,
        createdaddr: "",
        gasused: 0,
        gasusedblock: gasUsedBlock,
        charged_gas_price: 0,
        output: "",
        raw: evmTx.serialize()
    };

    return txBody;
}
