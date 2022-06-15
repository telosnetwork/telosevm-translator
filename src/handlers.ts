import {
    EosioEvmRaw,
    EosioEvmDeposit,
    EosioEvmWithdraw,
    EvmTransaction
} from './types/evm';

import { parseAsset, getRPCClient } from './utils/eosio';
import logger from './utils/winston';

const {Signature} = require('eosjs-ecc');

// ethereum tools
var Units = require('ethereumjs-units');
const ethers = require('ethers')
const BN = require('bn.js');
import {Transaction} from '@ethereumjs/tx';
import {default as ethCommon} from '@ethereumjs/common';
import {JsonRpc} from 'eosjs';

const KEYWORD_STRING_TRIM_SIZE = 32000;
const RECEIPT_LOG_START = "RCPT{{";
const RECEIPT_LOG_END = "}}RCPT";


const common = ethCommon.forCustomChain(
    "mainnet",
    {chainId: 40},
    "istanbul" 
);


const ethTxKeys = [
    "nonce",
    "gasPrice",
    "gasLimit",
    "to",
    "value",
    "data",
    "v",
    "r",
    "s"
];
export async function handleEvmTx(
    blockNum: number,
    tx: EosioEvmRaw,
    nativeSig: string,
    consoleLog: string
) {
    
    let receiptLog = consoleLog.slice(
        consoleLog.indexOf(RECEIPT_LOG_START) + RECEIPT_LOG_START.length,
        consoleLog.indexOf(RECEIPT_LOG_END)
    );

    let receipt;
    try {
        receipt = JSON.parse(receiptLog);
        logger.debug(`Receipt: ${JSON.stringify(receipt)}`);
    } catch (e) {
        logger.warning('WARNING: Failed to parse receiptLog');
        return null;
    }

    if (receipt.block != blockNum)
        throw new Error("Block number mismach");

    const rawTxHex = Buffer.from(tx.tx, 'hex');
    const decoded = ethers.utils.RLP.decode(rawTxHex)

    let evmTxData: {[key: string]: any} = {};

    for (const [i, key] of ethTxKeys.entries())
        evmTxData[key] = decoded[i];

    const evmTx = new Transaction(evmTxData, {common: common}); 

    let v, r, s: string;

    if (evmTx.v == new BN('0')) {
        const sig = Signature.fromString(nativeSig);
        v = `0x${(27).toString(16).padStart(64, '0')}`;
        r = `0x${sig.r.toHex().padStart(64, '0')}`;
        s = `0x${sig.s.toHex()}`;
    }

    if (receipt.itxs) {
        // @ts-ignore
        receipt.itxs.forEach((itx) => {
            if (itx.input)
                itx.input_trimmed = itx.input.substring(0, KEYWORD_STRING_TRIM_SIZE);
            else
                itx.input_trimmed = itx.input;
        });
    }

    const inputData = '0x' + evmTx.data?.toString('hex');
    const txBody = {
        hash: '0x' + evmTx.hash()?.toString('hex'),
        trx_index: receipt.trx_index,
        block: blockNum,
        block_hash: "",
        to: evmTx.to?.toString(),
        input_data: inputData,
        input_trimmed: inputData.substring(0, KEYWORD_STRING_TRIM_SIZE),
        value: evmTx.value?.toString(),
        nonce: evmTx.nonce?.toString(),
        gas_price: evmTx.gasPrice?.toString(),
        gas_limit: evmTx.gasLimit?.toString(),
        status: receipt.status,
        itxs: receipt.itxs,
        epoch: receipt.epoch,
        createdaddr: receipt.createdaddr.toLowerCase(),
        gasused: parseInt('0x' + receipt.gasused),
        gasusedblock: parseInt('0x' + receipt.gasusedblock),
        charged_gas_price: parseInt('0x' + receipt.charged_gas),
        output: receipt.output,
    };

    return txBody;
}

const stdGasPrice = "0x7a307efa80";
const stdGasLimit = `0x${(21000).toString(16)}`;

async function queryAddress(accountName: string, rpc: JsonRpc) {
    const result = await rpc.get_table_rows({
        code: 'eosio.evm',
        table: 'account',
        scope: 'eosio.evm',
        index_position: '3',
        key_type: 'name',
        upper_bound: accountName,
        lower_bound: accountName
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
    blockNum: number,
    tx: EosioEvmDeposit,
    nativeSig: string,
    rpc: JsonRpc
) {
    const quantity = parseAsset(tx.quantity);
    const quantWei = Units.convert(quantity.amount, 'eth', 'wei');

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
        try {
            toAddr = ethers.utils.getAddress(tx.memo);

        } catch (error) {
            const address = await queryAddress(tx.from, rpc);

            if (!address) {
                logger.error(JSON.stringify(tx));
                logger.error('seems user deposited to an invalid address!');
                return null;
            }

            toAddr = `0x${address}`;
        }
    }

    const sig = Signature.fromString(nativeSig);
    const txParams = {
        nonce: 0,
        gasPrice: stdGasPrice,
        gasLimit: stdGasLimit,
        to: toAddr,
        value: `0x${new BN(quantWei, 16)._strip()}`,
        data: "0x",
        v: `0x${(27).toString(16).padStart(64, '0')}`,
        r: `0x${sig.r.toHex().padStart(64, '0')}`,
        s: `0x${sig.s.toHex()}`
    };
    const evmTx = new Transaction(txParams, {common: common});

    const inputData = '0x' + evmTx.data?.toString('hex');
    const txBody = {
        hash: '0x' + evmTx.hash()?.toString('hex'),
        trx_index: 0,
        block: blockNum,
        block_hash: "",
        to: evmTx.to?.toString(),
        input_data: inputData,
        input_trimmed: inputData.substring(0, KEYWORD_STRING_TRIM_SIZE),
        value: evmTx.value?.toString(),
        nonce: evmTx.nonce?.toString(),
        gas_price: evmTx.gasPrice?.toString(),
        gas_limit: evmTx.gasLimit?.toString(),
        status: "",
        itxs: new Array(),
        epoch: 0,
        createdaddr: 0,
        gasused: 0,
        gasusedblock: 0,
        charged_gas_price: 0,
        output: "",
    };

    return txBody;
}

export async function handleEvmWithdraw(
    blockNum: number,
    tx: EosioEvmWithdraw,
    nativeSig: string
) {
    const quantity = parseAsset(tx.quantity);
    const quantWei = Units.convert(quantity.amount, 'eth', 'wei');

    const sig = Signature.fromString(nativeSig);
    const txParams = {
        nonce: 0,
        gasPrice: stdGasPrice,
        gasLimit: stdGasLimit,
        to: "0x0000000000000000000000000000000000000000",
        value: `0x${new BN(quantWei, 16)._strip()}`,
        data: "0x",
        v: `0x${(27).toString(16).padStart(64, '0')}`,
        r: `0x${sig.r.toHex().padStart(64, '0')}`,
        s: `0x${sig.s.toHex()}`
    };
    const evmTx = new Transaction(txParams, {common: common});

    const inputData = '0x' + evmTx.data?.toString('hex');
    const txBody = {
        hash: '0x' + evmTx.hash()?.toString('hex'),
        trx_index: 0,
        block: blockNum,
        block_hash: "",
        to: evmTx.to?.toString(),
        input_data: inputData,
        input_trimmed: inputData.substring(0, KEYWORD_STRING_TRIM_SIZE),
        value: evmTx.value?.toString(),
        nonce: evmTx.nonce?.toString(),
        gas_price: evmTx.gasPrice?.toString(),
        gas_limit: evmTx.gasLimit?.toString(),
        status: "",
        itxs: new Array(),
        epoch: 0,
        createdaddr: 0,
        gasused: 0,
        gasusedblock: 0,
        charged_gas_price: 0,
        output: "",
    };

    return txBody;
}
