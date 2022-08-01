import {
    EosioEvmRaw,
    StorageEvmTransaction
} from '../types/evm';

const {Signature} = require('eosjs-ecc');

// ethereum tools
import Bloom from '../utils/evm';
import { Transaction, TransactionFactory } from '@ethereumjs/tx'
import Common from '@ethereumjs/common'
import { Chain, Hardfork } from '@ethereumjs/common'

const BN = require('bn.js');

import { parentPort, workerData } from 'worker_threads';

import logger from '../utils/winston';
import { isHexPrefixed, isValidAddress } from '@ethereumjs/util';

const args: {chainId: number} = workerData;

logger.info('Launching evm tx deserialization worker...');

const common: Common = Common.custom({
    chainId: args.chainId,
    defaultHardfork: Hardfork.Istanbul
}, {
    baseChain: Chain.Mainnet
});

const KEYWORD_STRING_TRIM_SIZE = 32000;
const RECEIPT_LOG_START = "RCPT{{";
const RECEIPT_LOG_END = "}}RCPT";

parentPort.on(
    'message',
    (param: Array<{
        blockNum: number,
        tx: EosioEvmRaw,
        nativeSig: string,
        consoleLog: string
    }>) => {
        const arg = param[0];
        try {
            let receiptLog = arg.consoleLog.slice(
                arg.consoleLog.indexOf(RECEIPT_LOG_START) + RECEIPT_LOG_START.length,
                arg.consoleLog.indexOf(RECEIPT_LOG_END)
            );

            let receipt;
            try {
                receipt = JSON.parse(receiptLog);
                // logger.info(`Receipt: ${JSON.stringify(receipt)}`);
            } catch (e) {
                logger.warn('WARNING: Failed to parse receiptLog');
                logger.warn(receiptLog);
                return parentPort.postMessage({success: false, message: String(e)});

            }

            if (receipt.block != arg.blockNum)
                throw new Error("Block number mismach");

            const txRaw = Buffer.from(arg.tx.tx, 'hex');
           
            let evmTx = Transaction.fromSerializedTx(
                txRaw, {common});

            const evmTxParams = evmTx.toJSON();
            let fromAddr = null;

            if (arg.tx.sender != null) {
                const sig = Signature.fromString(arg.nativeSig);
                evmTxParams.v = `0x${(27).toString(16).padStart(64, '0')}`;
                evmTxParams.r = `0x${sig.r.toHex().padStart(64, '0')}`;
                evmTxParams.s = `0x${sig.s.toHex()}`;

                evmTx = Transaction.fromTxData(evmTxParams, {common});

                let senderAddr = arg.tx.sender.toLowerCase();

                if (!isHexPrefixed(senderAddr))
                    senderAddr = `0x${senderAddr}`;

                if(isValidAddress(senderAddr)) {
                    fromAddr = senderAddr;

                } else {
                    logger.error(`error deserializing address \'${arg.tx.sender}\'`);
                    return parentPort.postMessage({success: false, message: 'invalid address'});
                }
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

            const txBody: StorageEvmTransaction = {
                hash: evmTx.hash().toString('hex'),
                trx_index: receipt.trx_index,
                block: arg.blockNum,
                block_hash: "",
                to: evmTx.to?.toString(),
                input_data: evmTx.data?.toString('hex'),
                input_trimmed: evmTx.data?.toString('hex').substring(0, KEYWORD_STRING_TRIM_SIZE),
                value: evmTx.value?.toString('hex'),
                nonce: evmTx.nonce?.toString('hex'),
                gas_price: evmTx.gasPrice?.toString('hex'),
                gas_limit: evmTx.gasLimit?.toString('hex'),
                status: receipt.status,
                itxs: receipt.itxs,
                epoch: receipt.epoch,
                createdaddr: receipt.createdaddr.toLowerCase(),
                gasused: parseInt('0x' + receipt.gasused),
                gasusedblock: parseInt('0x' + receipt.gasusedblock),
                charged_gas_price: parseInt('0x' + receipt.charged_gas),
                output: receipt.output,
                raw: txRaw 
            };

            if (fromAddr != null)
                txBody['from'] = fromAddr;

            if (receipt.logs) {
                txBody.logs = receipt.logs;
                if (txBody.logs.length === 0) {
                    delete txBody['logs'];
                } else {
                    //console.log('------- LOGS -----------');
                    //console.log(txBody['logs']);
                    const bloom = new Bloom();
                    for (const log of txBody['logs']) {
                        bloom.add(Buffer.from(log['address'], 'hex'));
                        for (const topic of log.topics)
                            bloom.add(Buffer.from(topic.padStart(64, '0'), 'hex'));
                    }

                    txBody['logsBloom'] = bloom.bitvector.toString('hex');
                }
            }

            if (receipt.errors) {
                txBody['errors'] = receipt.errors;
                if (txBody['errors'].length === 0) {
                    delete txBody['errors'];
                } else {
                    //console.log('------- ERRORS -----------');
                    //console.log(txBody['errors'])
                }
            }

            if (txBody.value) {  // divide values by 18 decimal places to bring them to telosland
                txBody['value_d'] = new BN(evmTx.value.toString()) / new BN('1000000000000000000');
            }

            return parentPort.postMessage({success: true, tx: txBody});
        } catch (e) {
            return parentPort.postMessage({success: false, message: String(e)});
        }
    }
);
