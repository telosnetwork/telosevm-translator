import {
    EosioEvmRaw,
    StorageEvmTransaction
} from '../types/evm';

const {Signature} = require('eosjs-ecc');

import { removeHexPrefix } from '../utils/evm';
const createKeccakHash = require('keccak');

// ethereum tools
import Bloom from '../utils/evm';
const ethers = require('ethers');
const BN = require('bn.js');

import { parentPort, workerData } from 'worker_threads';

import logger from '../utils/winston';

// const args: {abi: string} = workerData;

logger.info('Launching evm tx deserialization worker...');

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
                logger.info(`Receipt: ${JSON.stringify(receipt)}`);
            } catch (e) {
                logger.warn('WARNING: Failed to parse receiptLog');
                logger.warn(receiptLog);
                return parentPort.postMessage({success: false, message: String(e)});

            }

            if (receipt.block != arg.blockNum)
                throw new Error("Block number mismach");
            
            const evmTx = ethers.utils.parseTransaction(arg.tx.tx);

            if (arg.tx.sender != null) {
                const sig = Signature.fromString(arg.nativeSig);
                evmTx.v = `0x${(27).toString(16).padStart(64, '0')}`;
                evmTx.r = `0x${sig.r.toHex().padStart(64, '0')}`;
                evmTx.s = `0x${sig.s.toHex()}`;

                try {
                    evmTx.from = ethers.utils.getAddress(arg.tx.sender).toLowerCase();

                } catch(error) {
                    logger.info(`error deserializing address \'${arg.tx.sender}\'`);
                    logger.error(error);
                    return parentPort.postMessage({success: false, message: String(error)});
                }

                const flatParams = [
                    evmTx.from,
                    (ethers.BigNumber.from(evmTx.nonce)).toHexString(),
                    evmTx.gasPrice.toHexString(),
                    evmTx.gasLimit.toHexString(),
                    evmTx.value.toHexString(),
                    evmTx.data,
                    evmTx.v,
                    evmTx.r,
                    evmTx.s
                ];

                if (evmTx.to != null)
                    flatParams.unshift(evmTx.to);

                try {

                    const rlpHex = removeHexPrefix(
                        ethers.utils.RLP.encode(flatParams));

                    evmTx.hash = '0x' + createKeccakHash('keccak256')
                        .update(rlpHex)
                        .digest('hex'); 

                } catch (error) {
                    logger.info(JSON.stringify(evmTx,null,4));
                    process.exit(1)
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
                hash: evmTx.hash,
                from: evmTx.from,
                trx_index: receipt.trx_index,
                block: arg.blockNum,
                block_hash: "",
                to: evmTx.to,
                input_data: evmTx.data,
                input_trimmed: evmTx.data.substring(0, KEYWORD_STRING_TRIM_SIZE),
                value: evmTx.value.toHexString(),
                nonce: evmTx.nonce,
                gas_price: evmTx.gasPrice.toHexString(),
                gas_limit: evmTx.gasLimit.toHexString(),
                status: receipt.status,
                itxs: receipt.itxs,
                epoch: receipt.epoch,
                createdaddr: receipt.createdaddr.toLowerCase(),
                gasused: parseInt('0x' + receipt.gasused),
                gasusedblock: parseInt('0x' + receipt.gasusedblock),
                charged_gas_price: parseInt('0x' + receipt.charged_gas),
                output: receipt.output,
            };

            if (receipt.logs) {
                txBody.logs = receipt.logs;
                if (txBody.logs.length === 0) {
                    delete txBody['logs'];
                } else {
                    //console.log('------- LOGS -----------');
                    //console.log(txBody['logs']);
                    const bloom = new Bloom();
                    for (const log of txBody['logs']) {
                        bloom.add(Buffer.from(ethers.utils.getAddress(log['address']), 'hex'));
                        for (const topic of log.topics)
                            bloom.add(Buffer.from(topic.padStart(64, '0'), 'hex'));
                    }

                    txBody['bloom'] = bloom;
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
