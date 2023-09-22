import {
    EosioEvmRaw,
    StorageEvmTransaction
} from '../types/evm.js';

// ethereum tools
import {Bloom, generateUniqueVRS} from '../utils/evm.js';
import {TEVMTransaction} from '../utils/evm-tx.js';
import Common from '@ethereumjs/common'
import { Chain, Hardfork } from '@ethereumjs/common'

import BN from 'bn.js';

import { parentPort, workerData } from 'worker_threads';

import logger from '../utils/winston.js';
import { addHexPrefix, isHexPrefixed, isValidAddress, unpadHexString } from '@ethereumjs/util';

const args: {chainId: number} = workerData;

logger.info('Launching evm tx deserialization worker...');

const common: Common.default = Common.default.custom({
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
        nativeBlockHash: string,
        trx_index: number,
        blockNum: number,
        tx: EosioEvmRaw,
        consoleLog: string,
        gasUsedBlock: string
    }>) => {
        const arg = param[0];
        try {
            let receiptLog = arg.consoleLog.slice(
                arg.consoleLog.indexOf(RECEIPT_LOG_START) + RECEIPT_LOG_START.length,
                arg.consoleLog.indexOf(RECEIPT_LOG_END)
            );

            let receipt: {
                status: number,
                epoch: number,
                itxs: any[],
                logs: any[],
                errors?: any[],
                output: string,
                gasused: string,
                gasusedblock: string,
                charged_gas: string,
                createdaddr: string
            } = {
                status: 1,
                epoch: 0,
                itxs: [],
                logs: [],
                errors: [],
                output: '',
                gasused: '',
                gasusedblock: '',
                charged_gas: '',
                createdaddr: ''
            };
            try {
                receipt = JSON.parse(receiptLog);
            } catch (error) {
                logger.error(`Failed to parse receiptLog:\n${receiptLog}`);
                logger.error(JSON.stringify(error, null, 4));
                // @ts-ignore
                logger.error(error.message);
                // @ts-ignore
                logger.error(error.stack);
                process.exit(1);
            }

            const txRaw = Buffer.from(arg.tx.tx, "hex");

            let evmTx = TEVMTransaction.fromSerializedTx(
                txRaw, {common});

            const isSigned = evmTx.isSigned();

            const evmTxParams = evmTx.toJSON();

            let fromAddr = null;
            let v, r, s;

            if (!isSigned) {

                let senderAddr = arg.tx.sender.toLowerCase();

                if (!isHexPrefixed(senderAddr))
                    senderAddr = `0x${senderAddr}`;

                [v, r, s] = generateUniqueVRS(
                    arg.nativeBlockHash, senderAddr, arg.trx_index);

                evmTxParams.v = addHexPrefix(v.toString(16));
                evmTxParams.r = r;
                evmTxParams.s = s;

                evmTx = TEVMTransaction.fromTxData(evmTxParams, {common});

                if(isValidAddress(senderAddr)) {
                    fromAddr = senderAddr;

                } else {
                    logger.error(`error deserializing address \'${arg.tx.sender}\'`);
                    return parentPort.postMessage({success: false, message: 'invalid address'});
                }
            } else
                [v, r, s] = [
                    evmTx.v.toNumber(),
                    unpadHexString(addHexPrefix(evmTx.r.toString('hex'))),
                    unpadHexString(addHexPrefix(evmTx.s.toString('hex')))
                ];

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
                hash: '0x'+ evmTx.hash().toString('hex'),
                trx_index: arg.trx_index,
                block: arg.blockNum,
                block_hash: "",
                to: evmTx.to?.toString(),
                input_data: '0x' + evmTx.data?.toString('hex'),
                input_trimmed: '0x' + evmTx.data?.toString('hex').substring(0, KEYWORD_STRING_TRIM_SIZE),
                value: evmTx.value?.toString('hex'),
                value_d: (new BN(evmTx.value?.toString()).div(new BN('1000000000000000000'))).toString(),
                nonce: evmTx.nonce?.toString(),
                gas_price: evmTx.gasPrice?.toString(),
                gas_limit: evmTx.gasLimit?.toString(),
                status: receipt.status,
                itxs: receipt.itxs,
                epoch: receipt.epoch,
                createdaddr: receipt.createdaddr.toLowerCase(),
                gasused: new BN(receipt.gasused, 16).toString(),
                gasusedblock: new BN(arg.gasUsedBlock).add(new BN(receipt.gasused, 16)).toString(),
                charged_gas_price: new BN(receipt.charged_gas, 16).toString(),
                output: receipt.output,
                raw: evmTx.serialize(),
                v: v,
                r: r,
                s: s
            };

            if (!isSigned)
                txBody['from'] = fromAddr;

            else
                txBody['from'] = evmTx.getSenderAddress().toString().toLowerCase();

            if (receipt.logs) {
                txBody.logs = receipt.logs;
                if (txBody.logs.length === 0) {
                    delete txBody['logs'];
                } else {
                    //console.log('------- LOGS -----------');
                    //console.log(txBody['logs']);
                    const bloom = new Bloom();
                    for (const log of txBody['logs']) {
                        bloom.add(Buffer.from(log['address'].padStart(40, '0'), 'hex'));
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

            return parentPort.postMessage({success: true, tx: txBody});
        } catch (e) {
            return parentPort.postMessage({success: false, message: {
                'stack': e.stack,
                'name': e.name,
                'message': e.message
            }});
        }
    }
);
