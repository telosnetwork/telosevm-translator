import workerpool from 'workerpool';
import {EosioEvmDeposit, EosioEvmRaw, EosioEvmWithdraw, StorageEvmTransaction} from "../types/evm.js";
import {
    arrayToHex,
    generateUniqueVRS, hexStringToUint8Array, KEYWORD_STRING_TRIM_SIZE,
    queryAddress, RECEIPT_LOG_END, RECEIPT_LOG_START,
    stdGasLimit,
    stdGasPrice,
    TxDeserializationError,
    ZERO_ADDR
} from "../utils/evm.js";
import {getRPCClient, parseAsset} from "../utils/eosio.js";
import {createLogger, format, transports} from "winston";
import {addHexPrefix, bigIntToHex, isHexPrefixed, isValidAddress, unpadHex} from "@ethereumjs/util";
import * as evm from "@ethereumjs/common";
import {Bloom} from "@ethereumjs/vm";
import {TEVMTransaction} from "telos-evm-custom-ds";

const common = evm.Common.custom({
    chainId: parseInt(process.env.CHAIN_ID, 10),
    defaultHardfork: evm.Hardfork.Istanbul
}, {baseChain: evm.Chain.Mainnet});

const rpc = getRPCClient(process.env.ENDPOINT);
const logOptions = {
    exitOnError: false,
    level: process.env.LOG_LEVEL,
    format: format.combine(
        format.metadata(),
        format.colorize(),
        format.timestamp(),
        format.printf((info: any) => {
            return `${info.timestamp} [PID:${process.pid}] [${info.level}] : ${info.message} ${Object.keys(info.metadata).length > 0 ? JSON.stringify(info.metadata) : ''}`;
        })
    )
}
const logger = createLogger(logOptions);
logger.add(new transports.Console({
    level: process.env.LOG_LEVEL
}));

export interface HandlerArguments {
    nativeBlockHash: string;
    trx_index: number;
    blockNum: number;
    tx: EosioEvmRaw | EosioEvmDeposit | EosioEvmWithdraw;
    consoleLog?: string;
}

async function createEvm(args: HandlerArguments): Promise<StorageEvmTransaction | TxDeserializationError> {
    const tx = args.tx as EosioEvmRaw;
    if (!args.consoleLog || args.consoleLog.length == 0) {
        return new TxDeserializationError(
            `consoleLog undefined or empty string: ${args}`,
            {}
        );
    }

    try {
        let receiptLog = args.consoleLog.slice(
            args.consoleLog.indexOf(RECEIPT_LOG_START) + RECEIPT_LOG_START.length,
            args.consoleLog.indexOf(RECEIPT_LOG_END)
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
            return new TxDeserializationError(
                'Raw EVM deserialization error',
                {
                    'nativeBlockHash': args.nativeBlockHash,
                    'tx': tx,
                    'block_num': args.blockNum,
                    'error': error.message,
                    'stack': error.stack
                }
            );
        }

        const txRaw = hexStringToUint8Array(tx.tx);

        let evmTx = TEVMTransaction.fromSerializedTx(txRaw, {common});

        const isSigned = evmTx.isSigned();

        const evmTxParams = evmTx.toJSON();
        evmTxParams.value = unpadHex(evmTxParams.value);

        let fromAddr = null;
        let v, r, s;

        if (!isSigned) {

            let senderAddr = tx.sender.toLowerCase();

            if (!isHexPrefixed(senderAddr))
                senderAddr = `0x${senderAddr}`;

            [v, r, s] = generateUniqueVRS(
                args.nativeBlockHash, senderAddr, args.trx_index);

            evmTxParams.v = v;
            evmTxParams.r = r;
            evmTxParams.s = s;

            evmTx = TEVMTransaction.fromTxData(evmTxParams, {common});

            if (isValidAddress(senderAddr)) {
                fromAddr = senderAddr;

            } else {
                return new TxDeserializationError(
                    'Raw EVM deserialization error',
                    {
                        'nativeBlockHash': args.nativeBlockHash,
                        'tx': tx,
                        'block_num': args.blockNum,
                        'error': `error deserializing address \'${tx.sender}\'`
                    }
                );
            }
        } else
            [v, r, s] = [
                evmTx.v, evmTx.r, evmTx.s
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

        const inputData = arrayToHex(evmTx.data);

        const txBody: StorageEvmTransaction = {
            hash: '0x' + arrayToHex(evmTx.hash()),
            trx_index: args.trx_index,
            block: args.blockNum,
            block_hash: "",
            to: evmTx.to?.toString(),
            input_data: '0x' + inputData,
            input_trimmed: '0x' + inputData.substring(0, KEYWORD_STRING_TRIM_SIZE),
            value: evmTx.value?.toString(16),
            value_d: (evmTx.value / BigInt('1000000000000000000')).toString(),
            nonce: evmTx.nonce?.toString(),
            gas_price: evmTx.gasPrice?.toString(),
            gas_limit: evmTx.gasLimit?.toString(),
            status: receipt.status,
            itxs: receipt.itxs,
            epoch: receipt.epoch,
            createdaddr: receipt.createdaddr.toLowerCase(),
            gasused: BigInt(addHexPrefix(receipt.gasused)).toString(),
            gasusedblock: '',
            charged_gas_price: BigInt(addHexPrefix(receipt.charged_gas)).toString(),
            output: receipt.output,
            raw: Buffer.from(evmTx.serialize()).toString('base64'),
            v: v.toString(),
            r: unpadHex(bigIntToHex(r)),
            s: unpadHex(bigIntToHex(s))
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
                    bloom.add(hexStringToUint8Array(log['address'].padStart(40, '0')));
                    for (const topic of log.topics)
                        bloom.add(hexStringToUint8Array(topic.padStart(64, '0')));
                }

                txBody['logsBloom'] = arrayToHex(bloom.bitvector);
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

        return txBody;
    } catch (error) {
        return new TxDeserializationError(
            'Raw EVM deserialization error',
            {
                'nativeBlockHash': args.nativeBlockHash,
                'tx': tx,
                'block_num': args.blockNum,
                'error': error.message,
                'stack': error.stack
            }
        );
    }
}

async function createDeposit(args: HandlerArguments): Promise<StorageEvmTransaction | TxDeserializationError> {
    const tx = args.tx as EosioEvmDeposit;
    const quantity = parseAsset(tx.quantity);

    let toAddr = null;
    if (!tx.memo.startsWith('0x')) {
        const address = await queryAddress(tx.from, rpc, logger);

        if (address) {
            toAddr = `0x${address}`;
        } else {
            return new TxDeserializationError(
                "User deposited without registering",
                {
                    'nativeBlockHash': args.nativeBlockHash,
                    'tx': tx,
                    'block_num': args.blockNum
                });
        }
    } else {
        if (isValidAddress(tx.memo))
            toAddr = tx.memo;

        else {
            const address = await queryAddress(tx.from, rpc, logger);

            if (!address) {
                return new TxDeserializationError(
                    "User deposited to an invalid address",
                    {
                        'nativeBlockHash': args.nativeBlockHash,
                        'tx': tx,
                        'block_num': args.blockNum
                    });
            }

            toAddr = `0x${address}`;
        }
    }

    const [v, r, s] = generateUniqueVRS(
        args.nativeBlockHash, ZERO_ADDR, args.trx_index);

    const txParams = {
        nonce: 0,
        gasPrice: stdGasPrice,
        gasLimit: stdGasLimit,
        to: toAddr,
        value: BigInt(quantity.amount) * BigInt('100000000000000'),
        data: "0x",
        v: v,
        r: r,
        s: s
    };

    try {
        const evmTx = TEVMTransaction.fromTxData(txParams, {common});
        const inputData = '0x' + arrayToHex(evmTx.data);
        return {
            hash: '0x' + arrayToHex(evmTx.hash()),
            from: ZERO_ADDR,
            trx_index: args.trx_index,
            block: args.blockNum,
            block_hash: "",
            to: evmTx.to?.toString(),
            input_data: inputData,
            input_trimmed: inputData.substring(0, KEYWORD_STRING_TRIM_SIZE),
            value: evmTx.value?.toString(16),
            value_d: tx.quantity,
            nonce: evmTx.nonce?.toString(),
            gas_price: evmTx.gasPrice?.toString(),
            gas_limit: evmTx.gasLimit.toString(),
            status: 1,
            itxs: [],
            epoch: 0,
            createdaddr: "",
            gasused: stdGasLimit.toString(),
            gasusedblock: '',
            charged_gas_price: '0',
            output: "",
            raw: Buffer.from(evmTx.serialize()).toString('base64'),
            v: v.toString(),
            r: unpadHex(bigIntToHex(r)),
            s: unpadHex(bigIntToHex(s))
        };

    } catch (error) {
        return new TxDeserializationError(
            error.message,
            {
                'nativeBlockHash': args.nativeBlockHash,
                'tx': tx,
                'block_num': args.blockNum,
                'error': error.message,
                'stack': error.stack
            });
    }
}

async function createWithdraw(args: HandlerArguments): Promise<StorageEvmTransaction | TxDeserializationError> {
    const tx = args.tx as EosioEvmWithdraw;
    const address = await queryAddress(tx.to, rpc, logger);

    const quantity = parseAsset(tx.quantity);

    const [v, r, s] = generateUniqueVRS(
        args.nativeBlockHash, address, args.trx_index);

    const txParams = {
        nonce: 0,
        gasPrice: stdGasPrice,
        gasLimit: stdGasLimit,
        to: ZERO_ADDR,
        value: BigInt(quantity.amount) * BigInt('100000000000000'),
        data: "0x",
        v: v,
        r: r,
        s: s
    };
    try {
        const evmTx = new TEVMTransaction(txParams, {common});
        const inputData = '0x' + arrayToHex(evmTx.data);
        return {
            hash: '0x' + arrayToHex(evmTx.hash()),
            from: '0x' + address.toLowerCase(),
            trx_index: args.trx_index,
            block: args.blockNum,
            block_hash: "",
            to: evmTx.to?.toString(),
            input_data: inputData,
            input_trimmed: inputData.substring(0, KEYWORD_STRING_TRIM_SIZE),
            value: evmTx.value?.toString(16),
            value_d: tx.quantity,
            nonce: evmTx.nonce?.toString(),
            gas_price: evmTx.gasPrice?.toString(),
            gas_limit: evmTx.gasLimit?.toString(),
            status: 1,
            itxs: [],
            epoch: 0,
            createdaddr: "",
            gasused: stdGasLimit.toString(),
            gasusedblock: '',
            charged_gas_price: '0',
            output: "",
            raw: Buffer.from(evmTx.serialize()).toString('base64'),
            v: v.toString(),
            r: unpadHex(bigIntToHex(r)),
            s: unpadHex(bigIntToHex(s))
        };

    } catch (error) {
        return new TxDeserializationError(
            error.message,
            {
                'nativeBlockHash': args.nativeBlockHash,
                'tx': tx,
                'block_num': args.blockNum,
                'error': error.message,
                'stack': error.stack
            });
    }
}

workerpool.worker({
    createEvm: createEvm,
    createDeposit: createDeposit,
    createWithdraw: createWithdraw
});