import workerpool from 'workerpool';
import {
    arrayToHex,
    generateUniqueVRS, hexStringToUint8Array,
    queryAddress, RECEIPT_LOG_END, RECEIPT_LOG_START,
    stdGasLimit,
    stdGasPrice,
    TxDeserializationError,
    ZERO_ADDR
} from "../utils/evm.js";
import {getRPCClient, parseAsset} from "../utils/eosio.js";
import {createLogger, format, transports} from "winston";
import {addHexPrefix, isHexPrefixed, isValidAddress, unpadHex} from "@ethereumjs/util";
import * as evm from "@ethereumjs/common";
import {Bloom} from "@ethereumjs/vm";
import {TEVMTransaction} from "telos-evm-custom-ds";
import {EosioEvmDeposit, EosioEvmRaw, EosioEvmWithdraw, IndexedInternalTx, IndexedTx} from "../types/indexer.js";
import {featureManager, initFeatureManager} from "../features.js";
import {CompatTarget} from "../types/config";

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

initFeatureManager({
    mayor: parseInt(process.env.COMPAT_MAYOR),
    minor: parseInt(process.env.COMPAT_MINOR),
    patch: parseInt(process.env.COMPAT_PATCH)
});

export interface HandlerArguments {
    nativeBlockHash: string;
    trx_index: number;
    blockNum: bigint;
    tx: EosioEvmRaw | EosioEvmDeposit | EosioEvmWithdraw;
    consoleLog?: string;
}

async function createEvm(args: HandlerArguments): Promise<IndexedTx | TxDeserializationError> {
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

        let itxs: IndexedInternalTx[] = [];
        if (featureManager.isFeatureEnabled('STORE_ITXS') && receipt.itxs) {
            itxs = receipt.itxs.map((itx) => {
                return {
                    callType: itx.callType,
                    from: itx.from,
                    gas: BigInt(itx.gas),
                    input: itx.input,
                    to: itx.to,
                    value: BigInt(itx.value),
                    gasUsed: BigInt(itx.gasUsed),
                    output: itx.output,
                    subTraces: itx.subtraces,
                    traceAddress: itx.traceAddress,
                    type: itx.type,
                    depth: itx.depth
                };
            });

            itxs = receipt.itxs;
        }

        const logs = []
        const logsBloom = new Bloom();
        if (receipt.logs && receipt.logs.length > 0) {
            for (const log of receipt.logs) {
                logsBloom.add(hexStringToUint8Array(log.address.padStart(40, '0')));
                for (const topic of log.topics)
                    logsBloom.add(hexStringToUint8Array(topic.padStart(64, '0')));
            }
        }

        return {
            trxId: '',
            trxIndex: args.trx_index,
            blockNum: args.blockNum,
            blockHash: "",
            actionOrdinal: 0,
            hash: '0x' + arrayToHex(evmTx.hash()),

            raw: evmTx.serialize(),

            from: isSigned ? evmTx.getSenderAddress().toString().toLowerCase() : fromAddr,
            to: evmTx.to?.toString(),
            inputData: evmTx.data,
            value: evmTx.value,
            nonce: evmTx.nonce,
            gasPrice: evmTx.gasPrice,
            gasLimit: evmTx.gasLimit,
            v,
            r,
            s,

            status: receipt.status as (0 | 1),
            itxs,
            epoch: receipt.epoch,
            createAddr: receipt.createdaddr.toLowerCase(),
            gasUsed: BigInt(addHexPrefix(receipt.gasused)),
            gasUsedBlock: 0n,
            chargedGasPrice: BigInt(addHexPrefix(receipt.charged_gas)),
            output: receipt.output,
            logs,
            logsBloom: logsBloom.bitvector,

            errors: receipt.errors
        };
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

async function createDeposit(args: HandlerArguments): Promise<IndexedTx | TxDeserializationError> {
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
        return {
            trxId: '',
            trxIndex: args.trx_index,
            actionOrdinal: 0,
            blockNum: args.blockNum,
            blockHash: "",
            hash: '0x' + arrayToHex(evmTx.hash()),

            raw: evmTx.serialize(),

            from: ZERO_ADDR,
            to: evmTx.to?.toString(),
            inputData: evmTx.data,
            value: evmTx.value,
            nonce: evmTx.nonce,
            gasPrice: evmTx.gasPrice,
            gasLimit: evmTx.gasLimit,
            v,
            r,
            s,

            status: 1,
            itxs: [],
            epoch: 0,
            createAddr: '',
            gasUsed: stdGasLimit,
            gasUsedBlock: 0n,
            chargedGasPrice: 0n,
            output: '',
            logs: [],
            logsBloom: new Bloom().bitvector,

            errors: []
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

async function createWithdraw(args: HandlerArguments): Promise<IndexedTx | TxDeserializationError> {
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
        return {
            trxId: '',
            trxIndex: args.trx_index,
            actionOrdinal: 0,
            blockNum: args.blockNum,
            blockHash: "",
            hash: '0x' + arrayToHex(evmTx.hash()),

            raw: evmTx.serialize(),

            from: '0x' + address.toLowerCase(),
            to: evmTx.to?.toString(),
            inputData: evmTx.data,
            value: evmTx.value,
            nonce: evmTx.nonce,
            gasPrice: evmTx.gasPrice,
            gasLimit: evmTx.gasLimit,
            v,
            r,
            s,

            status: 1,
            itxs: [],
            epoch: 0,
            createAddr: '',
            gasUsed: stdGasLimit,
            gasUsedBlock: 0n,
            chargedGasPrice: 0n,
            output: '',
            logs: [],
            logsBloom: new Bloom().bitvector,

            errors: []
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

const handlers = {
    createEvm: createEvm,
    createDeposit: createDeposit,
    createWithdraw: createWithdraw
};

workerpool.worker(handlers);