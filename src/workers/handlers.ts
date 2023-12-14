import workerpool from 'workerpool';
import Common, {Chain, Hardfork} from "@ethereumjs/common";
import {EosioEvmDeposit, EosioEvmRaw, EosioEvmWithdraw, StorageEvmTransaction} from "../types/evm.js";
import {
    Bloom,
    generateUniqueVRS, KEYWORD_STRING_TRIM_SIZE,
    queryAddress, RECEIPT_LOG_END, RECEIPT_LOG_START, removeHexPrefix,
    stdGasLimit,
    stdGasPrice,
    TxDeserializationError,
    ZERO_ADDR
} from "../utils/evm.js";
import {getRPCClient, parseAsset} from "../utils/eosio.js";
import BN from "bn.js";
import {TEVMTransaction} from "../utils/evm-tx.js";
import {createLogger, format, transports} from "winston";
import {addHexPrefix, isHexPrefixed, isValidAddress, unpadHexString} from "@ethereumjs/util";

const common = Common.default.custom({
    chainId: parseInt(process.env.CHAIN_ID, 10),
    defaultHardfork: Hardfork.Istanbul
}, {baseChain: Chain.Mainnet});

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

async function createEvm(
    nativeBlockHash: string,
    trx_index: number,
    blockNum: number,
    tx: EosioEvmRaw,
    consoleLog: string
): Promise<StorageEvmTransaction | TxDeserializationError> {
    try {
        let receiptLog = consoleLog.slice(
            consoleLog.indexOf(RECEIPT_LOG_START) + RECEIPT_LOG_START.length,
            consoleLog.indexOf(RECEIPT_LOG_END)
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
                    'nativeBlockHash': nativeBlockHash,
                    'tx': tx,
                    'block_num': blockNum,
                    'error': error.message
                }
            );
        }

        const txRaw = Buffer.from(tx.tx, "hex");

        let evmTx = TEVMTransaction.fromSerializedTx(txRaw, {common});

        const isSigned = evmTx.isSigned();

        const evmTxParams = evmTx.toJSON();

        let fromAddr = null;
        let v, r, s;

        if (!isSigned) {

            let senderAddr = tx.sender.toLowerCase();

            if (!isHexPrefixed(senderAddr))
                senderAddr = `0x${senderAddr}`;

            [v, r, s] = generateUniqueVRS(
                nativeBlockHash, senderAddr, trx_index);

            evmTxParams.v = addHexPrefix(v.toString(16));
            evmTxParams.r = r;
            evmTxParams.s = s;

            evmTx = TEVMTransaction.fromTxData(evmTxParams, {common});

            if (isValidAddress(senderAddr)) {
                fromAddr = senderAddr;

            } else {
                return new TxDeserializationError(
                    'Raw EVM deserialization error',
                    {
                        'nativeBlockHash': nativeBlockHash,
                        'tx': tx,
                        'block_num': blockNum,
                        'error': `error deserializing address \'${tx.sender}\'`
                    }
                );
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
            hash: '0x' + evmTx.hash().toString('hex'),
            trx_index: trx_index,
            block: blockNum,
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
            gasusedblock: '',
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

        return txBody;
    } catch (error) {
        return new TxDeserializationError(
            'Raw EVM deserialization error',
            {
                'nativeBlockHash': nativeBlockHash,
                'tx': tx,
                'block_num': blockNum,
                'error': error.message
            }
        );
    }
}

async function createDeposit(
    nativeBlockHash: string,
    trx_index: number,
    blockNum: number,
    tx: EosioEvmDeposit
): Promise<StorageEvmTransaction | TxDeserializationError> {
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
                    'nativeBlockHash': nativeBlockHash,
                    'tx': tx,
                    'block_num': blockNum
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
        const evmTx = TEVMTransaction.fromTxData(txParams, {common});
        const gasUsed = new BN(removeHexPrefix(stdGasLimit), 16);
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
            value_d: tx.quantity,
            nonce: evmTx.nonce?.toString(),
            gas_price: evmTx.gasPrice?.toString(),
            gas_limit: evmTx.gasLimit.toString(),
            status: 1,
            itxs: new Array(),
            epoch: 0,
            createdaddr: "",
            gasused: gasUsed.toString(),
            gasusedblock: '',
            charged_gas_price: '0',
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

async function createWithdraw(
    nativeBlockHash: string,
    trx_index: number,
    blockNum: number,
    tx: EosioEvmWithdraw
): Promise<StorageEvmTransaction | TxDeserializationError> {
        const address = await queryAddress(tx.to, rpc, logger);

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
            const evmTx = new TEVMTransaction(txParams, {common});
            const gasUsed = new BN(removeHexPrefix(stdGasLimit), 16);
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
                value_d: tx.quantity,
                nonce: evmTx.nonce?.toString(),
                gas_price: evmTx.gasPrice?.toString(),
                gas_limit: evmTx.gasLimit?.toString(),
                status: 1,
                itxs: new Array(),
                epoch: 0,
                createdaddr: "",
                gasused: gasUsed.toString(),
                gasusedblock: '',
                charged_gas_price: '0',
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

workerpool.worker({
    createEvm: createEvm,
    createDeposit: createDeposit,
    createWithdraw: createWithdraw
});