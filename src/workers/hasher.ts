import { parentPort, workerData } from 'worker_threads';

import { IndexerConfig, IndexerState  } from '../types/indexer';
import { Connector } from '../database/connector';

import {
    StorageEosioAction, StorageEvmTransaction,
} from '../types/evm';

import logger from '../utils/winston';
import {Bloom, BlockHeader} from '../utils/evm';

var PriorityQueue = require("js-priority-queue");

import { Trie } from '@ethereumjs/trie';
import RLP from 'rlp';
import {
  bigIntToBuffer,
  bufArrToArr,
  intToBuffer,
} from '@ethereumjs/util';
import {Log} from '@ethereumjs/vm/dist/evm/types';
import {TxDeserializationError} from '../handlers';
import moment from 'moment';

const BN = require('bn.js');


type TxArray = Array<{
    trx_id: string,
    action_ordinal: number,
    signatures: string[],
    evmTx: StorageEvmTransaction
}>;

export interface HasherBlockInfo{
    nativeBlockHash: string,
    nativeBlockNumber: number,
    evmBlockNumber: number,
    blockTimestamp: string,
    evmTxs: TxArray,
    errors: TxDeserializationError[]
};

const args: {
    config: IndexerConfig,
    startEvmBlock: number,
    prevHash: string 
} = workerData;

const connector = new Connector(
    args.config, true);

logger.info('Launching hasher worker...');

const blockDrain = new PriorityQueue({
    // @ts-ignore
    comparator: function(a, b) {
        return a.evmBlockNumber - b.evmBlockNumber;
    }
});

let lastInOrder = args.startEvmBlock - 1;
let prevHash = args.prevHash;

function generateTxRootHash(evmTxs: TxArray): Buffer {
    const trie = new Trie()
    for (const [i, tx] of evmTxs.entries())
        trie.put(Buffer.from(RLP.encode(i)), tx.evmTx.raw).then();

    return trie.root()
}

interface TxReceipt {
    cumulativeGasUsed: typeof BN,
    bitvector: Buffer,
    logs: Log[],
    status: number
};

/**
 * Returns the encoded tx receipt.
 */
export function encodeReceipt(receipt: TxReceipt) {
    const encoded = Buffer.from(
        RLP.encode(
            bufArrToArr([
                (receipt.status === 0
                    ? Buffer.from([])
                    : Buffer.from('01', 'hex')),
                bigIntToBuffer(receipt.cumulativeGasUsed),
                receipt.bitvector,
                receipt.logs,
            ])
        )
    )

    return encoded
}

function generateReceiptRootHash(evmTxs: TxArray): Buffer {
    const receiptTrie = new Trie()
    for (const [i, tx] of evmTxs.entries()) {
        const logs: Log[] = [];

        let bloom = new Bloom().bitvector;

        if (tx.evmTx.logsBloom)
            bloom = Buffer.from(tx.evmTx.logsBloom, 'hex');

        if (tx.evmTx.logs) {
            for (const [j, hexLogs] of tx.evmTx.logs.entries()) {
                const topics: Buffer[] = [];

                for (const topic of hexLogs.topics)
                    topics.push(Buffer.from(topic, 'hex'))

                logs.push([ 
                    Buffer.from(hexLogs.address, 'hex'),
                    topics,
                    Buffer.from(hexLogs.data, 'hex')
                ]);
            }
        }

        const receipt: TxReceipt = {
            cumulativeGasUsed: new BN(tx.evmTx.gasusedblock),
            bitvector: bloom,
            logs: logs,
            status: tx.evmTx.status
        };
        const encodedReceipt = encodeReceipt(receipt)
        receiptTrie.put(Buffer.from(RLP.encode(i)), encodedReceipt).then();
    }
    return receiptTrie.root()
}

function getBlockGasUsed(evmTxs: TxArray): typeof BN {

    let totalGasUsed = 0;

    for (const evmTx of evmTxs)
        totalGasUsed += evmTx.evmTx.gasused;

    return new BN(totalGasUsed);
}

function generateBloom(evmTxs: TxArray): Buffer {
    const blockBloom: Bloom = new Bloom();

    for (const evmTx of evmTxs)
        if (evmTx.evmTx.logsBloom)
            blockBloom.or(
                new Bloom(Buffer.from(evmTx.evmTx.logsBloom, 'hex')));

    return blockBloom.bitvector;
}


function drainBlocks() {

    if (blockDrain.length == 0)
        return;

    let current: HasherBlockInfo = blockDrain.peek();
    while (current.evmBlockNumber == lastInOrder + 1) {

        const evmTxs = current.evmTxs;

        const transactionsRoot = generateTxRootHash(evmTxs);
        const receiptsRoot = generateReceiptRootHash(evmTxs);
        const bloom = generateBloom(evmTxs);

        const blockTimestamp = moment.utc(current.blockTimestamp);

        // generate 'valid' block header
        const blockHeader = BlockHeader.fromHeaderData({
            'parentHash': Buffer.from(prevHash, 'hex'),
            'transactionsTrie': transactionsRoot,
            'receiptTrie': receiptsRoot,
            'bloom': bloom,
            'number': new BN(current.evmBlockNumber),
            'gasLimit': new BN(1000000000),
            'gasUsed': getBlockGasUsed(evmTxs),
            'difficulty': new BN(0),
            'timestamp': new BN(blockTimestamp.unix()),
            'extraData': Buffer.from(current.nativeBlockHash, 'hex')
        })

        const currentBlockHash = blockHeader.hash().toString('hex');

        // generate storeable block info
        const storableActions: StorageEosioAction[] = [];
        const storableBlockInfo = {
            "transactions": storableActions,
            "errors": current.errors,
            "delta": {
                "@timestamp": blockTimestamp.format(),
                "block_num": current.nativeBlockNumber,
                "code": "eosio",
                "table": "global",
                "@global": {
                    "block_num": current.evmBlockNumber
                },
                "@evmBlockHash": currentBlockHash 
            },
            "nativeHash": current.nativeBlockHash.toLowerCase(),
            "parentHash": prevHash,
            "transactionsRoot": transactionsRoot.toString('hex'),
            "receiptsRoot": receiptsRoot.toString('hex'),
            "blockBloom": bloom.toString('hex')
        };

        if (evmTxs.length > 0) {
            for (const [i, evmTxData] of evmTxs.entries()) {
                evmTxData.evmTx.block_hash = currentBlockHash;
                delete evmTxData.evmTx['raw'];
                storableActions.push({
                    "@timestamp": current.blockTimestamp,
                    "trx_id": evmTxData.trx_id,
                    "action_ordinal": evmTxData.action_ordinal,
                    "signatures": evmTxData.signatures,
                    "@raw": evmTxData.evmTx
                });
            }
        }

        // push to db
        connector.pushBlock(storableBlockInfo);

        lastInOrder = current.evmBlockNumber;

        prevHash = currentBlockHash;
        blockDrain.dequeue();

        if (blockDrain.length > 0)
            current = blockDrain.peek();
    }

}

parentPort.on(
    'message',
    async (msg: {type: string, params: any}) => {

    try {

        // block handler
        if (msg.type == 'block') {
            const blockInfo: HasherBlockInfo = msg.params;
            blockDrain.queue(blockInfo);
            drainBlocks();

            return parentPort.postMessage({success: true, last: lastInOrder});
        }

        // lastInOrder query
        if (msg.type == 'last')
            return parentPort.postMessage({success: true, last: lastInOrder});

        // db write mode change handler
        if (msg.type == 'state') {
            connector.setState(msg.params.state);
            return parentPort.postMessage({success: true});
        }

        // fork handler
        if (msg.type == 'fork') {
            await connector.cleanupFork(msg.params['blockNum'])
            return parentPort.postMessage({success: true});
        }

        return parentPort.postMessage({
            success: false, error: 'unknown type'});
    } catch (e) {
        return parentPort.postMessage({success: false, message: e});
    }
});
