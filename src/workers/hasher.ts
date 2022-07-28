import { parentPort, workerData } from 'worker_threads';

import { ConnectorConfig  } from '../types/indexer';
import { ElasticConnector } from '../database/connector';

import {
    ethBlockHeader,
    StorageEosioAction, StorageEvmTransaction,
    Hash32,
    TreeRoot,
    BigIntHex,
    BLOCK_GAS_LIMIT,
    HexString,
    BloomHex
} from '../types/evm';

import logger from '../utils/winston';
import Bloom from '../utils/evm';

var PriorityQueue = require("js-priority-queue");

const createKeccakHash = require('keccak');


export interface HasherBlockInfo{
    nativeBlockHash: string,
    nativeBlockNumber: number,
    evmBlockNumber: number,
    blockTimestamp: string,
    evmTxs: Array<{
        trx_id: string,
        action_ordinal: number,
        signatures: string[],
        evmTx: StorageEvmTransaction
    }>
};


const args: {
    chainName: string,
    chainId: number,
    elasticConf: ConnectorConfig,
    startBlock: number,
    prevHash: Hash32 
} = workerData;

const connector = new ElasticConnector(
    args.chainName, args.elasticConf);

logger.info('Launching hasher worker...');

const blockDrain = new PriorityQueue({
    // @ts-ignore
    comparator: function(a, b) {
        return a.nativeBlockNumber - b.nativeBlockNumber;
    }
});

let lastInOrder = args.startBlock;
let prevHash = args.prevHash;


function generateTxRootHash(
    evmTxs: Array<{
        trx_id: string,
        action_ordinal: number,
        signatures: string[],
        evmTx: StorageEvmTransaction
    }>
): TreeRoot {
    const hash = createKeccakHash('keccak256');

    evmTxs.sort(
        // @ts-ignore
        (a, b) => {
            return a.action_ordinal - b.action_ordinal;
        }
    );

    for (const tx of evmTxs)
        hash.update(tx.evmTx.hash);

    return new TreeRoot(hash.digest('hex'));
}


function getBlockGasUsed(
    evmTxs: Array<{
        trx_id: string,
        action_ordinal: number,
        signatures: string[],
        evmTx: StorageEvmTransaction
    }>
): BigIntHex {

    let totalGasUsed = 0;

    for (const evmTx of evmTxs)
        totalGasUsed += evmTx.evmTx.gasused;

    return new BigIntHex(totalGasUsed);
}

function generateReceiptRootHash(
    evmTxs: Array<{
        trx_id: string,
        action_ordinal: number,
        signatures: string[],
        evmTx: StorageEvmTransaction
    }>
): TreeRoot {
    
    return new TreeRoot(''); 
}

function generateBloom(
    evmTxs: Array<{
        trx_id: string,
        action_ordinal: number,
        signatures: string[],
        evmTx: StorageEvmTransaction
    }>
):BloomHex {
    const blockBloom: Bloom = new Bloom();

    for (const evmTx of evmTxs) {
        if (evmTx.evmTx.bloom)
            blockBloom.or(evmTx.evmTx.bloom);
    }

    return new BloomHex(blockBloom.bitvector.toString('hex'));
}


function drainBlocks() {

    if (blockDrain.length == 0)
        return;

    let current: HasherBlockInfo = blockDrain.peek();
    while (current.nativeBlockNumber == lastInOrder + 1) {

        const evmTxs = current.evmTxs;

        // genereate 'valid' block header
        const blockHeader = ethBlockHeader();

        blockHeader.set('parentHash', prevHash);
        blockHeader.set('transactionRoot', generateTxRootHash(evmTxs));
        //blockHeader.set('receiptRoot', generateReceiptRootHash(evmTxs));
        blockHeader.set('bloom', generateBloom(evmTxs));
        blockHeader.set('blockNumber', new BigIntHex(current.evmBlockNumber));
        blockHeader.set('gasLimit', new BigIntHex(BLOCK_GAS_LIMIT));
        blockHeader.set('gasUsed', getBlockGasUsed(current.evmTxs));
        blockHeader.set('timestamp', new BigIntHex(Date.parse(current.blockTimestamp) / 1000));
        blockHeader.set('extraData', new HexString(current.nativeBlockHash));

        const currentBlockHash = blockHeader.hash().toString();

        // generate storeable block info
        const storableActions: StorageEosioAction[] = [];
        const storableBlockInfo = {
            "transactions": storableActions,
            "delta": {
                "@timestamp": current.blockTimestamp,
                "block_num": current.nativeBlockNumber,
                "code": "eosio",
                "table": "global",
                "@global": {
                    "block_num": current.evmBlockNumber
                },
                "@evmBlockHash": currentBlockHash 
            }
        };

        if (evmTxs.length > 0) {
            for (const [i, evmTxData] of evmTxs.entries()) {
                evmTxData.evmTx.block_hash = currentBlockHash;
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

        lastInOrder = current.nativeBlockNumber;

        prevHash = new Hash32(currentBlockHash);
        blockDrain.dequeue();

        if (blockDrain.length > 0)
            current = blockDrain.peek();
    }
    
}

parentPort.on(
    'message',
    (msg: {type: string, params: any}) => {

    try {

        if (msg.type == 'block') {
            const blockInfo: HasherBlockInfo = msg.params;
            blockDrain.queue(blockInfo);

            if (lastInOrder = blockInfo.nativeBlockNumber - 1)
                drainBlocks();

            return parentPort.postMessage({success: true, qlen: blockDrain.length});
        }

        return parentPort.postMessage({
            success: false, error: 'unknown type'});
    } catch (e) {
        return parentPort.postMessage({success: false, message: e});
    }
});
