import { parentPort, workerData } from 'worker_threads';

import { ConnectorConfig  } from '../types/indexer';
import { ElasticConnector } from '../database/connector';

import {
    StorageEosioAction, StorageEvmTransaction,
} from '../types/evm';

import logger from '../utils/winston';
import Bloom from '../utils/evm';

var PriorityQueue = require("js-priority-queue");

import { BlockHeader } from '@ethereumjs/block'

const createKeccakHash = require('keccak');
const BN = require('bn.js');


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
    prevHash: string 
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
): Buffer {
    const hash = createKeccakHash('keccak256');

    evmTxs.sort(
        // @ts-ignore
        (a, b) => {
            return a.action_ordinal - b.action_ordinal;
        }
    );

    for (const tx of evmTxs)
        hash.update(tx.evmTx.hash);

    return Buffer.from(hash.digest('hex'), 'hex');
}


function getBlockGasUsed(
    evmTxs: Array<{
        trx_id: string,
        action_ordinal: number,
        signatures: string[],
        evmTx: StorageEvmTransaction
    }>
): typeof BN {

    let totalGasUsed = 0;

    for (const evmTx of evmTxs)
        totalGasUsed += evmTx.evmTx.gasused;

    return new BN(totalGasUsed);
}

function generateBloom(
    evmTxs: Array<{
        trx_id: string,
        action_ordinal: number,
        signatures: string[],
        evmTx: StorageEvmTransaction
    }>
): Buffer {
    const blockBloom: Bloom = new Bloom();

    for (const evmTx of evmTxs) {
        if (evmTx.evmTx.bloom)
            blockBloom.or(evmTx.evmTx.bloom);
    }

    return blockBloom.bitvector;
}


function drainBlocks() {

    if (blockDrain.length == 0)
        return;

    let current: HasherBlockInfo = blockDrain.peek();
    while (current.nativeBlockNumber == lastInOrder + 1) {

        const evmTxs = current.evmTxs;

        // genereate 'valid' block header
        const blockHeader = BlockHeader.fromHeaderData({
            'parentHash': Buffer.from(prevHash, 'hex'),
            'transactionsTrie': generateTxRootHash(evmTxs),
            'bloom': generateBloom(evmTxs),
            'number': new BN(current.evmBlockNumber),
            'gasLimit': new BN(1000000000),
            'gasUsed': getBlockGasUsed(evmTxs),
            'difficulty': new BN(0),
            'timestamp': new BN(Date.parse(current.blockTimestamp) / 1000),
            'extraData': Buffer.from(current.nativeBlockHash, 'hex')
        })

        const currentBlockHash = blockHeader.hash().toString('hex');

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

        prevHash = currentBlockHash;
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
