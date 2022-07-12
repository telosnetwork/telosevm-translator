import { parentPort, workerData } from 'worker_threads';

import { ConnectorConfig  } from '../types/indexer';
import { ElasticConnector } from '../database/connector';

import {StorageEosioAction, StorageEvmTransaction} from '../types/evm';

import logger from '../utils/winston';

var PriorityQueue = require("js-priority-queue");

const createKeccakHash = require('keccak');


export interface HasherBlockInfo{
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

function drainBlocks() {

    if (blockDrain.length == 0)
        return;

    let current: HasherBlockInfo = blockDrain.peek();
    while (current.nativeBlockNumber == lastInOrder + 1) {

        // genereate block hash
        const hash = createKeccakHash('keccak256');
        hash.update(prevHash);

        const evmTxs = current.evmTxs; 

        evmTxs.sort(
            // @ts-ignore
            (a, b) => {
                return a.action_ordinal - b.action_ordinal;
            }
        );

        for (const tx of evmTxs)
            hash.update(tx.evmTx.hash);

        const currentBlockHash = hash.digest('hex');

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
