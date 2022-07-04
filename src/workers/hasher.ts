import { parentPort, workerData } from 'worker_threads';

import { ConnectorConfig  } from '../types/indexer';
import { ElasticConnector } from '../database/connector';

import {StorageEosioAction, StorageEvmTransaction} from '../types/evm';

import logger from '../utils/winston';

var SortedMap = require("collections/sorted-map");

const createKeccakHash = require('keccak');


export interface HasherBlockInfo{
    nativeBlockNumber: number,
    evmBlockNum: number,
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
    elasticConf: ConnectorConfig
} = workerData;

const connector = new ElasticConnector(
    args.chainName, args.elasticConf);

logger.info('Launching hasher worker...');

const blockDrain = SortedMap();

let prevHash: string | null = null;

async function drainBlocks() {

    let current = blockDrain.findLeast();
    while (blockDrain.has(current + 1)) {

        // genereate block hash
        const hash = createKeccakHash('keccak256');
        hash.update(prevHash);

        const blockInfo: HasherBlockInfo = blockDrain.get(current);
        const evmTxs = blockInfo.evmTxs; 

        evmTxs.sort(
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
                "@timestamp": blockInfo.blockTimestamp,
                "block_num": blockInfo.nativeBlockNumber,
                "code": "eosio",
                "table": "global",
                "@global": {
                    "block_num": blockInfo.evmBlockNum
                },
                "@evmBlockHash": currentBlockHash 
            }
        };

        if (evmTxs.length > 0) {
            for (const [i, evmTxData] of evmTxs.entries()) {
                evmTxData.evmTx.block_hash = currentBlockHash;
                storableActions.push({
                    "@timestamp": blockInfo.blockTimestamp,
                    "trx_id": evmTxData.trx_id,
                    "action_ordinal": evmTxData.action_ordinal,
                    "signatures": evmTxData.signatures,
                    "@raw": evmTxData.evmTx
                });
            }
        }

        // push to db 
        connector.pushBlock(storableBlockInfo);

        prevHash = currentBlockHash;
        blockDrain.delete(current);
        current = blockDrain.findLeast();
    }
    
}

parentPort.on(
    'message',
    (msg: {type: string, params: any}) => {

    try {
        if (msg.type == 'init') {
            return connector.init().then(() => {
                return parentPort.postMessage({success: true});
            })
        }

        if (msg.type == 'get_last_indexed') {
            return connector.getLastIndexedBlock().then((res) => {
                console.log(res);
                if (prevHash == null) {
                    if(res != null) {
                        prevHash = res['delta']['@evmBlockHash']
                    } else {
                        prevHash = createKeccakHash('keccak256')
                            .update(args.chainId.toString(16))
                            .digest('hex');
                    }
                }

                return parentPort.postMessage({
                    success: true, result: res}); 
            });
        }

        if (msg.type == 'block') {
            const blockInfo: HasherBlockInfo = msg.params;
            const blockNum = blockInfo.evmBlockNum;
            blockDrain.add(blockInfo, blockNum);

            if (blockDrain.has(blockNum - 1))
                drainBlocks().then()

            return parentPort.postMessage({success: true});
        }

        return parentPort.postMessage({
            success: false, error: 'unknown type'});
    } catch (e) {
        return parentPort.postMessage({success: false, message: e});
    }
});
