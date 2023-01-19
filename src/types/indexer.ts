import {TxDeserializationError} from '../handlers.js';
import {StorageEosioAction} from './evm.js';
import {StorageEosioDelta} from '../utils/evm.js';

export type ConnectorConfig = {
    node: string;
    auth: {
        username: string;
        password: string;
    },
    docsPerIndex: number,
    subfix: {
        delta: string;
        error: string;
        transaction: string;
    }
};

export type BroadcasterConfig = {
    wsHost: string;
    wsPort: number;
};

export type IndexerConfig = {
    chainName: string;
    chainId: number;
    endpoint: string;
    wsEndpoint: string;
    evmDeployBlock: number;
    evmPrevHash: string;
    evmDelta: number;
    startBlock: number;
    stopBlock: number;
    irreversibleOnly: boolean;
    perf: {
        workerAmount: number;
        elasticDumpSize: number;
    },
    elastic: ConnectorConfig;
    broadcast: BroadcasterConfig;
};


export type IndexedBlockInfo = {
    transactions: StorageEosioAction[];
    errors: TxDeserializationError[],
    delta: StorageEosioDelta;
    nativeHash: string;
    parentHash: string;
    receiptsRoot: string;
    blockBloom: string;
};

export enum IndexerState {
    SYNC = 0,
    HEAD = 1
}

export type StartBlockInfo = {
    startBlock: number;
    startEvmBlock?: number;
    prevHash: string;
}

export interface ElasticIndex {
    "health": string;
    "status": string;
    "index": string;
    "uuid": string;
    "pri": string;
    "rep": string;
    "docs.count": string;
    "docs.deleted": string;
    "store.size": string;
    "pri.store.size": string;
}
