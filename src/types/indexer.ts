import {TxDeserializationError} from '../handlers.js';
import {StorageEosioAction} from './evm.js';
import {StorageEosioDelta} from '../utils/evm.js';

export type ConnectorConfig = {
    node: string;
    auth: {
        username: string;
        password: string;
    },
    requestTimeout: number,
    docsPerIndex: number,
    subfix: {
        delta: string;
        error: string;
        transaction: string;
        fork: string;
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
    remoteEndpoint: string;
    wsEndpoint: string;
    evmBlockDelta: number;
    evmDeployBlock: number;
    evmPrevHash: string;
    evmValidateHash: string;
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

export const DEFAULT_CONF = {
    "chainName": "telos-local",
    "chainId": 41,

    "endpoint": "http://127.0.0.1:8888",
    "remoteEndpoint": "http://127.0.0.1:8888",
    "wsEndpoint": "ws://127.0.0.1:29999",

    "evmBlockDelta": 2,
    "evmDeployBlock": 35,
    "evmPrevHash": "",
    "evmValidateHash": "",

    "startBlock": 35,
    "stopBlock": 4294967295,
    "irreversibleOnly": false,
    "perf": {
        "workerAmount": 4,
        "elasticDumpSize": 2048
    },

    "elastic": {
        "node": "http://127.0.0.1:9200",
        "auth": {
            "username": "elastic",
            "password": "password"
        },
        "requestTimeout": 480000,
        "docsPerIndex": 10000000,
        "subfix": {
            "delta": "delta-v1.5",
            "transaction": "action-v1.5",
            "error": "error-v1.5",
            "fork": "fork-v1.5"
        }
    },

    "broadcast": {
        "wsHost": "127.0.0.1",
        "wsPort": 7300
    }
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

export type StorageForkInfo = {
    timestamp: string;
    lastNonForked: number;
    lastForked: number;
};
