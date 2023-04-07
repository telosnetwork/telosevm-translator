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
    "chainName": "telos-mainnet",
    "chainId": 40,

    "endpoint": "http://mainnet.telos.net",
    "wsEndpoint": "ws://api1.hosts.caleos.io:18999",

    "evmDeployBlock": 180698860,
    "evmPrevHash": "",

    "startBlock": 180698860,
    "stopBlock": 4294967295,
    "irreversibleOnly": false,
    "perf": {
        "workerAmount": 4,
        "elasticDumpSize": 2048
    },

    "elastic": {
        "node": "http://localhost:9200",
        "auth": {
            "username": "elastic",
            "password": "password"
        },
        "docsPerIndex": 10000000,
        "subfix": {
            "delta": "delta-v1.5",
            "transaction": "action-v1.5",
            "error": "error-v1.5"
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
