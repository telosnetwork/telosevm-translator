import {StorageEosioAction, StorageEosioDelta} from './evm.js';
import {TxDeserializationError} from '../utils/evm.js';

export type ConnectorConfig = {
    node: string;
    auth: {
        username: string;
        password: string;
    },
    requestTimeout: number,
    docsPerIndex: number,
    scrollSize: number,
    scrollWindow: string,
    numberOfShards?: number,
    numberOfReplicas?: number,
    refreshInterval?: number,
    codec?: string,
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
    logLevel: string;
    readerLogLevel: string;
    chainName: string;
    chainId: number;

    runtime: {
        trimFrom?: number;
        skipIntegrityCheck?: boolean;
        onlyDBCheck?: boolean;
        gapsPurge?: boolean;
        skipStartBlockCheck?: boolean;
        skipRemoteCheck?: boolean;
        reindexInto?: string;
    };

    endpoint: string;
    remoteEndpoint: string;
    wsEndpoint: string;
    evmBlockDelta: number;
    evmPrevHash: string;
    evmValidateHash: string;
    startBlock: number;
    stopBlock: number;
    irreversibleOnly: boolean;
    blockHistorySize: number;
    perf: {
        stallCounter: number;
        readerWorkerAmount: number;
        evmWorkerAmount: number;
        elasticDumpSize: number;
        maxMessagesInFlight?: number;
    },
    elastic: ConnectorConfig;
    broadcast: BroadcasterConfig;
};

export const DEFAULT_CONF = {
    "logLevel": "debug",
    "readerLogLevel": "info",
    "chainName": "telos-local",
    "chainId": 41,

    "runtime": {},

    "endpoint": "http://127.0.0.1:8888",
    "remoteEndpoint": "http://127.0.0.1:8888",
    "wsEndpoint": "ws://127.0.0.1:29999",

    "evmBlockDelta": 2,
    "evmPrevHash": "",
    "evmValidateHash": "",

    "startBlock": 35,
    "stopBlock": -1,
    "irreversibleOnly": false,
    "blockHistorySize": (15 * 60 * 2),  // 15 minutes in blocks
    "perf": {
        "stallCounter": 5,
        "readerWorkerAmount": 4,
        "evmWorkerAmount": 4,
        "elasticDumpSize": 2 * 1000,
    },

    "elastic": {
        "node": "http://127.0.0.1:9200",
        "auth": {
            "username": "elastic",
            "password": "password"
        },
        "requestTimeout": 5 * 1000,
        "docsPerIndex": 10000000,
        "scrollSize": 6000,
        "scrollWindow": "8s",
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