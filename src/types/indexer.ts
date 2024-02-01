import {StorageEosioAction, StorageEosioDelta} from './evm.js';
import {TxDeserializationError} from '../utils/evm.js';

export interface ElasticConnectorConfig {
    node: string;
    auth: {
        username: string;
        password: string;
    },
    requestTimeout: number,
    docsPerIndex: number,
    scrollSize?: number,
    scrollWindow?: string,
    numberOfShards: number,
    numberOfReplicas: number,
    refreshInterval: number,
    codec: string,
    dumpSize: number;
    suffix: {
        delta: string;
        error: string;
        transaction: string;
        fork: string;
    }
};

export interface ArrowConnectorConfig {
    dataDir: string;
    bucketSize?: number;
    dumpSize?: number;
}

export interface ChainConfig {
    chainName: string;
    chainId: number;
    startBlock: number;
    stopBlock: number;
    evmBlockDelta: number;
    evmPrevHash: string;
    evmValidateHash: string;
    irreversibleOnly: boolean;
}

export interface BroadcasterConfig {
    wsHost: string;
    wsPort: number;
};

export interface ConnectorConfig {
    chain?: Partial<ChainConfig>;
    elastic?: ElasticConnectorConfig;
    arrow?: ArrowConnectorConfig;

    logLevel?: string;
    trimFrom?: number;
    skipIntegrityCheck?: boolean;
    gapsPurge?: boolean;
}

export interface SourceConnectorConfig extends ConnectorConfig {
    chain: ChainConfig;
    nodeos?: {
        endpoint: string;
        remoteEndpoint: string;
        wsEndpoint: string;
        blockHistorySize: number;
        stallCounter: number;
        evmWorkerAmount: number;
        readerWorkerAmount: number;

        skipStartBlockCheck?: boolean;
        skipRemoteCheck?: boolean;
        maxMessagesInFlight?: number;
        maxWsPayloadMb?: number;
    }
}

export interface TranslatorConfig {
    source: SourceConnectorConfig;
    target: ConnectorConfig;

    // process config
    logLevel: string;
    readerLogLevel: string;
    runtime: {
        eval?: boolean;
        timeout?: number;
        onlyDBCheck?: boolean;
    };
    broadcast: BroadcasterConfig;
};

export const DEFAULT_CONF: TranslatorConfig = {
    "source": {
        "chain": {
            "chainName": "telos-local",
            "chainId": 41,
            "startBlock": 35,
            "stopBlock": 4294967295,
            "evmBlockDelta": 2,
            "evmPrevHash": "",
            "evmValidateHash": "",
            "irreversibleOnly": false,
        },
        "nodeos": {
            "endpoint": "http://127.0.0.1:8888",
            "remoteEndpoint": "http://127.0.0.1:8888",
            "wsEndpoint": "ws://127.0.0.1:29999",
            "blockHistorySize": (15 * 60 * 2),  // 15 minutes in blocks
            "stallCounter": 5,
            "readerWorkerAmount": 4,
            "evmWorkerAmount": 4,
        }
    },
    "target": {
        "elastic": {
            "node": "http://127.0.0.1:9200",
            "auth": {
                "username": "elastic",
                "password": "password"
            },
            "requestTimeout": 5 * 1000,
            "docsPerIndex": 10000000,
            "scrollSize": 6000,
            "scrollWindow": "1m",
            "numberOfShards": 1,
            "numberOfReplicas": 0,
            "refreshInterval": -1,
            "codec": "best-compression",
            "dumpSize": 2000,
            "suffix": {
                "delta": "delta-v1.5",
                "transaction": "action-v1.5",
                "error": "error-v1.5",
                "fork": "fork-v1.5"
            }
        }
    },

    "logLevel": "debug",
    "readerLogLevel": "info",
    "runtime": {},
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