import {StorageEosioAction, StorageEosioDelta} from '../types/evm';

export type ConnectorConfig = {
    node: string;
    auth: {
        username: string;
        password: string;
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
    perf: {
        workerAmount: number;
        elasticDumpSize: number;
        maxMsgsInFlight: number;
    },
    elastic: ConnectorConfig;
    broadcast: BroadcasterConfig;
};


export type IndexedBlockInfo = {
    transactions: StorageEosioAction[];
    delta: StorageEosioDelta;
    nativeHash: string;
    parentHash: string;
    transactionsRoot: string;
    receiptsRoot: string;
    blockBloom: string;
};
