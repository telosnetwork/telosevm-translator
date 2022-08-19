import {TxDeserializationError} from '../handlers';
import {StorageEosioAction, StorageEosioDelta} from '../types/evm';

export type ConnectorConfig = {
    node: string;
    auth: {
        username: string;
        password: string;
    },
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
    debug: boolean;
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
    errors: TxDeserializationError[],
    delta: StorageEosioDelta;
    nativeHash: string;
    parentHash: string;
    transactionsRoot: string;
    receiptsRoot: string;
    blockBloom: string;
};
