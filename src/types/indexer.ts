export type IndexerStateDocument = {
    timestamp: string;
    lastIndexedBlock: number;
};

export type ConnectorConfig = {
    node: string;
    auth: {
        username: string;
        password: string;
    }
};

export type IndexerConfig = {
    endpoint: string;
    wsEndpoint: string;
    startBlock: number;
    stopBlock: number;
    perf: {
        workerAmount: number;
        maxMsgsInFlight: number;
    },
    elastic: ConnectorConfig;
};
