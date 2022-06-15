const { Client, ApiResponse } = require('@elastic/elasticsearch');

import { IndexerStateDocument, ConnectorConfig } from '../types/indexer';

import { StorageEosioAction, StorageEosioDelta } from '../types/evm';

import logger from '../utils/winston';

const transactionIndexPrefix = "telos-net-action-v1-"
const deltaIndexPrefix = "telos-net-delta-v1-"

const chain = "telos-net";

interface ConfigInterace {
    [key: string]: any;
};


export class ElasticConnector {
    elastic: typeof Client;
    totalIndexedBlocks: number;

    constructor(config: ConnectorConfig) {
        this.elastic = new Client(config);
        this.totalIndexedBlocks = 0;
    }

    getSubfix() {
        return String(Math.floor(this.totalIndexedBlocks / 1000000)).padStart(7, '0');
    }

    async init() {
        const indexConfig: ConfigInterace = await import('./templates');

        const indicesList = [
            {name: "action", type: "action"},
            {name: "delta", type: "delta"}
        ];

        logger.info(`Updating index templates for ${chain}...`);
        let updateCounter = 0;
        for (const index of indicesList) {
            try {
                if (indexConfig[index.name]) {
                    const creation_status: typeof ApiResponse = await this.elastic['indices'].putTemplate({
                        name: `${chain}-${index.type}`,
                        body: indexConfig[index.name]
                    });
                    if (!creation_status || !creation_status['acknowledged']) {
                        logger.error(`Failed to create template: ${chain}-${index}`);
                    } else {
                        updateCounter++;
                        logger.info(`${chain}-${index.type} template updated!`);
                    }
                } else {
                    logger.warn(`${index.name} template not found!`);
                }
            } catch (e) {
                logger.error(`[FATAL] ${e.message}`);
                if (e.meta) {
                    logger.error(e.meta.body);
                }
                process.exit(1);
            }
        }
        logger.info(`${updateCounter} index templates updated`);
    }

    async getIndexerState() {
        try {
            const resp = await this.elastic.search({
                index: 'indexer-state',
                size: 1,
                sort: [
                    {"timestamp": { "order": "desc"} }
                ]
            });

            return resp.hits.hits._source;
        } catch (error) {
            return null;
        }
    }

    async indexState(indexerState: IndexerStateDocument) {
        await this.elastic.index({
            index: 'indexer-state',
            body: indexerState 
        });
    }

    async indexBlock(
        blockNum: number,
        transactions: StorageEosioAction[],
        delta: StorageEosioDelta
    ) {
        const suffix = this.getSubfix();
        const txIndex = transactionIndexPrefix + suffix;
        const dtIndex = deltaIndexPrefix + suffix;
        
        const txOperations = transactions.flatMap(
           doc => [{index: {_index: txIndex}}, doc]);

        const operations = [...txOperations, {index: {_index: dtIndex}}, delta];
        
        const bulkResponse = await this.elastic.bulk({ refresh: true, operations })
        if (bulkResponse.errors)
            throw new Error(JSON.stringify(bulkResponse, null, 4));

        this.totalIndexedBlocks++;
    }
};
