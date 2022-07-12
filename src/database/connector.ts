const { Client, ApiResponse } = require('@elastic/elasticsearch');

import { ConnectorConfig, IndexedBlockInfo } from '../types/indexer';

import logger from '../utils/winston';

const transactionIndexPrefix = "-action-v1-"
const deltaIndexPrefix = "-delta-v1-"

const chain = "telos-net";

interface ConfigInterface {
    [key: string]: any;
};


export class ElasticConnector {
    elastic: typeof Client;
    chainName: string;
    blockDrain: {
        done: any[];
        building: any[];
    };

    constructor(chainName: string, config: ConnectorConfig) {
        this.chainName = chainName;
        this.elastic = new Client(config);

        this.blockDrain = {
            done: [],
            building: []
        };
    }

    getSubfix(blockNum: number) {
        return String(Math.floor(blockNum / 1000000)).padStart(7, '0');
    }

    async init() {
        const indexConfig: ConfigInterface = await import('./templates');

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

    async getLastIndexedBlock() {
        try {
            const index = this.chainName + deltaIndexPrefix + '*';
            const result = await this.elastic.search({
                index: index,
                size: 1,
                sort: [
                    {"@timestamp": { "order": "desc"} }
                ]
            });

            return result?.hits?.hits[0]?._source; 

        } catch (error) {
            return null;
        }
    }

    pushBlock(blockInfo: IndexedBlockInfo) {
        const suffix = this.getSubfix(blockInfo.delta.block_num);
        const txIndex = this.chainName + transactionIndexPrefix + suffix;
        const dtIndex = this.chainName + deltaIndexPrefix + suffix;
        
        const txOperations = blockInfo.transactions.flatMap(
           doc => [{index: {_index: txIndex}}, doc]);

        const operations = [...txOperations, {index: {_index: dtIndex}}, blockInfo.delta];

        this.blockDrain.building = [...this.blockDrain.building, ...operations];

        if (this.blockDrain.building.length >= 4096) {
            this.blockDrain.done = this.blockDrain.building;
            this.blockDrain.building = [];

            setTimeout(
                this.drainBlocks.bind(this), 0);
        }
    }

    async drainBlocks() {
        const bulkResponse = await this.elastic.bulk({
            refresh: true,
            operations: this.blockDrain.done
        });

        if (bulkResponse.errors)
            throw new Error(JSON.stringify(bulkResponse, null, 4));

        logger.info(`drained ${this.blockDrain.done.length} operations.`);
    }
};
