const { Client, ApiResponse } = require('@elastic/elasticsearch');

import * as elasticConfig from '../config/elastic.json'; 

import { IndexerStateDocument } from '../types/indexer';

import { StorageEvmTransaction } from '../types/evm';

import logger from '../utils/winston';

import {dynamicImport} from 'tsimportlib';

const transactionIndexPrefix = "telos-net-action-v1-"


function getSubfixGivenBlock(blockNum: number) {
 return String(blockNum / 1000000).padStart(6, '0');
}

const chain = "telos-net";

export class ElasticConnector {
    elastic: typeof Client;

    constructor() {
        this.elastic = new Client(elasticConfig);
    }

    async init() {
        const indexConfig = await dynamicImport('./templates', module) as typeof import('./templates');

        const indicesList = [
            {name: "action", type: "action"},
            {name: "block", type: "block"},
            {name: "abi", type: "abi"},
            {name: "delta", type: "delta"},
            {name: "logs", type: "logs"},
            {name: 'permissionLink', type: 'link'},
            {name: 'permission', type: 'perm'},
            {name: 'resourceLimits', type: 'reslimits'},
            {name: 'resourceUsage', type: 'userres'},
            {name: 'generatedTransaction', type: 'gentrx'},
            {name: 'failedTransaction', type: 'trxerr'}
        ];

        logger.info(`Updating index templates for ${chain}...`);
        let updateCounter = 0;
        for (const index of indicesList) {
            try {
                if (indexConfig[index.name]) {
                    const creation_status: ApiResponse = await this.elastic['indices'].putTemplate({
                        name: `${chain}-${index.type}`,
                        body: indexConfig[index.name]
                    });
                    if (!creation_status || !creation_status['body']['acknowledged']) {
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
        return await this.elastic.search({
            index: 'indexer-state',
            size: 1,
            sort: [
                {"timestamp": { "order": "desc"} }
            ]
        });
    }

    async indexState(indexerState: IndexerStateDocument) {
        await this.elastic.index({
            index: 'indexer-state',
            body: indexerState 
        });
    }

    async indexTransactions(blockNum: number, transactions: StorageEvmTransaction[]) {
        const index = transactionIndexPrefix + getSubfixGivenBlock(blockNum)
        
        const operations = transactions.flatMap(
           doc => [{ index: { _index: index } }, doc]);
        
        const bulkResponse = await this.elastic.bulk({ refresh: true, operations })
        if (bulkResponse.errors)
            throw new Error(JSON.stringify(bulkResponse, null, 4));
    }
};
