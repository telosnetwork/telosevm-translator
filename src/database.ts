const { Client } = require('@elastic/elasticsearch');

import * as elasticConfig from './config/elastic.json'; 

import { IndexerStateDocument } from './types/indexer';

import { StorageEvmTransaction } from './types/evm';


const transactionIndexPrefix = "telos-net-action-v1-"


function getSubfixGivenBlock(blockNum: number) {
 return String(blockNum / 1000000).padStart(6, '0');
}


export class ElasticConnector {
    elastic: typeof Client;

    constructor() {
        this.elastic = new Client(elasticConfig);
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
