const { Client } = require('@elastic/elasticsearch');

import * as elasticConfig from './config/elastic.json'; 

import { IndexerStateDocument } from './types/indexer';
import { AbiDocument } from './types/eosio';
import { EvmTransaction } from './types/evm';

import { getEvmTxHash, removeHexPrefix } from './utils/evm';


function prepareRawEvmTx(evmTx: EvmTransaction) {
    for (const attr in evmTx) {
        // @ts-ignore
        if (typeof evmTx[attr] === 'string') {
            // @ts-ignore
            evmTx[attr] = removeHexPrefix(evmTx[attr]);
        }
    }
    return evmTx;
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

    async indexAbiProposal(abiDoc: AbiDocument) {
        await this.elastic.index({
            index: 'abi-proposals',
            document: abiDoc
        });
    }

    async getAbiProposal(proposal_name: string) {
        const result = await this.elastic.search({
            index: 'abi-proposals',
            query: {
                match: {
                    proposal_name: proposal_name
                }
            }
        });
        return result.hits.hits;
    }

    async indexAbi(abiDoc: AbiDocument) {
        await this.elastic.index({
            index: 'abi',
            document: abiDoc
        });
    }

    async indexEvmTransaction(evmTx: EvmTransaction) {
        await this.elastic.index({
            id: getEvmTxHash(evmTx), 
            index: 'evm-transactions',
            body: {
                timestamp: new Date().toISOString(),
                raw: prepareRawEvmTx(evmTx)
            } 
        });
    }
};
