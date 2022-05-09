const { Client } = require('@elastic/elasticsearch');

import * as elasticConfig from './config/elastic.json'; 

import { AbiDocument } from './types/eosio';
import { EvmTransaction } from './types/evm';


export class ElasticConnector {
    elastic: typeof Client;
    i: number;

    constructor() {
        this.elastic = new Client(elasticConfig);
        this.i = 0;
    }

    async indexAbi(abiDoc: AbiDocument) {
        await this.elastic.index({
            index: 'abis',
            document: abiDoc
        });
    }

    async indexEvmTransaction(evmTx: EvmTransaction) {
        await this.elastic.index({
            id: this.i, 
            index: 'evm-transactions',
            body: {
                timestamp: new Date().toISOString(),
                raw: evmTx
            }
        });
        this.i++;
    }
};
