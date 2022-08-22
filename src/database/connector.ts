const { Client, ApiResponse } = require('@elastic/elasticsearch');

import RPCBroadcaster from '../publisher';
import { IndexerConfig, IndexedBlockInfo, IndexerState } from '../types/indexer';
import { getTemplatesForChain } from './templates';

import logger from '../utils/winston';
import {BulkResponseItem} from '@elastic/elasticsearch/lib/api/types';

interface ConfigInterface {
    [key: string]: any;
};


export class Connector {
    config: IndexerConfig;
    elastic: typeof Client;
    broadcast: RPCBroadcaster;
    chainName: string;

    state: IndexerState;

    blockDrain: {
        done: any[];
        building: any[];
    };

    opDrain: {
        done: any[];
        building: any[];
    };

    draining: boolean = false;
    cleanupInProgress: boolean = false;
    isBroadcaster: boolean;

    constructor(config: IndexerConfig, isBroadcaster: boolean) {
        this.config = config;
        this.chainName = config.chainName;
        this.elastic = new Client(config.elastic);
        this.isBroadcaster = isBroadcaster;

        if (isBroadcaster)
            this.broadcast = new RPCBroadcaster(config.broadcast);

        this.opDrain = {
            done: [],
            building: []
        };

        this.blockDrain = {
            done: [],
            building: []
        };

        this.state = IndexerState.SYNC;
    }

    getSubfix(blockNum: number) {
        return String(Math.floor(blockNum / 10000000)).padStart(8, '0');
    }

    setState(state: IndexerState) {
        this.state = state;
    }

    async init() {
        const indexConfig: ConfigInterface = getTemplatesForChain(this.chainName);

        const indicesList = [
            {name: "action", type: "action"},
            {name: "delta", type: "delta"},
            {name: "error", type: "error"}
        ];

        logger.info(`Updating index templates for ${this.chainName}...`);
        let updateCounter = 0;
        for (const index of indicesList) {
            try {
                if (indexConfig[index.name]) {
                    const creation_status: typeof ApiResponse = await this.elastic['indices'].putTemplate({
                        name: `${this.chainName}-${index.type}`,
                        body: indexConfig[index.name]
                    });
                    if (!creation_status || !creation_status['acknowledged']) {
                        logger.error(`Failed to create template: ${this.chainName}-${index}`);
                    } else {
                        updateCounter++;
                        logger.info(`${this.chainName}-${index.type} template updated!`);
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

        logger.info('Initializing ws broadcaster...');

        if (this.isBroadcaster)
            this.broadcast.initUWS();
    }

    async getLastIndexedBlock() {
        try {
            const result = await this.elastic.search({
                index: `${this.chainName}-${this.config.elastic.subfix.delta}-*`,
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
        const txIndex = `${this.chainName}-${this.config.elastic.subfix.transaction}-${suffix}`;
        const dtIndex = `${this.chainName}-${this.config.elastic.subfix.delta}-${suffix}`;
        const errIndex = `${this.chainName}-${this.config.elastic.subfix.error}-${suffix}`; 
        
        const txOperations = blockInfo.transactions.flatMap(
           doc => [{index: {_index: txIndex}}, doc]);

        const errOperations = blockInfo.errors.flatMap(
           doc => [{index: {_index: errIndex}}, doc]);

        const operations = [
            ...errOperations,
            ...txOperations,
            {index: {_index: dtIndex}}, blockInfo.delta
        ];

        this.opDrain.building = [...this.opDrain.building, ...operations];
        this.blockDrain.building.push(blockInfo);

        if (!this.draining &&
            !this.cleanupInProgress &&
            (this.state == IndexerState.HEAD ||
             this.opDrain.building.length >= this.config.perf.elasticDumpSize)) {

            this.opDrain.done = this.opDrain.building;
            this.opDrain.building = [];

            this.blockDrain.done = this.blockDrain.building;
            this.blockDrain.building = [];

            this.draining = true;
            setTimeout(
                this.drainBlocks.bind(this), 0);
        }
    }

    async drainBlocks() {
        const bulkResponse = await this.elastic.bulk({
            refresh: true,
            operations: this.opDrain.done,
            error_trace: true
        });

        if (bulkResponse.errors) {
            const erroredDocuments: any[] = []
            // The items array has the same order of the dataset we just indexed.
            // The presence of the `error` key indicates that the operation
            // that we did for the document has failed.
            bulkResponse.items.forEach((
                action: BulkResponseItem, i: number) => {
                const operation = Object.keys(action)[0]
                // @ts-ignore
                if (action[operation].error) {
                    erroredDocuments.push({
                        // If the status is 429 it means that you can retry the document,
                        // otherwise it's very likely a mapping error, and you should
                        // fix the document before to try it again.
                        // @ts-ignore
                        status: action[operation].status,
                        // @ts-ignore
                        error: action[operation].error,
                        operation: this.opDrain.done[i * 2],
                        document: this.opDrain.done[i * 2 + 1]
                    })
                }
            });

            throw new Error(JSON.stringify(erroredDocuments, null, 4));
        }

        logger.info(`drained ${this.opDrain.done.length} operations.`);
        logger.info(`broadcasting ${this.blockDrain.done.length} blocks...`)

        for (const block of this.blockDrain.done)
            this.broadcast.broadcastBlock(block);

        logger.info('done.');

        this.draining = false;
    }

    async cleanupFork(blockNum: number) {
        // set fork cleanup flag, no new write tasks should start
        this.cleanupInProgress = true;

        // wait until `draining` flag is false, this means all data has been
        // wrote to db, we can begin cleanup
        while (this.draining)
            await new Promise(f => setTimeout(f, 1000));

        // cleanup operation queues
        // nuke done queue
        this.opDrain.done.splice(0);

        // search for specific block num and splice at that point
        let spliceIndex = this.opDrain.building.length - 1;
        while (spliceIndex < 0) {
            if ('block_num' in this.opDrain.building[spliceIndex] &&
               this.opDrain.building[spliceIndex].block_num < blockNum)
                break;
            spliceIndex--;
        }
        this.opDrain.building.splice(spliceIndex);

        // repeat process for block header queues
        this.blockDrain.done.splice(0);
        spliceIndex = this.blockDrain.building.length - 1;
        while (spliceIndex < 0) {
            if (this.blockDrain.building[spliceIndex].delta.block_num < blockNum)
                break;
            spliceIndex--;
        }
        this.blockDrain.building.splice(spliceIndex);

        // finally clean up db
        
        // deltas
        let resp = await this.elastic.deleteByQuery({
            index: `${this.chainName}-${this.config.elastic.subfix.delta}-*`,
            q: `block_num >= ${blockNum}`
        });
        logger.info('delta: \n' + JSON.stringify(resp, null, 4));

        // actions 
        resp = await this.elastic.deleteByQuery({
            index: `${this.chainName}-${this.config.elastic.subfix.transaction}-*`,
            q: `@raw.block >= ${blockNum - this.config.evmDelta}`
        });
        logger.info('action: \n' + JSON.stringify(resp, null, 4));

        // clear fork cleanup flag, new write tasks should be created
        this.cleanupInProgress = false;
    }
};
