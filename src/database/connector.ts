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

    blockDrain: any[];
    opDrain: any[];

    writeCounter: number = 0;

    cleanupInProgress: boolean = false;
    isBroadcaster: boolean;

    constructor(config: IndexerConfig, isBroadcaster: boolean) {
        this.config = config;
        this.chainName = config.chainName;
        this.elastic = new Client(config.elastic);
        this.isBroadcaster = isBroadcaster;

        if (isBroadcaster)
            this.broadcast = new RPCBroadcaster(config.broadcast);

        this.opDrain = [];
        this.blockDrain = [];

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

    async checkGaps() {
        try {

            const result = await this.elastic.search({
                index: `${this.chainName}-${this.config.elastic.subfix.delta}-*`,
                size: 0,
                aggs: {
                    gaps: {
                        scripted_metric: {
                            init_script: "state.blocks = [];",
                            map_script: `
                                state.blocks.add(
                                    [doc["@global.block_num"].value, doc["block_num"].value]
                                );
                            `,
                            combine_script: `
                                def result = [];
                                for (blockInfo in state.blocks) { result.add(blockInfo); }
                                return result;
                            `,
                            reduce_script: `
                                def blocks = [];
                                for (b in states) {
                                    for (bInfo in b) {
                                    blocks.add(bInfo);
                                    }
                                }

                                blocks.sort((a,b)-> a[0].compareTo(b[0]));

                                def prevInfo = blocks.get(0);
                                def result = [];
                                def item = new HashMap();
                                for (blockInfo in blocks) {

                                    def gapSize = Math.abs(blockInfo[0] - prevInfo[0]);
                                    if (gapSize > 1) {
                                        item["gap_size"] = gapSize;
                                        item["to"] = blockInfo;
                                        item["from"] = prevInfo;
                                        result.add(item);
                                        item = new HashMap();
                                    }

                                    prevInfo = blockInfo;
                                }
                                return result;
                            `
                        }
                    }
                }
            });
            return result;
        } catch (error) {
            return null;
        }
    }

    async getLastIndexedBlock() {
        try {
            const result = await this.elastic.search({
                index: `${this.chainName}-${this.config.elastic.subfix.delta}-*`,
                size: 1,
                sort: [
                    {"block_num": { "order": "desc"} }
                ]
            });

            return result?.hits?.hits[0]?._source;

        } catch (error) {
            return null;
        }
    }

    async purgeBlocksNewerThan(blockNum: number) {
        const result = await this.elastic.deleteByQuery({
            index: `${this.chainName}-${this.config.elastic.subfix.delta}-*`,
            body: {
                query: {
                    range: {
                        block_num: {
                            gte: blockNum
                        }
                    }
                }
            },
            refresh: true
        })

        return result;
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

        this.opDrain = [...this.opDrain, ...operations];
        this.blockDrain.push(blockInfo);

        if (!this.cleanupInProgress &&
            (this.state == IndexerState.HEAD ||
             this.blockDrain.length >= this.config.perf.elasticDumpSize)) {

            const ops = this.opDrain;
            const blocks = this.blockDrain;

            this.opDrain = [];
            this.blockDrain = [];
            this.writeCounter++;
            setTimeout(
                this.drainBlocks.bind(this, ops, blocks), 0);
        }
    }

    async drainBlocks(ops: any[], blocks: any[]) {
        const bulkResponse = await this.elastic.bulk({
            refresh: true,
            operations: ops,
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
                        operation: ops[i * 2],
                        document: ops[i * 2 + 1]
                    })
                }
            });

            throw new Error(JSON.stringify(erroredDocuments, null, 4));
        }

        logger.info(`drained ${ops.length} operations.`);
        logger.info(`broadcasting ${blocks.length} blocks...`)

        for (const block of blocks)
            this.broadcast.broadcastBlock(block);

        logger.info('done.');

        this.writeCounter--;
    }

    async cleanupFork(blockNum: number) {
        // set fork cleanup flag, no new write tasks should start
        this.cleanupInProgress = true;

        // wait until `draining` flag is false, this means all data has been
        // wrote to db, we can begin cleanup
        while (this.writeCounter > 0)
            await new Promise(f => setTimeout(f, 1000));

        // search for specific block num and splice at that point
        let spliceIndex = this.opDrain.length - 1;
        while (spliceIndex < 0) {
            if ('block_num' in this.opDrain[spliceIndex] &&
               this.opDrain[spliceIndex].block_num < blockNum)
                break;
            spliceIndex--;
        }
        this.opDrain.splice(spliceIndex);

        // repeat process for block header queues
        spliceIndex = this.blockDrain.length - 1;
        while (spliceIndex < 0) {
            if (this.blockDrain[spliceIndex].delta.block_num < blockNum)
                break;
            spliceIndex--;
        }
        this.blockDrain.splice(spliceIndex);

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
