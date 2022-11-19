const { Client, ApiResponse } = require('@elastic/elasticsearch');

import RPCBroadcaster from '../publisher';
import { IndexerConfig, IndexedBlockInfo, IndexerState } from '../types/indexer';
import { getTemplatesForChain } from './templates';

import logger from '../utils/winston';
import {BulkResponseItem} from '@elastic/elasticsearch/lib/api/types';
import {StorageEosioDelta} from '../utils/evm';

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

    constructor(config: IndexerConfig) {
        this.config = config;
        this.chainName = config.chainName;
        this.elastic = new Client(config.elastic);

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

        this.broadcast.initUWS();
    }

    async getIndexedBlock(blockNum: number) {
        try {
            const result = await this.elastic.search({
                index: `${this.chainName}-${this.config.elastic.subfix.delta}-*`,
                query: {
                    match: {
                        block_num: {
                            query: blockNum
                        }
                    }
                }
            });

            return new StorageEosioDelta(result?.hits?.hits[0]?._source);

        } catch (error) {
            return null;
        }
    }

    async getIndexedBlockEVM(blockNum: number) {
        try {
            const result = await this.elastic.search({
                index: `${this.chainName}-${this.config.elastic.subfix.delta}-*`,
                query: {
                    match: {
                        '@global.block_num': {
                            query: blockNum
                        }
                    }
                }
            });

            return new StorageEosioDelta(result?.hits?.hits[0]?._source);

        } catch (error) {
            return null;
        }
    }

    async getFirstIndexedBlock() {
        try {
            const result = await this.elastic.search({
                index: `${this.chainName}-${this.config.elastic.subfix.delta}-*`,
                size: 1,
                sort: [
                    {"block_num": { "order": "asc"} }
                ]
            });

            return new StorageEosioDelta(result?.hits?.hits[0]?._source);

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

            return new StorageEosioDelta(result?.hits?.hits[0]?._source);

        } catch (error) {
            return null;
        }
    }

    async fullGapCheck() : Promise<number> {
        const lowerBound = (await this.getFirstIndexedBlock())['@global'].block_num;
        const upperBound = (await this.getLastIndexedBlock())['@global'].block_num;
        const gapCheck = async (
            lowerBound: number,
            upperBound: number,
            interval: number
        ) => {
            const results = await this.elastic.search({
                index: `${this.chainName}-${this.config.elastic.subfix.delta}-*`,
                aggs: {
                    "block_histogram": {
                        "histogram": {
                            "field": "@global.block_num",
                            "interval": interval,
                            "min_doc_count": 1
                        },
                        "aggs": {
                            "min_block": {
                                "min": {
                                    "field": "@global.block_num"
                                }
                            },
                            "max_block": {
                                "max": {
                                    "field": "@global.block_num"
                                }
                            }
                        }
                    }
                },
                size: 0,
                query: {
                    "bool": {
                        "must": [
                            {
                                "range": {
                                    "@global.block_num": {
                                        "gte": lowerBound,
                                        "lte": upperBound
                                    }
                                }
                            }
                        ]
                    }
                }
            });

            for (const bucket of results.aggregations.block_histogram.buckets) {
                const lower = bucket.min_block.value;
                const upper = bucket.max_block.value;
                const total = bucket.doc_count;
                const totalRange = (upper - lower) + 1;
                const hasGap = totalRange != total;

                if (hasGap)
                    return [lower, upper];
            }
            return null;
        }
        let interval = 10000000;
        let gap: Array<number> = [lowerBound, upperBound];

        // run gap check routine with smaller and smaller intervals each time
        while (interval >= 10 && gap != null) {
            gap = await gapCheck(gap[0], gap[1], interval);
            console.log(`checked ${JSON.stringify(gap)} with interval ${interval}, ${gap}`);
            interval /= 10;
        }

        // if gap is null now there are no gaps
        if (gap == null)
            return null;

        // at this point we have the gap located between a range no more than 10
        // blocks in length, plan is to move lower bound up until we miss the gap
        // then we know gap starts from previous lower bound
        let lower = gap[0];
        let upper = gap[1];
        gap = await gapCheck(lower, upper, 10);
        while(gap != null) {
            console.log(gap);
            lower += 1;
            gap = await gapCheck(lower, upper, 10);
        }
        lower -= 1;

        // now do the same to find the correct upper bound, bring it down one at
        // a time
        gap = await gapCheck(lower, upper, 10);
        while(gap != null) {
            console.log(gap);
            upper -= 1;
            gap = await gapCheck(lower, upper, 10);
        }
        upper += 1;

        console.log(`found gap at ${lower + 1}`);

        return lower;
    }

    async purgeNewerThan(blockNum: number, evmBlockNum: number) {
        const deltaResult = await this.elastic.deleteByQuery({
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
            refresh: true,
            error_trace: true
        });
        const actionResult = await this.elastic.deleteByQuery({
            index: `${this.chainName}-${this.config.elastic.subfix.transaction}-*`,
            body: {
                query: {
                    range: {
                        '@raw.block': {
                            gte: evmBlockNum
                        }
                    }
                }
            },
            refresh: true,
            error_trace: true
        });

        if (deltaResult.errors)
            throw new Error(JSON.stringify(deltaResult, null, 4));

        if (actionResult.errors)
            throw new Error(JSON.stringify(actionResult, null, 4));

        return { deltaResult, actionResult };
    }

    async pushBlock(blockInfo: IndexedBlockInfo) {
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

        if (this.state == IndexerState.HEAD ||
             this.blockDrain.length >= this.config.perf.elasticDumpSize) {

            const ops = this.opDrain;
            const blocks = this.blockDrain;

            this.opDrain = [];
            this.blockDrain = [];
            this.writeCounter++;

            if (this.state == IndexerState.HEAD)
                await this.writeBlocks(ops, blocks);
            else
                this.writeBlocks(ops, blocks);
        }
    }

    async writeBlocks(ops: any[], blocks: any[]) {
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

};
