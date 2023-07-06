import RPCBroadcaster from '../publisher.js';
import {IndexedBlockInfo, IndexerConfig, IndexerState} from '../types/indexer.js';
import {getTemplatesForChain} from './templates.js';

import logger from '../utils/winston.js';
import {Client, estypes} from '@elastic/elasticsearch';
import {StorageEosioDelta} from '../utils/evm.js';
import { StorageEosioAction } from '../types/evm.js';

interface ConfigInterface {
    [key: string]: any;
};

function getSuffix(blockNum: number, docsPerIndex: number) {
    return String(Math.floor(blockNum / docsPerIndex)).padStart(8, '0');
}

function indexToSuffixNum(index: string) {
    const spltIndex = index.split('-');
    const suffix = spltIndex[spltIndex.length - 1];
    return parseInt(suffix);
}

export class Connector {
    config: IndexerConfig;
    elastic: Client;
    broadcast: RPCBroadcaster;
    chainName: string;

    state: IndexerState;

    blockDrain: any[];
    opDrain: any[];

    totalPushed: number = 0;
    lastPushed: number = 0;

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
                    const creation_status: estypes.IndicesPutTemplateResponse = await this.elastic['indices'].putTemplate({
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

    async getOrderedDeltaIndices() {
        const deltaIndices: estypes.CatIndicesResponse = await this.elastic.cat.indices({
            index: `${this.chainName}-${this.config.elastic.subfix.delta}-*`,
            format: 'json'
        });
        deltaIndices.sort((a, b) => {
            const aNum = indexToSuffixNum(a.index);
            const bNum = indexToSuffixNum(b.index);
            if (aNum < bNum)
                return -1;
            if (aNum > bNum)
                return 1;
            return 0;
        });
        return deltaIndices;
    }

    async getIndexedBlock(blockNum: number) {
        const suffix = getSuffix(blockNum, this.config.elastic.docsPerIndex);
        try {
            const result = await this.elastic.search({
                index: `${this.chainName}-${this.config.elastic.subfix.delta}-${suffix}`,
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
        const suffix = getSuffix(blockNum, this.config.elastic.docsPerIndex);
        const sufNum = indexToSuffixNum(suffix);
        const indices: Array<string> = (await this.getOrderedDeltaIndices())
            .filter((val) =>
                Math.abs(indexToSuffixNum(val.index) - sufNum) <= 1)
            .map((val) => val.index);

        try {
            const result = await this.elastic.search({
                index: indices,
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
        const indices = await this.getOrderedDeltaIndices();

        if (indices.length == 0)
            return null;

        const firstIndex = indices.shift().index;
        try {
            const result = await this.elastic.search({
                index: firstIndex,
                size: 1,
                sort: [
                    {"block_num": {"order": "asc"}}
                ]
            });

            if (result?.hits?.hits?.length == 0)
                return null;

            return new StorageEosioDelta(result?.hits?.hits[0]?._source);

        } catch (error) {
            return null;
        }
    }

    async getLastIndexedBlock() {
        const indices = await this.getOrderedDeltaIndices();
        if (indices.length == 0)
            return null;

        for (let i = indices.length - 1; i >= 0; i--) {
            const lastIndex = indices[i].index;
            try {
                const result = await this.elastic.search({
                    index: lastIndex,
                    size: 1,
                    sort: [
                        {"block_num": {"order": "desc"}}
                    ]
                });

                logger.debug(`getLastIndexedBlock:\n${JSON.stringify(result, null, 4)}`);

                if (result?.hits?.hits?.length == 0)
                    continue;

                return new StorageEosioDelta(result?.hits?.hits[0]?._source);

            } catch (error) {
                logger.error(error);
                throw error;
            }
        }

        return null;
    }

    async findGapInIndices() {
        const deltaIndices = await this.getOrderedDeltaIndices();
        logger.debug('delta indices: ');
        logger.debug(JSON.stringify(deltaIndices, null, 4))
        for(let i = 1; i < deltaIndices.length; i++) {
            const previousIndexSuffixNum = indexToSuffixNum(deltaIndices[i-1].index);
            const currentIndexSuffixNum = indexToSuffixNum(deltaIndices[i].index);

            if(currentIndexSuffixNum - previousIndexSuffixNum > 1) {
                return {
                    gapStart: previousIndexSuffixNum,
                    gapEnd: currentIndexSuffixNum
                };
            }
        }

        // Return null if no gaps found
        return null;
    }

    async runHistogramGapCheck(lower: number, upper: number, interval: number): Promise<any> {
        const results = await this.elastic.search<any, any>({
            index: `${this.config.chainName}-${this.config.elastic.subfix.delta}-*`,
            size: 0,
            body: {
                query: {
                    range: {
                        "@global.block_num": {
                            gte: lower,
                            lte: upper
                        }
                    }
                },
                aggs: {
                    "block_histogram": {
                        "histogram": {
                            "field": "@global.block_num",
                            "interval": interval,
                            "min_doc_count": 0
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
                }
            }
        });

        const buckets = results.aggregations.block_histogram.buckets;

        logger.debug(`runHistogramGapCheck: ${lower}-${upper}, interval: ${interval}`);
        logger.debug(JSON.stringify(buckets, null, 4));

        return buckets;
    }

    async findDuplicateDeltas(lower: number, upper: number): Promise<number[]> {
        const results = await this.elastic.search<any, any>({
            index: `${this.config.chainName}-${this.config.elastic.subfix.delta}-*`,
            size: 0,
            body: {
                query: {
                    range: {
                        "@global.block_num": {
                            gte: lower,
                            lte: upper
                        }
                    }
                },
                aggs: {
                    "duplicate_blocks": {
                        "terms": {
                            "field": "@global.block_num",
                            "min_doc_count": 2,
                            "size": 100
                        }
                    }
                }
            }
        });

	if (results.aggregations) {

            const buckets = results.aggregations.duplicate_blocks.buckets;

            logger.debug(`findDuplicateDeltas: ${lower}-${upper}`);

            return buckets.map(bucket => bucket.key); // Return the block numbers with duplicates

	} else {
	    return [];
	}
    }

    async findDuplicateActions(lower: number, upper: number): Promise<number[]> {
        const results = await this.elastic.search<any, any>({
            index: `${this.config.chainName}-${this.config.elastic.subfix.transaction}-*`,
            size: 0,
            body: {
                query: {
                    range: {
                        "@raw.block": {
                            gte: lower,
                            lte: upper
                        }
                    }
                },
                aggs: {
                    "duplicate_txs": {
                        "terms": {
                            "field": "@raw.hash",
                            "min_doc_count": 2,
                            "size": 100
                        }
                    }
                }
            }
        });

	if (results.aggregations) {

            const buckets = results.aggregations.duplicate_txs.buckets;

            logger.debug(`findDuplicateActions: ${lower}-${upper}`);

            return buckets.map(bucket => bucket.key); // Return the block numbers with duplicates

	} else {
	    return [];
	}
    }

    async checkGaps(lowerBound: number, upperBound: number, interval: number): Promise<number> {
        interval = Math.ceil(interval);

        // Base case
        if (interval == 1) {
            return lowerBound;
        }

        const middle = Math.floor((upperBound + lowerBound) / 2);

        logger.debug(`calculated middle ${middle}`);

        logger.debug('first half');
        // Recurse on the first half
        const lowerBuckets = await this.runHistogramGapCheck(lowerBound, middle, interval / 2);
        if (lowerBuckets.length === 0) {
            return middle; // Gap detected
        } else if (lowerBuckets[lowerBuckets.length - 1].max_block.value < middle) {
            const lowerGap = await this.checkGaps(lowerBound, middle, interval / 2);
            if (lowerGap)
                return lowerGap;
        }

        logger.debug('second half');
        // Recurse on the second half
        const upperBuckets = await this.runHistogramGapCheck(middle + 1, upperBound, interval / 2);
        if (upperBuckets.length === 0) {
            return middle + 1; // Gap detected
        } else if (upperBuckets[0].min_block.value > middle + 1) {
            const upperGap = await this.checkGaps(middle + 1, upperBound, interval / 2);
            if (upperGap)
                return upperGap;
        }

        // Check for gap between the halves
        if ((lowerBuckets[lowerBuckets.length - 1].max_block.value + 1) < upperBuckets[0].min_block.value) {
            return lowerBuckets[lowerBuckets.length - 1].max_block.value;
        }

        // Find gaps inside bucket by doc_count
        const buckets = [...lowerBuckets, ...upperBuckets];
        for (let i = 0; i < buckets.length; i++) {
            if (buckets[i].doc_count != (buckets[i].max_block.value - buckets[i].min_block.value) + 1) {
                const insideGap = await this.checkGaps(buckets[i].min_block.value, buckets[i].max_block.value, interval / 2);
                if (insideGap)
                    return insideGap;
            }
        }

        // No gap found
        return null;
    }

    async fullIntegrityCheck(): Promise<number> {
        const lowerBoundDoc = await this.getFirstIndexedBlock();
        const upperBoundDoc = await this.getLastIndexedBlock();

        if (!lowerBoundDoc || !upperBoundDoc) {
            return null;
        }

        const lowerBound = lowerBoundDoc['@global'].block_num;
        const upperBound = upperBoundDoc['@global'].block_num;

        // check duplicates
        const deltaDuplicates = await this.findDuplicateDeltas(lowerBound, upperBound);
        if (deltaDuplicates.length > 0)
            logger.error(`block duplicates found: ${JSON.stringify(deltaDuplicates)}`);

        const actionDuplicates = await this.findDuplicateActions(lowerBound, upperBound);
        if (actionDuplicates.length > 0)
            logger.error(`tx duplicates found: ${JSON.stringify(actionDuplicates)}`);

        if (deltaDuplicates.length + actionDuplicates.length > 0)
            throw new Error('Duplicates found!')

	if (upperBound - lowerBound < 2)
	    return null;

        // first just check if whole indices are missing
        const gap = await this.findGapInIndices();
        if (gap) {
            logger.debug(`whole index seems to be missing `);
            const lower = gap.gapStart * this.config.elastic.docsPerIndex;
            const upper = (gap.gapStart + 1) * this.config.elastic.docsPerIndex;
            const agg = await this.runHistogramGapCheck(
                lower, upper, this.config.elastic.docsPerIndex)
            return agg[0].max_block.value;
        }

        const initialInterval = upperBound - lowerBound;

        logger.info(`starting full gap check from ${lowerBound} to ${upperBound}`);

        return this.checkGaps(lowerBound, upperBound, initialInterval);
    }

    async _purgeBlocksNewerThan(blockNum: number, evmBlockNum: number) {
        const targetSuffix = getSuffix(blockNum, this.config.elastic.docsPerIndex);
        const deltaIndex = `${this.chainName}-${this.config.elastic.subfix.delta}-${targetSuffix}`;
        const actionIndex = `${this.chainName}-${this.config.elastic.subfix.transaction}-${targetSuffix}`;
        try {
            const deltaResult = await this.elastic.deleteByQuery({
                index: deltaIndex,
                body: {
                    query: {
                        range: {
                            block_num: {
                                gte: blockNum
                            }
                        }
                    }
                },
                conflicts: 'proceed',
                refresh: true,
                error_trace: true
            });
            logger.debug(`delta delete result: ${JSON.stringify(deltaResult, null, 4)}`);
        } catch (e) {
            if (e.name != 'ResponseError' ||
                e.meta.body.error.type != 'index_not_found_exception')
                throw e;
        }
        try {
            const actionResult = await this.elastic.deleteByQuery({
                index: actionIndex,
                body: {
                    query: {
                        range: {
                            '@raw.block': {
                                gte: evmBlockNum
                            }
                        }
                    }
                },
                conflicts: 'proceed',
                refresh: true,
                error_trace: true
            });
            logger.debug(`action delete result: ${JSON.stringify(actionResult, null, 4)}`);
        } catch (e) {
            if (e.name != 'ResponseError' ||
                e.meta.body.error.type != 'index_not_found_exception')
                throw e;
        }
    }

    async _purgeIndicesNewerThan(blockNum: number) {
        logger.info(`purging indices in db from block ${blockNum}...`);
        const targetSuffix = getSuffix(blockNum, this.config.elastic.docsPerIndex);
        const targetNum = parseInt(targetSuffix);

        const deleteList = [];

        const deltaIndices = await this.elastic.cat.indices({
            index: `${this.config.chainName}-${this.config.elastic.subfix.delta}-*`,
            format: 'json'
        });

        for (const deltaIndex of deltaIndices)
            if (indexToSuffixNum(deltaIndex.index) > targetNum)
                deleteList.push(deltaIndex.index);

        const actionIndices = await this.elastic.cat.indices({
            index: `${this.config.chainName}-${this.config.elastic.subfix.transaction}-*`,
            format: 'json'
        });

        for (const actionIndex of actionIndices)
            if (indexToSuffixNum(actionIndex.index) > targetNum)
                deleteList.push(actionIndex.index);

        if (deleteList.length > 0) {
            const deleteResult = await this.elastic.indices.delete({
                index: deleteList
            });
            logger.info(`deleted indices result: ${JSON.stringify(deleteResult, null, 4)}`);
        }

        return deleteList;
    }

    async purgeNewerThan(blockNum: number, evmBlockNum: number) {
        await this._purgeIndicesNewerThan(blockNum);
        await this._purgeBlocksNewerThan(blockNum, evmBlockNum);
    }

    async pushBlock(blockInfo: IndexedBlockInfo) {
        const currentEvmBlock = blockInfo.delta['@global'].block_num;
        if (this.totalPushed != 0 && currentEvmBlock != this.lastPushed + 1)
            throw new Error(`Expected: ${this.lastPushed + 1} and got ${currentEvmBlock}`);

        const suffix = getSuffix(blockInfo.delta.block_num, this.config.elastic.docsPerIndex);
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

        this.lastPushed = currentEvmBlock;
        this.totalPushed++;

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
                void this.writeBlocks(ops, blocks);
        }
    }

    forkCleanup(
        timestamp: string,
        lastNonForkedEvm: number,
        lastNonForked: number,
        lastForked: number
    ) {
        // fix state flag
        this.lastPushed = lastNonForkedEvm - 1;

        // clear elastic operations drain
        let i = this.opDrain.length;
        while (i > 0) {
            const op = this.opDrain[i];
            if (op && (Object.getPrototypeOf(op) == StorageEosioDelta.prototype ||
                Object.getPrototypeOf(op) == StorageEosioAction.prototype) &&
                op.block_num > lastNonForked) {
                this.opDrain.splice(i - 1); // delete indexing info
                this.opDrain.splice(i);     // delete the document
            }
            i--;
        }

        // clear broadcast queue
        i = this.blockDrain.length;
        while (i > 0) {
            const block = this.blockDrain[i];
            if (block && block.delta.block_num > lastNonForked) {
                this.blockDrain.splice(i);
            }
            i--;
        }

        // write information about fork event
        const suffix = getSuffix(lastNonForked, this.config.elastic.docsPerIndex);
        const frkIndex = `${this.chainName}-${this.config.elastic.subfix.fork}-${suffix}`;
        this.opDrain.push({index: {_index: frkIndex}});
        this.opDrain.push({timestamp, lastNonForked, lastForked});
    }

    async writeBlocks(ops: any[], blocks: any[]) {
        const bulkResponse = await this.elastic.bulk<any, any>({
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
                action: Partial<Record<estypes.BulkOperationType, estypes.BulkResponseItem>>, i: number) => {
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

        if (global.gc) {global.gc();}
    }

};
