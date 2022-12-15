import RPCBroadcaster from '../publisher.js';
import {ElasticIndex, IndexedBlockInfo, IndexerConfig, IndexerState} from '../types/indexer.js';
import {getTemplatesForChain} from './templates.js';

import logger from '../utils/winston.js';
import {Client, estypes} from '@elastic/elasticsearch';
import {StorageEosioDelta} from '../utils/evm.js';

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

        const lastIndex = indices.pop().index;
        try {
            const result = await this.elastic.search({
                index: lastIndex,
                size: 1,
                sort: [
                    {"block_num": {"order": "desc"}}
                ]
            });

            if (result?.hits?.hits?.length == 0)
                return null;

            return new StorageEosioDelta(result?.hits?.hits[0]?._source);

        } catch (error) {
            return null;
        }
    }

    async fullGapCheck(): Promise<number> {
        const lowerBoundDoc = await this.getFirstIndexedBlock();
        logger.warn(JSON.stringify(lowerBoundDoc, null, 4));

        if (lowerBoundDoc == null)
            return null;

        const lowerBound = lowerBoundDoc['@global'].block_num;

        const upperBoundDoc = await this.getLastIndexedBlock();
        if (upperBoundDoc == null)
            return null;

        const upperBound = upperBoundDoc['@global'].block_num;

        const gapCheck = async (
            lowerBound: number,
            upperBound: number,
            interval: number
        ) => {
            const results = await this.elastic.search<any, any>({
                index: `${this.config.chainName}-${this.config.elastic.subfix.delta}-*`,
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

            const len = results.aggregations.block_histogram.buckets.length;
            for (let i = 0; i < len; i++) {

                const bucket = results.aggregations.block_histogram.buckets[i];
                const lower = bucket.min_block.value;
                const upper = bucket.max_block.value;
                const total = bucket.doc_count;
                const totalRange = (upper - lower) + 1;
                let hasGap = total < totalRange;

                if (len > 1 && i < (len - 1)) {
                    const nextBucket = results.aggregations.block_histogram.buckets[i + 1];
                    hasGap = hasGap || (nextBucket.key - upper) != 1;
                }

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
        while (gap != null) {
            console.log(gap);
            lower += 1;
            gap = await gapCheck(lower, upper, 10);
        }
        lower -= 1;

        // now do the same to find the correct upper bound, bring it down one at
        // a time
        gap = await gapCheck(lower, upper, 10);
        while (gap != null) {
            console.log(gap);
            upper -= 1;
            gap = await gapCheck(lower, upper, 10);
        }
        upper += 1;

        console.log(`found gap at ${lower + 1}`);

        return lower;
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
            throw new Error(`Expected: ${this.lastPushed + 1} and got ${currentEvmBlock}`)

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
                this.writeBlocks(ops, blocks);
        }
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
    }

};
