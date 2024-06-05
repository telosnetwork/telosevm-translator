import RPCBroadcaster from '../publisher.js';
import {IndexedBlockInfo, IndexerConfig, IndexerState} from '../types/indexer.js';
import {getTemplatesForChain} from './templates.js';

import {Client, estypes} from '@elastic/elasticsearch';
import {
    isStorableDocument, StorageEosioAction, StorageEosioActionSchema,
    StorageEosioDelta,
    StorageEosioDeltaSchema, StorageEosioGenesisDeltaSchema
} from '../types/evm.js';
import {Logger} from "winston";
import EventEmitter from "events";


interface ConfigInterface {
    [key: string]: any;
};

function indexToSuffixNum(index: string) {
    const spltIndex = index.split('-');
    const suffix = spltIndex[spltIndex.length - 1];
    return parseInt(suffix);
}

export interface ScrollOptions {
    fields?: string[];
    scroll?: string;
    size?: number;
}

export interface BlockData {block: StorageEosioDelta, actions: StorageEosioAction[]};

export interface ScrollResult {minBlock: number, maxBlock: number, data: BlockData[]};

export class BlockScroller {

    get isInit(): boolean {
        return this._isInit;
    }
    get isDone(): boolean {
        return this._isDone;
    }

    private readonly conn: Connector;
    private readonly from: number;
    private readonly to: number;
    private readonly validate: boolean;
    private readonly scrollOpts: ScrollOptions;
    private readonly scrollSize: number;
    readonly tag: string;

    private last: number;
    private lastYielded: number;
    private lastBlockTx: number;

    private currentDeltaScrollId: string;
    private currentActionScrollId: string;

    private currentDeltaSuff: number;
    private currentActionSuff: number;
    private deltaIndices: string[];
    private actionIndices: string[];

    private _isDone: boolean = false;
    private _isInit: boolean = false;

    private range: BlockData[] = [];
    private rangeTxs: StorageEosioAction[] = [];

    constructor(
        connector: Connector,
        params: {
            from: number,
            to: number,
            validate?: boolean,
            scrollOpts?: ScrollOptions,
            tag?: string
        }
    ) {
        this.conn = connector;
        this.from = params.from;
        this.to = params.to;
        this.scrollOpts = params.scrollOpts ? params.scrollOpts : {};
        this.validate = params.validate ? params.validate : false;
        this.tag = params.tag ? params.tag : '';

        this.lastYielded = this.from - 1;
        this.lastBlockTx = this.lastYielded - this.conn.config.evmBlockDelta;

        this.scrollSize = this.scrollOpts.size ? this.scrollOpts.size : 1000;
    }

    private unwrapDeltaHit(hit): StorageEosioDelta {
        const doc = hit._source;
        if (this.validate)
            return StorageEosioGenesisDeltaSchema.parse(doc);

        return doc
    }

    private unwrapActionHit(hit): StorageEosioAction {
        const doc = hit._source;
        if (this.validate)
            return StorageEosioActionSchema.parse(doc);

        return doc
    }

    private async maybeConfigureIndices(indices: string[]) {
        if (this.scrollSize > 10000) {
            for (const index of indices) {
                const indexSettings = await this.conn.elastic.indices.getSettings({index});
                const resultWindowSetting = indexSettings[index].settings.max_result_window;
                if (resultWindowSetting < this.scrollSize) {
                    await this.conn.elastic.indices.putSettings({
                        index,
                        settings: {
                            index: {
                                max_result_window: this.scrollSize
                            }
                        }
                    });
                }
            }
        }
    }

    /*
     * Perform initial scroll/search request on current index
     */
    private async deltaScrollRequest(): Promise<{scrollId: string, hits: StorageEosioDelta[]}> {
        const deltaResponse = await this.conn.elastic.search({
            index: this.deltaIndices[this.currentDeltaSuff],
            scroll: this.scrollOpts.scroll ? this.scrollOpts.scroll : '1s',
            size: this.scrollSize,
            sort: [{'block_num': 'asc'}],
            query: {
                range: {
                    block_num: {
                        gte: this.from,
                        lte: this.to
                    }
                }
            },
            _source: this.scrollOpts.fields
        });
        const deltaScrollInfo = {
            scrollId: deltaResponse._scroll_id,
            hits: deltaResponse.hits.hits.map(h => this.unwrapDeltaHit(h))
        };
        return deltaScrollInfo;
    }

    /*
     * Perform initial scroll/search request on current index
     */
    private async actionScrollRequest(): Promise<{scrollId: string, hits: StorageEosioAction[]}> {
        const actionResponse = await this.conn.elastic.search({
            index: this.actionIndices[this.currentActionSuff],
            scroll: this.scrollOpts.scroll ? this.scrollOpts.scroll : '1s',
            size: this.scrollOpts.size ? this.scrollOpts.size : 1000,
            sort: [{'@raw.block': 'asc', '@raw.trx_index': 'asc'}],
            query: {
                range: {
                    '@raw.block': {
                        gte: this.from - this.conn.config.evmBlockDelta,
                        lte: this.to - this.conn.config.evmBlockDelta
                    }
                }
            }
        });
        const actionScrollInfo = {
            scrollId: actionResponse._scroll_id,
            hits: actionResponse.hits.hits.map(h => this.unwrapActionHit(h))
        };
        return actionScrollInfo;
    }

    private async nextActionScroll(target: number): Promise<void> {
        try {
            const actionScrollResponse = await this.conn.elastic.scroll({
                scroll_id: this.currentActionScrollId,
                scroll: this.scrollOpts.scroll ? this.scrollOpts.scroll : '1s'
            });
            let hits = actionScrollResponse.hits.hits;

            if (hits.length === 0) {
                // clear current scroll context
                await this.conn.elastic.clearScroll({
                    scroll_id: this.currentActionScrollId
                });
                if (this.lastBlockTx < target) {
                    this.currentActionSuff++;
                    if (this.currentActionSuff == this.actionIndices.length)
                        throw new Error(
                            `Scanned all relevant indices but could not reach target tx block ${target}`);

                    // open new scroll & return hits from next one
                    const scrollInfo = await this.actionScrollRequest();
                    this.currentActionScrollId = scrollInfo.scrollId;
                    this.rangeTxs.push(...scrollInfo.hits);
                }
            } else
                this.rangeTxs.push(...hits.map(h => this.unwrapActionHit(h)));

            if (this.rangeTxs.length > 0)
                this.lastBlockTx = this.rangeTxs[this.rangeTxs.length - 1]["@raw"].block;

        } catch (e) {
            this.conn.logger.error('BlockScroller error while fetching next batch:')
            this.conn.logger.error(e.message);
            throw e;
        }
    }

    private async packScrollResult(deltaHits: StorageEosioDelta[]): Promise<ScrollResult> {
        const result: BlockData[] = [];
        const minBlock = deltaHits[0].block_num;
        const maxBlock = deltaHits[deltaHits.length - 1].block_num;
        let curBlock = minBlock;
        for (const delta of deltaHits) {
            curBlock = delta.block_num;
            const evmBlockNum = curBlock - this.conn.config.evmBlockDelta;
            const blockTxs = [];

            while (evmBlockNum > this.lastBlockTx)
                await this.nextActionScroll(evmBlockNum)

            while (this.rangeTxs.length > 0 &&
                   this.rangeTxs[0]['@raw'].block == evmBlockNum) {
                const tx = this.rangeTxs.shift();
                blockTxs.push(tx);
            }

            if (blockTxs.length === 0 && 'gasUsed' in delta && delta.gasUsed !== '0')
                throw new Error(`block #${curBlock}: gasUsed != 0 && blockTxs len > 0`);

            result.push({
                block: delta,
                actions: blockTxs
            })
        }
        return {minBlock, maxBlock, data: result};
    }

    /*
     * Scroll an already open request, if we got 0 hits
     * check if reached end, if not try to move to next delta index
     */
    private async nextScroll() {
        let result: ScrollResult;

        try {
            const deltaScrollResponse = await this.conn.elastic.scroll({
                scroll_id: this.currentDeltaScrollId,
                scroll: this.scrollOpts.scroll ? this.scrollOpts.scroll : '1s'
            });
            const hits = deltaScrollResponse.hits.hits;

            if (hits.length === 0) {
                // clear current scroll context
                await this.conn.elastic.clearScroll({
                    scroll_id: this.currentDeltaScrollId
                });
                // is scroll done?
                if (this.last >= this.to)
                    this._isDone = true;

                else {
                    // are indexes exhausted?
                    this.currentDeltaSuff++;
                    if (this.currentDeltaSuff == this.deltaIndices.length)
                        throw new Error(`Scanned all relevant indexes but didnt reach ${this.to}`);

                    // open new scroll & return hits from next one
                    const scrollInfo = await this.deltaScrollRequest();
                    this.currentDeltaScrollId = scrollInfo.scrollId;
                    if (scrollInfo.hits.length > 0)
                        result = await this.packScrollResult(scrollInfo.hits);
                }
            } else
                result = await this.packScrollResult(hits.map(h => this.unwrapDeltaHit(h)));

            if (result) {
                this.last = result.maxBlock;
                this.range.push(...result.data);
            }

        } catch (e) {
            this.conn.logger.error('BlockScroller error while fetching next batch:')
            this.conn.logger.error(e.message);
            this.conn.logger.error(e.stack);
            throw e;
        }
    }

    /*
     * Perform first scroll request and set state tracking vars
     */
    async init() {
        // get relevant indexes
        this.deltaIndices = await this.conn.getRelevantDeltaIndicesForRange(this.from, this.to);
        this.actionIndices = await this.conn.getRelevantActionIndicesForRange(this.from, this.to);
        this.currentDeltaSuff = 0;
        this.currentActionSuff = 0;

        await this.maybeConfigureIndices(this.actionIndices);
        await this.maybeConfigureIndices(this.deltaIndices);

        // first scroll request
        const deltaScrollResult = await this.deltaScrollRequest();
        const actionScrollResult = await this.actionScrollRequest();

        if (deltaScrollResult.hits.length == 0)
            throw new Error(`could not find blocks on ${this.deltaIndices}`);

        this.currentDeltaScrollId =  deltaScrollResult.scrollId;

        if (this.actionIndices.length == 0)
            this.lastBlockTx = this.to - this.conn.config.evmBlockDelta;

        this.currentActionScrollId =  actionScrollResult.scrollId;

        this.rangeTxs = actionScrollResult.hits;
        const initialResult = await this.packScrollResult(deltaScrollResult.hits);
        this.range = initialResult.data;

        // bail early if no hits
        if (this.range.length > 0)
            this.last = initialResult.maxBlock;
        else
            this._isDone = true;

        if (this.rangeTxs.length > 0)
            this.lastBlockTx = this.rangeTxs[this.rangeTxs.length - 1]['@raw'].block;

        this._isInit = true;
    }

    async nextResult(): Promise<BlockData> {
        if (!this._isInit) throw new Error('Must call init() before nextResult()!');

        const nextBlock = this.lastYielded + 1;
        // this.conn.logger.info(`${this.tag}: nextBlock prescroll: ${nextBlock}`)
        while (!this._isDone && nextBlock > this.last)
            await this.nextScroll();

        // this.conn.logger.info(`${this.tag}: nextBlock postscroll: ${nextBlock}`)
        if (!this._isDone && nextBlock !== this.range[0].block.block_num)
            throw new Error(`from ${this.tag}: nextblock != range[0]`)

        const block = this.range.shift();
        this.lastYielded = nextBlock;
        return block;
    }

    /*
     * Important before using this in a for..of statement,
     * call this.init! class gets info about indexes needed
     * for scroll from elastic
     */
    async *[Symbol.asyncIterator](): AsyncIterableIterator<BlockData> {
        do {
            const block = await this.nextResult();
            if (!this._isDone) yield block;
        } while (!this._isDone)
    }
}

export class Connector {
    config: IndexerConfig;
    logger: Logger;
    elastic: Client;
    chainName: string;

    state: IndexerState;

    blockDrain: IndexedBlockInfo[];
    opDrain: any[];

    totalPushed: number = 0;
    lastPushed: number = 0;

    writeCounter: number = 0;
    lastDeltaIndexSuff: number = undefined;
    lastActionIndexSuff: number = undefined;

    broadcast: RPCBroadcaster;
    isBroadcasting: boolean = false;

    events = new EventEmitter();

    constructor(config: IndexerConfig, logger: Logger) {
        this.config = config;
        this.logger = logger;
        this.chainName = config.chainName;
        this.elastic = new Client(config.elastic);

        this.broadcast = new RPCBroadcaster(config.broadcast, logger);

        this.opDrain = [];
        this.blockDrain = [];

        this.state = IndexerState.SYNC;
    }

    getSuffixForBlock(blockNum: number) {
        const adjustedNum = Math.floor(blockNum / this.config.elastic.docsPerIndex);
        return String(adjustedNum).padStart(8, '0');
    }

    getDeltaIndexForBlock(blockNum: number) {
        return `${this.config.chainName}-${this.config.elastic.subfix.delta}-${this.getSuffixForBlock(blockNum)}`;
    }

    getActionIndexForBlock(blockNum: number) {
        return `${this.config.chainName}-${this.config.elastic.subfix.transaction}-${this.getSuffixForBlock(blockNum)}`;
    }

    async init() {
        const indexConfig: ConfigInterface = getTemplatesForChain(
            this.chainName,
            this.config.elastic.subfix,
            this.config.elastic.numberOfShards,
            this.config.elastic.numberOfReplicas,
            this.config.elastic.refreshInterval,
            this.config.elastic.codec
        );

        this.logger.info(`Updating index templates for ${this.chainName}...`);
        let updateCounter = 0;
        for (const [name, template] of Object.entries(indexConfig)) {
            try {
                const creation_status: estypes.IndicesPutTemplateResponse = await this.elastic.indices.putTemplate({
                    name: `${this.chainName}-${name}`,
                    body: template
                });
                if (!creation_status || !creation_status['acknowledged']) {
                    this.logger.error(`Failed to create template: ${this.chainName}-${name}`);
                } else {
                    updateCounter++;
                    this.logger.info(`${this.chainName}-${name} template updated!`);
                }
            } catch (e) {
                this.logger.error(`[FATAL] ${e.message}`);
                if (e.meta) {
                    this.logger.error(e.meta.body);
                }
                process.exit(1);
            }
        }
        this.logger.info(`${updateCounter} index templates updated`);
    }

    startBroadcast() {
        this.broadcast.initUWS();
        this.isBroadcasting = true;
    }

    stopBroadcast() {
        this.broadcast.close();
        this.isBroadcasting = false;
    }

    async deinit() {
        await this.flush();

        if (this.isBroadcasting)
            this.stopBroadcast();

        await this.elastic.close();
    }

    async getOrderedDeltaIndices() {
        // if (this.deltaIndexCache) return this.deltaIndexCache;

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

        // this.deltaIndexCache = deltaIndices;

        return deltaIndices;
    }

    async getRelevantDeltaIndicesForRange(from: number, to: number): Promise<string[]> {
        const startSuffNum = Math.floor(from / this.config.elastic.docsPerIndex);
        const endSuffNum = Math.floor(to / this.config.elastic.docsPerIndex);
        return (await this.getOrderedDeltaIndices()).filter((index) => {
            const indexSuffNum = indexToSuffixNum(index.index);
            return (indexSuffNum >= startSuffNum && indexSuffNum <= endSuffNum)
        }).map(index => index.index);
    }

    async getOrderedActionIndices() {
        // if (this.actionIndexCache) return this.actionIndexCache;

        const actionIndices: estypes.CatIndicesResponse = await this.elastic.cat.indices({
            index: `${this.chainName}-${this.config.elastic.subfix.transaction}-*`,
            format: 'json'
        });
        actionIndices.sort((a, b) => {
            const aNum = indexToSuffixNum(a.index);
            const bNum = indexToSuffixNum(b.index);
            if (aNum < bNum)
                return -1;
            if (aNum > bNum)
                return 1;
            return 0;
        });

        // this.actionIndexCache = actionIndices;

        return actionIndices;
    }

    async getRelevantActionIndicesForRange(from: number, to: number): Promise<string[]> {
        const startSuffNum = Math.floor(from / this.config.elastic.docsPerIndex);
        const endSuffNum = Math.floor(to / this.config.elastic.docsPerIndex);
        return (await this.getOrderedActionIndices()).filter((index) => {
            const indexSuffNum = indexToSuffixNum(index.index);
            return (indexSuffNum >= startSuffNum && indexSuffNum <= endSuffNum);
        }).map(index => index.index);
    }

    private unwrapSingleElasticResult(result) {
        const hits = result?.hits?.hits;

        if (!hits)
            throw new Error(`Elastic unwrap error hits undefined`);

        if (hits.length != 1)
            throw new Error(`Elastic unwrap error expected one and got ${hits.length}`);

        const document = hits[0]._source;

        this.logger.debug(`elastic unwrap document:\n${JSON.stringify(document, null, 4)}`);

        let parseResult = StorageEosioDeltaSchema.safeParse(document);
        if (parseResult.success)
            return parseResult.data;

        parseResult = StorageEosioGenesisDeltaSchema.safeParse(document);
        if (parseResult.success)
            return parseResult.data;

        throw new Error(`Document is not a valid StorageEosioDelta!`);
    }

    async getIndexedBlock(blockNum: number) : Promise<StorageEosioDelta> {
        const suffix = this.getSuffixForBlock(blockNum);
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

            return this.unwrapSingleElasticResult(result);

        } catch (error) {
            return null;
        }
    }

    async getFirstIndexedBlock() : Promise<StorageEosioDelta> {
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

            return this.unwrapSingleElasticResult(result);

        } catch (error) {
            return null;
        }
    }

    async getLastIndexedBlock() : Promise<StorageEosioDelta> {
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
                if (result?.hits?.hits?.length == 0)
                    continue;

                return this.unwrapSingleElasticResult(result);

            } catch (error) {
                this.logger.error(error);
                throw error;
            }
        }

        return null;
    }

    async findGapInIndices() {
        const deltaIndices = await this.getOrderedDeltaIndices();
        this.logger.debug('delta indices: ');
        this.logger.debug(JSON.stringify(deltaIndices, null, 4))
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

        this.logger.debug(`runHistogramGapCheck: ${lower}-${upper}, interval: ${interval}`);
        this.logger.debug(JSON.stringify(buckets, null, 4));

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

            this.logger.debug(`findDuplicateDeltas: ${lower}-${upper}`);

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

            this.logger.debug(`findDuplicateActions: ${lower}-${upper}`);

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

        this.logger.debug(`calculated middle ${middle}`);

        this.logger.debug('first half');
        // Recurse on the first half
        const lowerBuckets = await this.runHistogramGapCheck(lowerBound, middle, interval / 2);
        if (lowerBuckets.length === 0) {
            return middle; // Gap detected
        } else if (lowerBuckets[lowerBuckets.length - 1].max_block.value < middle) {
            const lowerGap = await this.checkGaps(lowerBound, middle, interval / 2);
            if (lowerGap)
                return lowerGap;
        }

        this.logger.debug('second half');
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

        const lowerBoundDelta = lowerBoundDoc.block_num - lowerBound;
        if (lowerBoundDelta != this.config.evmBlockDelta) {
            this.logger.error(`wrong block delta on lower bound doc ${lowerBoundDelta}`);
            throw new Error(`wrong block delta on lower bound doc ${lowerBoundDelta}`);
        }

        const upperBoundDelta = upperBoundDoc.block_num - upperBound;
        if (upperBoundDelta != this.config.evmBlockDelta) {
            this.logger.error(`wrong block delta on upper bound doc ${upperBoundDelta}`);
            throw new Error(`wrong block delta on upper bound doc ${upperBoundDelta}`);
        }

        const step = 10000000; // 10 million blocks

        const deltaDups = [];
        const actionDups = [];

        for (let currentLower = lowerBound; currentLower < upperBound; currentLower += step) {
            const currentUpper = Math.min(currentLower + step, upperBound);

            // check duplicates for the current range
            deltaDups.push(...(await this.findDuplicateDeltas(currentLower, currentUpper)));
            actionDups.push(...(await this.findDuplicateActions(currentLower, currentUpper)));

            this.logger.info(
                `checked range ${currentLower}-${currentUpper} for duplicates, found: ${deltaDups.length + actionDups.length}`);
        }

        if (deltaDups.length > 0)
            this.logger.error(`block duplicates found: ${JSON.stringify(deltaDups)}`);

        if (actionDups.length > 0)
            this.logger.error(`tx duplicates found: ${JSON.stringify(actionDups)}`);

        if (deltaDups.length + actionDups.length > 0)
            throw new Error('Duplicates found!');

        if (upperBound - lowerBound < 2)
            return null;

        // first just check if whole indices are missing
        const gap = await this.findGapInIndices();
        if (gap) {
            this.logger.debug(`whole index seems to be missing `);
            const lower = gap.gapStart * this.config.elastic.docsPerIndex;
            const upper = (gap.gapStart + 1) * this.config.elastic.docsPerIndex;
            const agg = await this.runHistogramGapCheck(
                lower, upper, this.config.elastic.docsPerIndex)
            return agg[0].max_block.value;
        }

        const initialInterval = upperBound - lowerBound;

        this.logger.info(`starting full gap check from ${lowerBound} to ${upperBound}`);

        return this.checkGaps(lowerBound, upperBound, initialInterval);
    }

    async _deleteBlocksInRange(startBlock: number, endBlock: number) {
        const targetSuffix = this.getSuffixForBlock(endBlock);
        const deltaIndex = `${this.chainName}-${this.config.elastic.subfix.delta}-${targetSuffix}`;
        const actionIndex = `${this.chainName}-${this.config.elastic.subfix.transaction}-${targetSuffix}`;

        try {
            await this._deleteFromIndex(deltaIndex, 'block_num', startBlock, endBlock);
            await this._deleteFromIndex(
                actionIndex, '@raw.block', startBlock - this.config.evmBlockDelta, endBlock - this.config.evmBlockDelta);
        } catch (e) {
            if (e.name != 'ResponseError' || e.meta.body.error.type != 'index_not_found_exception') {
                throw e;
            }
        }
    }

    async _deleteFromIndex(index: string, blockField: string, startBlock: number, endBlock: number) {
        const result = await this.elastic.deleteByQuery({
            index: index,
            body: {
                query: {
                    range: {
                        [blockField]: {
                            gte: startBlock,
                            lte: endBlock
                        }
                    }
                }
            },
            conflicts: 'proceed',
            refresh: true,
            error_trace: true
        });
        this.logger.debug(`${index} delete result: ${JSON.stringify(result, null, 4)}`);
    }

    async _purgeBlocksNewerThan(blockNum: number) {
        const lastBlock = await this.getLastIndexedBlock();
        if (lastBlock == null)
            return;
        const maxBlockNum = lastBlock.block_num;
        const batchSize = Math.min(maxBlockNum, 20000); // Batch size set to 20,000
        const maxConcurrentDeletions = 4; // Maximum of 4 deletions in parallel

        for (let startBlock = blockNum; startBlock <= maxBlockNum; startBlock += batchSize * maxConcurrentDeletions) {
            const deletionTasks = [];

            for (let i = 0; i < maxConcurrentDeletions && (startBlock + i * batchSize) <= maxBlockNum; i++) {
                const batchStart = startBlock + i * batchSize;
                const batchEnd = Math.min(batchStart + batchSize - 1, maxBlockNum);
                deletionTasks.push(this._deleteBlocksInRange(batchStart, batchEnd));
            }

            await Promise.all(deletionTasks);
            const batchEnd = Math.min(startBlock + (batchSize * maxConcurrentDeletions), maxBlockNum);
            this.logger.info(`deleted blocks from ${startBlock} to ${batchEnd}`);
        }
    }

    async _purgeIndicesNewerThan(blockNum: number) {
        this.logger.info(`purging indices in db from block ${blockNum}...`);
        const targetSuffix = this.getSuffixForBlock(blockNum);
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
            this.logger.info(`deleted indices result: ${JSON.stringify(deleteResult, null, 4)}`);
        }

        return deleteList;
    }

    async purgeNewerThan(blockNum: number) {
        await this._purgeIndicesNewerThan(blockNum);
        await this._purgeBlocksNewerThan(blockNum);
    }

    async flush() {
        if (this.opDrain.length == 0) return;
        this.writeCounter++;
        await this.writeBlocks();
    }

    async pushBlock(blockInfo: IndexedBlockInfo) {
        const currentBlock = blockInfo.delta.block_num;
        if (this.totalPushed != 0 && currentBlock != this.lastPushed + 1)
            throw new Error(`Expected: ${this.lastPushed + 1} and got ${currentBlock}`);

        const suffix = this.getSuffixForBlock(blockInfo.delta.block_num);
        const txIndex = `${this.chainName}-${this.config.elastic.subfix.transaction}-${suffix}`;
        const dtIndex = `${this.chainName}-${this.config.elastic.subfix.delta}-${suffix}`;
        const errIndex = `${this.chainName}-${this.config.elastic.subfix.error}-${suffix}`;

        const txOperations = blockInfo.transactions.flatMap(
            doc => [{create: {_index: txIndex, _id: `${this.chainName}-tx-${currentBlock}-${doc['@raw'].trx_index}`}}, doc]);

        const errOperations = blockInfo.errors.flatMap(
            doc => [{index: {_index: errIndex}}, doc]);

        // const operations = [
        //     ...errOperations,
        //     ...txOperations,
        //     {create: {_index: dtIndex, _id: `${this.chainName}-block-${currentBlock}`}}, blockInfo.delta
        // ];
        const operations = [];
        Array.prototype.push.apply(operations, errOperations);
        Array.prototype.push.apply(operations, txOperations);
        operations.push({create: {_index: dtIndex, _id: `${this.chainName}-block-${currentBlock}`}}, blockInfo.delta);

        // this.opDrain = [...this.opDrain, ...operations];
        Array.prototype.push.apply(this.opDrain, operations);
        this.blockDrain.push(blockInfo);

        this.lastPushed = currentBlock;
        this.totalPushed++;

        if (this.state == IndexerState.HEAD ||
            this.opDrain.length >= (this.config.perf.elasticDumpSize * 2)) {
            await this.flush();
        }
    }

    forkCleanup(
        timestamp: string,
        lastNonForked: number,
        lastForked: number
    ) {
        // fix state flag
        this.lastPushed = lastNonForked;

        // clear elastic operations drain
        let i = this.opDrain.length - 1;
        while (i > 0) {
            const op = this.opDrain[i];
            if (op && isStorableDocument(op) &&
                op.block_num > lastNonForked) {
                this.opDrain.splice(i - 1); // delete indexing info
                this.opDrain.splice(i);     // delete the document
            }
            i -= 2;
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
        const suffix = this.getSuffixForBlock(lastNonForked);
        const frkIndex = `${this.chainName}-${this.config.elastic.subfix.fork}-${suffix}`;
        this.opDrain.push({index: {_index: frkIndex}});
        this.opDrain.push({timestamp, lastNonForked, lastForked});
    }

    async writeBlocks() {
        const bulkResponse = await this.elastic.bulk({
            refresh: true,
            operations: this.opDrain,
            error_trace: true
        })

        const first = this.blockDrain[0].delta.block_num;
        const last = this.blockDrain[this.blockDrain.length - 1].delta.block_num;

        const lastAdjusted = Math.floor(last / this.config.elastic.docsPerIndex);

        if (lastAdjusted !== this.lastDeltaIndexSuff)
            this.lastDeltaIndexSuff = lastAdjusted;

        if (lastAdjusted !== this.lastActionIndexSuff)
            this.lastActionIndexSuff = lastAdjusted;

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
                        operation: this.opDrain[i * 2],
                        document: this.opDrain[i * 2 + 1]
                    })
                }
            });

            throw new Error(JSON.stringify(erroredDocuments, null, 4));
        }
        this.logger.info(`drained ${this.opDrain.length} operations.`);
        if (this.isBroadcasting) {
            this.logger.info(`broadcasting ${this.opDrain.length} blocks...`)

            for (const block of this.blockDrain)
                this.broadcast.broadcastBlock(block);
        }

        this.logger.info('done.');

        this.opDrain = [];
        this.blockDrain = [];

        this.writeCounter--;

        const writeEvent = {from: first, to: last};

        this.events.emit('write', writeEvent);
    }

    async blockScroll(params: {
        from: number,
        to: number,
        validate?: boolean,
        scrollOpts?: ScrollOptions
    }) {
        const scroller = new BlockScroller(this, params);
        return scroller;
    }

}
