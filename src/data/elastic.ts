import {
    ConnectorConfig,
    ElasticConnectorConfig,
    IndexedBlockInfo,
    IndexerState,
} from '../types/indexer.js';
import {getTemplatesForChain} from './templates.js';

import {Client, estypes} from '@elastic/elasticsearch';
import {
    isStorableDocument, StorageEosioAction, StorageEosioActionSchema,
    StorageEosioDelta,
    StorageEosioDeltaSchema, StorageEosioGenesisDeltaSchema
} from '../types/evm.js';
import {createLogger, format, Logger, transports} from "winston";
import EventEmitter from "events";
import {BlockData, BlockScroller, Connector} from "./connector.js";


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

export class ElasticScroller extends BlockScroller {

    private readonly conn: ElasticConnector;
    private readonly scrollOpts: ScrollOptions;  // es scroll options

    private last: number;         // native block num of last block available on current range array, starts at `from` - 1
    private lastYielded: number;  // last block yielded on iterator, starts at `from` - 1
    private lastBlockTx: number;  // last evm block we have full transactions on current rangeTx array, starts at evm equivalent for `last`

    // we should at most have only two open scroll contexts per BlockScroller
    private currentDeltaScrollId: string;  // one for deltas
    private currentActionScrollId: string;  // one for actions

    private _currentDeltaIndex: number;  // index of current delta index based on `deltaIndices` array
    private _currentActionIndex: number;  // index of current action index based on `actionIndices` array
    private _deltaIndices: string[];  // array of relevant delta indices found for range `from` - `to`, must have at least one to start
    private _actionIndices: string[]; // array of relevant action indices found for range `from` - `to`, can be empty array

    private range: BlockData[] = [];  // contains latest batch of blocks we have prepared to yield
    private rangeTxs: StorageEosioAction[] = [];  // contains latest batch of action documents we need to unpack into the right block BlockData

    constructor(
        connector: ElasticConnector,
        params: {
            from: number,
            to: number,
            tag: string
            logLevel?: string,
            validate?: boolean,
            scrollOpts?: ScrollOptions
        }
    ) {
        super();
        this.conn = connector;
        this.from = params.from;
        this.to = params.to;
        this.tag = params.tag;

        this.scrollOpts = params.scrollOpts ? params.scrollOpts : {
            size: 1000,
            scroll: '3m'
        };
        this.validate = params.validate ? params.validate : false;

        this.last = this.from - 1;
        this.lastBlockTx = this.last - this.conn.config.chain.evmBlockDelta;

        this.lastYielded = this.from - 1;

        const logLevel = params.logLevel ? params.logLevel : 'warning';
        const loggingOptions = {
            exitOnError: false,
            level: logLevel,
            format: format.combine(
                format.metadata(),
                format.colorize(),
                format.timestamp(),
                format.printf((info: any) => {
                    return `${info.timestamp} [PID:${process.pid}] [${info.level}] [BlockScroller-${this.tag}] : ${info.message} ${Object.keys(info.metadata).length > 0 ? JSON.stringify(info.metadata) : ''}`;
                })
            )
        }
        this.logger = createLogger(loggingOptions);
        this.logger.add(new transports.Console({
            level: logLevel
        }));
        this.logger.debug('Logger initialized with level ' + logLevel);
    }

    get currentDeltaIndexNum() {
        return this._currentDeltaIndex;
    }

    get currentActionIndexNum() {
        return this._currentActionIndex;
    }

    get currentDeltaIndex() {
        return this.deltaIndices[this._currentDeltaIndex];
    }

    get currentActionIndex() {
        return this.actionIndices[this._currentActionIndex];
    }

    get deltaIndices() {
        return this._deltaIndices;
    }

    get actionIndices() {
        return this._actionIndices;
    }

    get lastBlock() {
        if (this.lastYielded > this.from - 1)
            return this.lastYielded;
        else
            return undefined;
    }

    /*
     * Maybe perform delta schema validation
     */
    private unwrapDeltaHit(hit): StorageEosioDelta {
        const doc = hit._source;
        if (this.validate)
            return StorageEosioGenesisDeltaSchema.parse(doc);

        return doc
    }

    /*
     * Maybe perform action schema validation
     */
    private unwrapActionHit(hit): StorageEosioAction {
        const doc = hit._source;
        if (this.validate)
            return StorageEosioActionSchema.parse(doc);

        return doc
    }

    /*
     * By default elastic allows up to 10,000 blocks scroll batch size, in case
     * user requested more, set the max_result_window setting
     */
    private async maybeConfigureIndices(indices: string[]) {
        for (const index of indices) {
            const indexSettings = await this.conn.elastic.indices.getSettings({index});
            const resultWindowSetting = indexSettings[index].settings.max_result_window;
            if (!resultWindowSetting || resultWindowSetting < this.scrollOpts.size) {
                await this.conn.elastic.indices.putSettings({
                    index,
                    settings: {
                        index: {
                            max_result_window: this.scrollOpts.size
                        }
                    }
                });
                this.logger.debug(`Configured index ${index} index.max_result_window: ${this.scrollOpts.size}`);
            }
        }
    }

    /*
     * Set current block data batch to read from also maybe update last block available var,
     * deltas assumed to be length > 0
     */
    private addRange(deltas: BlockData[]) {
        this.range.push(...deltas);
        this.last = deltas[deltas.length - 1].block.block_num;
        this.logger.debug(`set last to ${this.last}, range length: ${this.range.length}`);
    }

    /*
     * Open scroll/search request on current index
     */
    private async deltaScrollRequest(): Promise<StorageEosioDelta[]> {
        const deltaResponse = await this.conn.elastic.search({
            index: this._deltaIndices[this._currentDeltaIndex],
            scroll: this.scrollOpts.scroll,
            size: this.scrollOpts.size,
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
        this.currentDeltaScrollId = deltaResponse._scroll_id;
        this.logger.debug(`opened new scroll with index ${this._deltaIndices[this._currentDeltaIndex]}`);
        return deltaResponse.hits.hits.map(h => this.unwrapDeltaHit(h));
    }

    /*
     * Add a batch of actions to our current rangeTxs array,
     * maybe set `lastBlockTx` to the last block we know we have ALL
     * txs for.
     *
     * This is done by always requesting txs up to evm block `to` + 1, then
     * we only assume we got transactions up to block `lastBlockTx` if and
     * only if we see txs for block `lastBlockTx` + 1 on scroll results.
     */
    private addRangeTxs(actions: StorageEosioAction[]) {
        this.rangeTxs.push(...actions);
        if (this.rangeTxs.length > 0) {
            const _lastBlockTx = this.rangeTxs[this.rangeTxs.length - 1]['@raw'].block;
            for (let i = this.rangeTxs.length - 1; i >= 0; i--) {
                const curEvmBlock = this.rangeTxs[i]['@raw'].block;
                if (curEvmBlock < _lastBlockTx) {
                    this.lastBlockTx = curEvmBlock;
                    break;
                }
            }

            this.logger.debug(`set lastBlockTx to ${this.lastBlockTx}, rangeTxs length: ${this.rangeTxs.length}`);
        }
    }

    /*
     * Open scroll/search request on current index, search up to `@raw.block` == (evm equivalent of `to` + 1),
     * in order for addRangeTxs lastBlockTx setter algo to work
     */
    private async actionScrollRequest(): Promise<void> {
        const actionResponse = await this.conn.elastic.search({
            index: this._actionIndices[this._currentActionIndex],
            scroll: this.scrollOpts.scroll,
            size: this.scrollOpts.size,
            sort: [{'@raw.block': 'asc', '@raw.trx_index': 'asc'}],
            query: {
                range: {
                    '@raw.block': {
                        gte: this.from - this.conn.config.chain.evmBlockDelta,
                        lte: this.to - this.conn.config.chain.evmBlockDelta + 1
                    }
                }
            }
        });
        this.currentActionScrollId = actionResponse._scroll_id;
        this.addRangeTxs(actionResponse.hits.hits.map(h => this.unwrapActionHit(h)));
        this.logger.debug(`opened new scroll with index ${this._actionIndices[this._currentActionIndex]}`);
    }

    /*
     * Fetch next batch of actions from the currently open scroll query, called by packScrollResult when
     * it needs transactions from a block > current `lastBlockTx`, it can happen that the underlying
     * action scroll context timed out and we need to open a new one.
     *
     * On first call will open the action scroll request and return
     *
     * If we scrolled all available action indices and haven't found more txs assume we reached end and no
     * more txs are present on indices.
     */
    private async nextActionScroll(target: number): Promise<void> {
        this.logger.debug(`nextActionScroll: target ${target}, rangeTx length: ${this.rangeTxs.length}, lastBlockTx: ${this.lastBlockTx}`);
        if (this.lastBlockTx < (this.from - this.conn.config.chain.evmBlockDelta)) {
            await this.actionScrollRequest();
            return;
        }
        try {
            const actionScrollResponse = await this.conn.elastic.scroll({
                scroll_id: this.currentActionScrollId,
                scroll: this.scrollOpts.scroll
            });
            let hits = actionScrollResponse.hits.hits;

            this.logger.debug(`action scroll returned ${hits.length} hits.`);

            if (hits.length === 0) {
                // clear current scroll context
                await this.conn.elastic.clearScroll({
                    scroll_id: this.currentActionScrollId
                });
                if (this.lastBlockTx < target) {
                    if (this._currentActionIndex == this._actionIndices.length - 1) {
                        this.lastBlockTx = this.to - this.conn.config.chain.evmBlockDelta;
                        this.logger.debug(`Action scroller reached end, set lastBlockTx to ${this.lastBlockTx}`);
                        return;
                    }

                    // open new scroll & return hits from next one
                    this._currentActionIndex++;
                    await this.actionScrollRequest();
                }
            } else
                this.addRangeTxs(hits.map(h => this.unwrapActionHit(h)));

        } catch (e) {
            if (e.message.includes('No search context found for id')) {
                this.logger.warn(`Tried to scroll on non existent action scroll context ${this.currentActionScrollId}, reopen scroll...`);
                this.logger.warn(`current scroll window setting: ${this.scrollOpts.scroll}`);
                await this.actionScrollRequest();

            } else {
                this.conn.logger.error('BlockScroller error while fetching next batch:')
                this.conn.logger.error(e.message);
                throw e;
            }
        }
    }

    /*
     * Remove all transactions from rangeTxs array that match target block
     * and calculate total gasused
     */
    private drainTxsFromRange(target: number): [bigint, StorageEosioAction[]] {
        const txs = [];
        let calculatedGasUsed = BigInt(0);
        while (this.rangeTxs.length > 0 &&
               this.rangeTxs[0]['@raw'].block == target) {
            const tx = this.rangeTxs.shift();
            calculatedGasUsed += BigInt(tx['@raw'].gasused);
            txs.push(tx);
        }
        return [calculatedGasUsed, txs];
    }

    /*
     * Pump action scroll search until `lastBlockTx` >= target, then
     * drain Txs using `drainTxsFromRange`
     */
    private async buildBlockTxArray(target: number): Promise<[bigint, StorageEosioAction[]]> {
        if (this._currentActionIndex == this.actionIndices.length)
            return [BigInt(0), []];

        while (target > this.lastBlockTx)
            await this.nextActionScroll(target);

        return this.drainTxsFromRange(target);
    }

    /*
     * Pack a fresh batch of deltas returned from delta scroll into BlockData,
     * this involves getting the respective transactions for every block,
     * which is handled by `buildBlockTxArray`.
     *
     * After gathering the relevant actions for a block we also perform additional checks on the
     * gasUsed as a final sanity check.
     */
    private async packScrollResult(deltaHits: StorageEosioDelta[]): Promise<void> {
        const minBlock = deltaHits[0].block_num;
        const maxBlock = deltaHits[deltaHits.length - 1].block_num;
        this.logger.debug(`packScrollResult: min ${minBlock} max ${maxBlock} lastBlockTx: ${this.lastBlockTx}`);
        let curBlock = minBlock;
        const newRange = [];
        for (const delta of deltaHits) {
            curBlock = delta.block_num;
            const evmBlockNum = curBlock - this.conn.config.chain.evmBlockDelta;
            const [calculatedGasUsed, blockTxs] = await this.buildBlockTxArray(evmBlockNum);

            const gasUsed = delta.gasUsed ? BigInt(delta.gasUsed) : BigInt(0);
            if (gasUsed != calculatedGasUsed)
                throw new Error(
                    `block #${curBlock}, evm: ${evmBlockNum}: ` +
                    `calculatedGasUsed (${calculatedGasUsed}) ` +
                    `doesn\'t match source gasUsed (${gasUsed})`
                );

            newRange.push({
                block: delta,
                actions: blockTxs
            })
        }
        this.addRange(newRange);
    }

    /*
     * Scroll an already open request, if we got 0 hits
     * check if reached end, if not try to move to next delta index
     */
    private async nextScroll() {
        this.logger.debug(`nextScroll currentDeltaIndex: ${this._deltaIndices[this._currentDeltaIndex]}`);

        const openNewScroll = async () => {
            const newScrollHits = await this.deltaScrollRequest();
            if (newScrollHits.length > 0)
                await this.packScrollResult(newScrollHits);
        };

        try {
            const deltaScrollResponse = await this.conn.elastic.scroll({
                scroll_id: this.currentDeltaScrollId,
                scroll: this.scrollOpts.scroll
            });
            const hits = deltaScrollResponse.hits.hits;

            this.logger.debug(`delta scroll returned ${hits.length} hits.`);

            if (hits.length === 0) {
                // clear current scroll context
                await this.conn.elastic.clearScroll({
                    scroll_id: this.currentDeltaScrollId
                });
                // is scroll done?
                if (this.last >= this.to) {
                    this.logger.debug('nextScroll reached end!');

                } else {
                    // are indexes exhausted?
                    if (this._currentDeltaIndex == this._deltaIndices.length - 1)
                        throw new Error(`Scanned all relevant indexes but didnt reach ${this.to}`);

                    this._currentDeltaIndex++;
                    await openNewScroll();
                }
            } else
                await this.packScrollResult(hits.map(h => this.unwrapDeltaHit(h)));

            this.logger.debug(`nextScroll result: last ${this.last}, range length: ${this.range.length}`);

        } catch (e) {
            if (e.message.includes('No search context found for id')) {
                this.logger.warn(`Tried to scroll on non existant scroll context ${this.currentDeltaScrollId}, reopen scroll...`)
                this.logger.warn(`current scroll window setting: ${this.scrollOpts.scroll}`);
                await openNewScroll();
            } else {
                this.conn.logger.error('BlockScroller error while fetching next batch:')
                this.conn.logger.error(e.message);
                throw e;
            }
        }
    }

    /*
     * Perform first scroll request and set state tracking vars
     */
    async init() {
        this.logger.debug('Initializing scroller...');
        // get relevant indexes
        this._deltaIndices = await this.conn.getRelevantDeltaIndicesForRange(this.from, this.to);
        this._actionIndices = await this.conn.getRelevantActionIndicesForRange(this.from, this.to);
        this._currentDeltaIndex = 0;
        this._currentActionIndex = 0;

        if (this._deltaIndices.length == 0)
            throw new Error(`Could not find delta indices with pattern ${this.conn.getDeltaIndexForBlock(this.from)}`);

        this.logger.debug(`Relevant delta indices:\n${JSON.stringify(this._deltaIndices, null, 4)}`);
        this.logger.debug(`Relevant action indices:\n${JSON.stringify(this._actionIndices, null, 4)}`);

        await this.maybeConfigureIndices(this._actionIndices);
        await this.maybeConfigureIndices(this._deltaIndices);

        // first scroll request
        const deltaScrollResult = await this.deltaScrollRequest();

        if (deltaScrollResult.length == 0)
            throw new Error(`Could not find blocks on ${this._deltaIndices}`);

        await this.packScrollResult(deltaScrollResult);

        this._isInit = true;
        this.logger.debug(
            `Initialized with range length: ${this.range.length}, last: ${this.last}, lastBlockTx: ${this.lastBlockTx}`
        );
    }

    async nextResult(): Promise<BlockData> {
        if (!this._isInit) throw new Error('Must call init() before nextResult()!');

        const nextBlock = this.lastYielded + 1;
        while (!this._isDone && nextBlock > this.last)
            await this.nextScroll();

        if (!this._isDone && nextBlock !== this.range[0].block.block_num)
            throw new Error(`from ${this.tag}: nextblock != range[0]`)

        const block = this.range.shift();
        this.lastYielded = nextBlock;
        this._isDone = this.lastYielded == this.to;
        return block;
    }
}

export class ElasticConnector extends Connector {
    elastic: Client;
    esconfig: ElasticConnectorConfig;

    blockDrain: IndexedBlockInfo[];
    opDrain: any[];

    totalPushed: number = 0;
    lastPushed: number = 0;

    writeCounter: number = 0;
    lastDeltaIndexSuff: number = undefined;
    lastActionIndexSuff: number = undefined;

    events = new EventEmitter();

    constructor(config: ConnectorConfig) {
        super(config);

        if (!config.elastic)
            throw new Error(`Tried to init elastic connector with null config`);

        this.esconfig = config.elastic;
        this.elastic = new Client(this.esconfig);

        this.opDrain = [];
        this.blockDrain = [];

        this.state = IndexerState.SYNC;
    }

    getSuffixForBlock(blockNum: number) {
        const adjustedNum = Math.floor(blockNum / this.esconfig.docsPerIndex);
        return String(adjustedNum).padStart(8, '0');
    }

    getDeltaIndexForBlock(blockNum: number) {
        return `${this.chainName}-${this.esconfig.suffix.delta}-${this.getSuffixForBlock(blockNum)}`;
    }

    getActionIndexForBlock(blockNum: number) {
        return `${this.chainName}-${this.esconfig.suffix.transaction}-${this.getSuffixForBlock(blockNum)}`;
    }

    async init(): Promise<number | null> {

        const indexConfig: ConfigInterface = getTemplatesForChain(
            this.chainName,
            this.config.elastic.suffix,
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

        const gap = await super.init();
        if (gap != null)
            return gap;

        return null;
    }

    async deinit() {
        await super.deinit();

        await this.elastic.close();
    }

    async getDocumentCountAtIndex(index: string): Promise<number> {
        try {
            const response = await this.elastic.count({
                index: index
            });
            return response.count;
        } catch (e) {
            if (e.message.includes('index_not_found'))
                return undefined;
        }
    }

    async getOrderedDeltaIndices() {
        // if (this.deltaIndexCache) return this.deltaIndexCache;

        const deltaIndices: estypes.CatIndicesResponse = await this.elastic.cat.indices({
            index: `${this.chainName}-${this.esconfig.suffix.delta}-*`,
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
        const startSuffNum = Math.floor(from / this.esconfig.docsPerIndex);
        const endSuffNum = Math.floor(to / this.esconfig.docsPerIndex);
        return (await this.getOrderedDeltaIndices()).filter((index) => {
            const indexSuffNum = indexToSuffixNum(index.index);
            return (indexSuffNum >= startSuffNum && indexSuffNum <= endSuffNum)
        }).map(index => index.index);
    }

    async getOrderedActionIndices() {
        // if (this.actionIndexCache) return this.actionIndexCache;

        const actionIndices: estypes.CatIndicesResponse = await this.elastic.cat.indices({
            index: `${this.chainName}-${this.esconfig.suffix.transaction}-*`,
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
        const startSuffNum = Math.floor(from / this.esconfig.docsPerIndex);
        const endSuffNum = Math.floor(to / this.esconfig.docsPerIndex);
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

    async getIndexedBlock(blockNum: number) : Promise<StorageEosioDelta | null> {
        const suffix = this.getSuffixForBlock(blockNum);
        try {
            const result = await this.elastic.search({
                index: `${this.chainName}-${this.esconfig.suffix.delta}-${suffix}`,
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

    async getBlockRange(from: number, to: number): Promise<BlockData[]> {
        const blocks: BlockData[] = [];
        const scroll = this.blockScroll({from, to, tag: 'get-block-range'});
        await scroll.init();
        for await (const block of scroll)
            blocks.push(block);
        return blocks;
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
            index: `${this.config.chain.chainName}-${this.esconfig.suffix.delta}-*`,
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
            index: `${this.config.chain.chainName}-${this.esconfig.suffix.delta}-*`,
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
            index: `${this.config.chain.chainName}-${this.esconfig.suffix.transaction}-*`,
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

    async fullIntegrityCheck(): Promise<number | null> {
        const lowerBoundDoc = await this.getFirstIndexedBlock();
        const upperBoundDoc = await this.getLastIndexedBlock();

        if (!lowerBoundDoc || !upperBoundDoc) {
            return null;
        }

        const lowerBound = lowerBoundDoc['@global'].block_num;
        const upperBound = upperBoundDoc['@global'].block_num;

        const lowerBoundDelta = lowerBoundDoc.block_num - lowerBound;
        if (lowerBoundDelta != this.config.chain.evmBlockDelta) {
            this.logger.error(`wrong block delta on lower bound doc ${lowerBoundDelta}`);
            throw new Error(`wrong block delta on lower bound doc ${lowerBoundDelta}`);
        }

        const upperBoundDelta = upperBoundDoc.block_num - upperBound;
        if (upperBoundDelta != this.config.chain.evmBlockDelta) {
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
            const lower = gap.gapStart * this.esconfig.docsPerIndex;
            const upper = (gap.gapStart + 1) * this.esconfig.docsPerIndex;
            const agg = await this.runHistogramGapCheck(
                lower, upper, this.esconfig.docsPerIndex)
            return agg[0].max_block.value;
        }

        const initialInterval = upperBound - lowerBound;

        this.logger.info(`starting full gap check from ${lowerBound} to ${upperBound}`);

        return this.checkGaps(lowerBound, upperBound, initialInterval);
    }

    async _deleteBlocksInRange(startBlock: number, endBlock: number) {
        const targetSuffix = this.getSuffixForBlock(endBlock);
        const deltaIndex = `${this.chainName}-${this.esconfig.suffix.delta}-${targetSuffix}`;
        const actionIndex = `${this.chainName}-${this.esconfig.suffix.transaction}-${targetSuffix}`;

        try {
            await this._deleteFromIndex(deltaIndex, 'block_num', startBlock, endBlock);
            await this._deleteFromIndex(
                actionIndex, '@raw.block', startBlock - this.config.chain.evmBlockDelta, endBlock - this.config.chain.evmBlockDelta);
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
            index: `${this.config.chain.chainName}-${this.esconfig.suffix.delta}-*`,
            format: 'json'
        });

        for (const deltaIndex of deltaIndices)
            if (indexToSuffixNum(deltaIndex.index) > targetNum)
                deleteList.push(deltaIndex.index);

        const actionIndices = await this.elastic.cat.indices({
            index: `${this.config.chain.chainName}-${this.esconfig.suffix.transaction}-*`,
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
        const txIndex = `${this.chainName}-${this.esconfig.suffix.transaction}-${suffix}`;
        const dtIndex = `${this.chainName}-${this.esconfig.suffix.delta}-${suffix}`;
        const errIndex = `${this.chainName}-${this.esconfig.suffix.error}-${suffix}`;

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
            this.opDrain.length >= (this.esconfig.dumpSize * 2)) {
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
        const frkIndex = `${this.chainName}-${this.esconfig.suffix.fork}-${suffix}`;
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

        const lastAdjusted = Math.floor(last / this.esconfig.docsPerIndex);

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

    blockScroll(params: {
        from: number,
        to: number,
        tag: string,
        logLevel?: string,
        validate?: boolean,
        scrollOpts?: any
    }) {
        return new ElasticScroller(this, params);
    }

}
