import {ConnectorConfig, IndexedBlockInfo, PolarsConnectorConfig} from "../../types/indexer.js";
import {BlockData, BlockScroller, Connector} from "../connector.js";
import {BLOCK_GAS_LIMIT_HEX, EMPTY_TRIE} from "../../utils/evm.js";
import {StorageEosioDelta} from "../../types/evm.js";
import {sleep} from "../../utils/indexer.js";

import {promises as fs} from 'node:fs';
import path from "node:path";


import pl, {DataType} from 'nodejs-polars';
import moment from "moment";
import workerpool from 'workerpool';


export function _columnarObj(schema: Record<string, DataType>): Record<string, any[]> {
    const unwrappedObject: Record<string, any> = {};

    Object.keys(schema).forEach(columnName => {
        unwrappedObject[columnName] = [];
    });

    return unwrappedObject;
}


export type BlockRow = [
    number,  // 64
    number,  // 64
    string,  // 256 hash
    string,  // 256
    string,  // 256
    string,  // 256
    string,  // 256
    number,  // 64
    number,  // 32
    number,  // 32
];

export interface BlockJSONDataFrame {
    columns: {
        name: string;
        datatype: string;
        bit_settings: 'SORTED_ASC';
        values: any[];
    }[]
}

export interface BlockObject {
    timestamp: number;
    block_num: number;
    block_hash: string;
    evm_block_hash: string;
    evm_prev_block_hash: string;
    receipts_hash: string;
    txs_hash: string;
    gas_used: number;
    txs_amount: number;
    size: number;
};

export const BlockSchema = {
    timestamp: pl.UInt64,
    block_num: pl.UInt64,
    block_hash: pl.Utf8,  // 64 char lengh hex hash
    evm_block_hash: pl.Utf8,
    evm_prev_block_hash: pl.Utf8,
    receipts_hash: pl.Utf8,
    txs_hash: pl.Utf8,
    gas_used: pl.UInt64,
    txs_amount: pl.UInt32,
    size: pl.UInt32
};

export const TransactionSchema = {
    timestamp: pl.UInt64,
    trx_id: pl.Utf8,  // native tx hash
    action_ordinal: pl.UInt32,
    hash: pl.Utf8,  // evm tx hash
    from: pl.Utf8,
    trx_index: pl.UInt32,
    block_num: pl.UInt64,  // evm block num
    to: pl.Utf8,
    input: pl.Utf8,
    value: pl.Utf8,
};

export class ParquetScroller extends BlockScroller {

    private readonly conn: PolarsConnector;

    private _currentReadBucket: number;
    private _lastYielded: number;

    private _currentBucket: pl.DataFrame;
    private _currentIter: Generator;

    constructor(
        connector: PolarsConnector,
        params: {
            from: number,
            to: number,
            tag: string
            logLevel?: string,
            validate?: boolean,
            scrollOpts?: {}
        }
    ) {
        super();
        this.conn = connector;
        this.from = params.from;
        this.to = params.to;
        this.tag = params.tag;

        this._currentReadBucket = this.conn._adjustBlockNum(this.from);
        this._lastYielded = this.from - 1;
    }

    async init(): Promise<void> {
        this._currentBucket = await this.conn.getLazyFrameForBlock(this.from).collect();
        this._currentIter = this._currentBucket[Symbol.iterator]();
    }

    private async nextRow() {
        const result = this._currentIter.next();
        if (result.done)
            return result.value;
        else {
            if (this._currentReadBucket == this.conn._adjustBlockNum(this.to))
                throw new Error(`Read all source data but couldnt reach ${this.to}`);

            this._currentReadBucket++;
            this._currentBucket = await this.conn.getLazyFrameForBlock(this._lastYielded + 1).collect();
            this._currentIter = this._currentBucket[Symbol.iterator]();

            const res = this._currentIter.next();

            if (!res.done)
                throw new Error(`Is ${this.conn.getBlockBucketFor(this._lastYielded + 1)} empty?`);

            return res.value;
        }
    }

    async nextResult(): Promise<BlockData> {
        if (!this._isInit) throw new Error('Must call init() before nextResult()!');

        const block = this.conn._deltaFromRow(await this.nextRow());
        this._lastYielded = block.block_num;
        this._isDone = this._lastYielded == this.to;
        return {block, actions: []};
    }
}

export class PolarsConnector extends Connector {

    readonly pconfig: PolarsConnectorConfig;

    private _currentWriteBucket: number = 0;

    private _blocks: BlockJSONDataFrame;
    private _blocksLength: number;

    // bucket file suffix: 8 digit 0 prefixed integer based on block_num

    // block bucket naming: blocks-XXXXXXXX
    private _blockBuckets: string[] = [];

    // tx bucket naming: txs-XXXXXXXX
    private _txBuckets: string[] = [];

    private firstBlock: BlockRow;
    lastBlock: BlockRow;

    private writer;
    private maxWrites = 2;
    private _wipWrites = new Set<string>();

    constructor(config: ConnectorConfig) {
        super(config);

        if (!config.polars)
            throw new Error(`Tried to init polars connector with null config`);

        this.pconfig = config.polars;

        if (!this.pconfig.bucketSize)
            this.pconfig.bucketSize = 1e7;

        if (this.pconfig.maxWrites)
            this.maxWrites = this.pconfig.maxWrites;

        if (!this.pconfig.format)
            this.pconfig.format = 'parquet';

        this._initBuffer();
        this.writer = workerpool.pool('./build/data/polars/writer.js', {minWorkers: this.maxWrites});
    }

    private _initBuffer() {
        this._blocks = {
            columns: [
                {name: 'timestamp',           datatype: 'UInt64', bit_settings: 'SORTED_ASC', values: []},
                {name: 'block_num',           datatype: 'UInt64', bit_settings: 'SORTED_ASC', values: []},
                {name: 'block_hash',          datatype: 'Utf8',   bit_settings: 'SORTED_ASC', values: []},
                {name: 'evm_block_hash',      datatype: 'Utf8',   bit_settings: 'SORTED_ASC', values: []},
                {name: 'evm_prev_block_hash', datatype: 'Utf8',   bit_settings: 'SORTED_ASC', values: []},
                {name: 'receipts_hash',       datatype: 'Utf8',   bit_settings: 'SORTED_ASC', values: []},
                {name: 'txs_hash',            datatype: 'Utf8',   bit_settings: 'SORTED_ASC', values: []},
                {name: 'gas_used',            datatype: 'UInt64', bit_settings: 'SORTED_ASC', values: []},
                {name: 'txs_amount',          datatype: 'UInt32', bit_settings: 'SORTED_ASC', values: []},
                {name: 'size'    ,            datatype: 'UInt32', bit_settings: 'SORTED_ASC', values: []}
            ]
        };
        this._blocksLength = 0;
    }

    _unwrapRow(row: any[], schema: Record<string, DataType>): Record<string, any> {
        const unwrappedObject: Record<string, any> = {};

        Object.keys(schema).forEach((columnName, index) => {
            unwrappedObject[columnName] = row[index];
        });

        return unwrappedObject;
    }

    _unwrapFrame(df: pl.DataFrame): Record<string, any> {
        if (df.height != 1)
            throw new Error("DataFrame contains multiple rows. Can't unwrap.");

        return this._unwrapRow(df.row(0), df.schema);
    }

    _deltaFromUnwrapped(obj: BlockObject): StorageEosioDelta {
        return {
            '@timestamp': new Date(obj.timestamp).toISOString(),
            block_num: obj.block_num,
            '@global': {
                block_num: obj.block_num - this.config.chain.evmBlockDelta
            },
            '@blockHash': obj.block_hash,
            '@evmBlockHash': obj.evm_block_hash,
            '@evmPrevBlockHash': obj.evm_prev_block_hash,
            '@receiptsRootHash': obj.receipts_hash ? obj.receipts_hash : EMPTY_TRIE,
            '@transactionsRoot': obj.txs_hash ? obj.txs_hash : EMPTY_TRIE,
            'gasUsed': obj.gas_used ? String(obj.gas_used) : '0',
            'gasLimit': BLOCK_GAS_LIMIT_HEX,
            'txAmount': obj.txs_amount,
            'size': String(obj.size)
        }
    }

    _deltaFromFrame(df: pl.DataFrame): StorageEosioDelta {
        return this._deltaFromUnwrapped(this._unwrapFrame(df) as BlockObject);
    }

    _deltaFromRow(row: any[]): StorageEosioDelta {
        return this._deltaFromUnwrapped(this._unwrapRow(row, BlockSchema) as BlockObject);
    }

    _rowFromDelta(delta: StorageEosioDelta): BlockRow {
        let receiptHash = EMPTY_TRIE;
        if (delta["@receiptsRootHash"])
            receiptHash = delta["@receiptsRootHash"];

        let txsHash = EMPTY_TRIE;
        if (delta["@transactionsRoot"])
            txsHash = delta["@transactionsRoot"];

        let gasUsed = 0;
        if (delta.gasUsed)
            gasUsed = parseInt(delta.gasUsed, 10);

        return [
            moment.utc(delta["@timestamp"]).unix(),
            delta.block_num,
            delta["@blockHash"],
            delta["@evmBlockHash"],
            delta['@evmPrevBlockHash'],
            receiptHash,
            txsHash,
            gasUsed,
            delta.txAmount,
            parseInt(delta.size, 10)
        ];
    }

    _rowFromCurrentSeries(blockNum: number): BlockRow {
        const seriesStart = this._currentWriteBucket * this.pconfig.bucketSize;
        const index = blockNum - seriesStart;

        if (index < 0 || index >= this._blocksLength)
            throw new Error(`Tried to get row out of series bounds!`);

        const row = [];
        for (let i = 0; i < 10; i++)
            row.push(this._blocks.columns[i].values[index]);

        return row as BlockRow;
    }

    _adjustBlockNum(blockNum: number) {
        return Math.floor(blockNum / this.pconfig.bucketSize);
    }

    getSuffixForBlock(blockNum: number) {
        return String(this._adjustBlockNum(blockNum)).padStart(8, '0');
    }

    private _bucketNameToNum(bucketName: string): number {
        const match = bucketName.match(/\d+/);
        return match ? parseInt(match[0], 10) : NaN;
    }

    private _getBlockBucketNameFor(blockNum: number) {
        return `blocks-${this.getSuffixForBlock(blockNum)}.${this.pconfig.format}`;
    }

    private _getTxsBucketNameFor(blockNum: number) {
        return `txs-${this.getSuffixForBlock(blockNum)}.${this.pconfig.format}`;
    }

    getBlockBucketFor(blockNum: number): string | undefined {
        const bucketName = this._getBlockBucketNameFor(blockNum);
        if (!this._blockBuckets.includes(bucketName)) return undefined;

        return bucketName;
    }

    getTxsBucketFor(blockNum: number): string | undefined {
        const bucketName = this._getTxsBucketNameFor(blockNum);
        if (!this._txBuckets.includes(bucketName)) return undefined;

        return bucketName;
    }

    getLazyFrameForBlock(blockNum: number): pl.LazyDataFrame {
        if (this._currentWriteBucket == this._adjustBlockNum(blockNum))
            return pl.DataFrame(this._blocks, {schema: BlockSchema}).lazy();

        const bucket = this.getBlockBucketFor(blockNum);
        if (!bucket)
            return pl.DataFrame({}, {schema: BlockSchema}).lazy();

        return pl.scanParquet(bucket);
    }

    async reloadOnDiskBuckets() {

        const blockBuckets = [];
        const txsBuckets = [];

        const sortNameFn = (a, b) => {
            const aNum = this._bucketNameToNum(a);
            const bNum = this._bucketNameToNum(b);
            if (aNum < bNum)
                return -1;
            if (aNum > bNum)
                return 1;
            return 0;
        };

        const dataDirFiles = await fs.readdir(this.pconfig.dataDir);
        dataDirFiles.sort(sortNameFn);

        for (const file of dataDirFiles) {
            if (file.includes(`.${this.pconfig.format}`)) {
                if (file.includes('blocks'))
                    blockBuckets.push(file);
                else if (file.includes('txs'))
                    txsBuckets.push(file);
            }
        }

        this._blockBuckets = blockBuckets;
        this._txBuckets = txsBuckets;
    }

    async init(): Promise<number | null> {

        try {
            await fs.mkdir(this.pconfig.dataDir, {recursive: true});
        } catch (e) {
            this.logger.error(e.message);
        }

        await this.reloadOnDiskBuckets();

        if (this._blockBuckets.length > 0) {
            this.firstBlock = pl.scanParquet(
                path.join(this.pconfig.dataDir, this._blockBuckets[0])).first().row(0) as BlockRow;

            const lastBucketName = this._blockBuckets[this._blockBuckets.length - 1];
            const lastBucket = pl.scanParquet(path.join(this.pconfig.dataDir, lastBucketName));
            this.lastBlock = (await lastBucket.last().fetch(1)).row(0) as BlockRow;
            this.lastPushed = this.lastBlock[1];

            this._currentWriteBucket = this._bucketNameToNum(lastBucketName);

            if (this._adjustBlockNum(this.lastPushed + 1) > this._currentWriteBucket)
                this._currentWriteBucket++;

            else {
                this.logger.info(`Loading unfinished bucket ${lastBucketName}...`);
                const lastDf = pl.readParquet(
                    path.join(this.pconfig.dataDir, lastBucketName));

                this._blocks;
            }
        }

        return await super.init();
    }

    async getBlockRange(from: number, to: number): Promise<BlockData[]> {
        return [];
    }

    async getFirstIndexedBlock(): Promise<StorageEosioDelta | null> {
        if (!this.firstBlock) return null;

        return this._deltaFromRow(this.firstBlock);
    }

    async getIndexedBlock(blockNum: number): Promise<StorageEosioDelta | null> {
        const df = await this.getLazyFrameForBlock(blockNum)
            .filter(
                pl.col('block_num').eq(blockNum)
                    ).fetch(1);

        if (df.height == 1)
            return this._deltaFromFrame(df);

        else if (df.height == 0)
            return null;

        else
            throw new Error(`scan returned multiple results for single block_num!`);
    }

    async getLastIndexedBlock(): Promise<StorageEosioDelta> {
        if (!this.lastBlock) return null;

        return this._deltaFromRow(this.lastBlock);
    }

    private async finishBucket() {
        if (this._blocksLength == 0)
            return;

        const blockBucket = this._getBlockBucketNameFor(this.lastPushed);

        if (this._wipWrites.has(blockBucket)) {
            this.logger.warn(`Tried to write ${blockBucket} twice!`);
            return;
        }
        const bucketPath = path.join(this.pconfig.dataDir, blockBucket);

        this._currentWriteBucket = this._adjustBlockNum(this.lastPushed);
        const blocks = this._blocks;
        this._initBuffer();
        this._wipWrites.add(blockBucket);

        const start = performance.now();
        this.logger.info(`loading data into df...`)
        const df = pl.DataFrame(
            blocks, {schema: BlockSchema});

        this.logger.info(`done, writing ${bucketPath}...`)
        const writeOptions: {compression: 'zstd'} = {compression: 'zstd'};
        switch (this.pconfig.format) {
            case "parquet": {
                df.writeParquet(bucketPath, writeOptions);
                break;
            }
            case "ipc": {
                df.writeIPC(bucketPath, writeOptions);
                break;
            }
            case "csv": {
                df.writeCSV(bucketPath);
                break;
            }
        }
        this._wipWrites.delete(blockBucket);
        this.logger.info(`done writing ${bucketPath}, took ${performance.now() - start} ms`);
        // if (this._wipWrites.size > this.maxWrites)
        //     while (this._wipWrites.size > this.maxWrites) {
        //         this.logger.info(`awating space in write queue for ${blockBucket}`);
        //         await sleep(200);
        //     }
        // else
        //     await sleep(0);  // pump event loop;

        // this.logger.info(`start writing to ${bucketPath}, wip writes len: ${this._wipWrites.size}`);
        // this.writer.exec(
        //     'writeData',
        //     [blocks, rowAmount, {schema: BlockSchema}, bucketPath, this.pconfig.format, {compression: 'zstd'}]
        // ).then(() => {
        //     this.logger.info(`wrote batch ${bucketPath}, wip writes len: ${this._wipWrites.size}`);
        //     this._blockBuckets.push(blockBucket);
        //     this._wipWrites.delete(blockBucket);
        // }).catch((err) => {
        //     this.logger.error(err.message);
        //     this.logger.error(err.stack);
        //     throw err;
        // });
    }

    private async removeBucket(bucketName: string) {
        const bucketPath = path.join(this.pconfig.dataDir, bucketName);
        try {
            await fs.access(bucketPath);
            await fs.unlink(bucketPath);
        } catch (e) {
            if (e.code !== 'ENOENT')
                throw e;
        }

        this._blockBuckets = this._blockBuckets.filter(b => b !== bucketName);
        this._txBuckets = this._txBuckets.filter(b => b !== bucketName);
    }

    private purgeMemoryFrom(blockNum: number) {
        // purge on mem
        const bucketStartNum = this._currentWriteBucket * this.pconfig.bucketSize;
        const targetIndex = blockNum - bucketStartNum;

        this._blocksLength = targetIndex;
        for (let i = 0; i < 10; i++)
            this._blocks.columns[i].values = this._blocks.columns[i].values.slice(0, targetIndex);
    }

    async purgeNewerThan(blockNum: number): Promise<void> {
        // purge on disk
        await this.reloadOnDiskBuckets();

        const deleteList = [];
        for (const bucket of this._blockBuckets)
            if (this._bucketNameToNum(bucket) > this._adjustBlockNum(blockNum))
                deleteList.push(bucket);

        for (const bucket of this._txBuckets)
            if (this._bucketNameToNum(bucket) > this._adjustBlockNum(blockNum))
                deleteList.push(bucket);

        await Promise.all(deleteList.map(b => this.removeBucket(b)));

        this.purgeMemoryFrom(blockNum);
    }

    async fullIntegrityCheck(): Promise<number | null> { return null; }

    async flush(): Promise<void> {
        if (this._blocksLength > 0)
            if (!this._wipWrites.has(this.getBlockBucketFor(this.lastPushed)))
                await this.finishBucket();
    }

    async pushBlock(blockInfo: IndexedBlockInfo): Promise<void> {
        this.lastBlock = this._rowFromDelta(blockInfo.delta);

        this._blocksLength++;
        for (let i = 0; i < 10; i++)
            this._blocks.columns[i].values.push(this.lastBlock[i]);

        this.lastPushed = this.lastBlock[1];
        if (this.lastPushed % (this.pconfig.bucketSize - 1) == 0)
            await this.finishBucket();
    }

    forkCleanup(timestamp: string, lastNonForked: number, lastForked: number): void {
        this.purgeMemoryFrom(lastNonForked + 1);
        this.lastPushed = lastNonForked;
        this.lastBlock = this._rowFromCurrentSeries(lastNonForked);
    }

    blockScroll(params: {
        from: number;
        to: number;
        tag: string;
        logLevel?: string;
        validate?: boolean;
        scrollOpts?: any
    }): BlockScroller {
        return new ParquetScroller(this, params);
    }

    async deinit() {
        await super.deinit();

        if (this._wipWrites.size > 0) {
            this.logger.info(`Awating writes:\n ${JSON.stringify(
                [...this._wipWrites.values()], null, 4)}`);

            while (this._wipWrites.size > 0)
                await sleep(200);
        }

        await this.writer.terminate(true);
    }

}