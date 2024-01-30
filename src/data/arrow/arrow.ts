import {ConnectorConfig, IndexedBlockInfo, ArrowConnectorConfig} from "../../types/indexer.js";
import {BlockData, BlockScroller, Connector} from "../connector.js";
import {BLOCK_GAS_LIMIT_HEX, EMPTY_TRIE} from "../../utils/evm.js";
import {StorageEosioDelta} from "../../types/evm.js";
import {sleep} from "../../utils/indexer.js";

import {promises as fs} from 'node:fs';
import path from "node:path";


import moment from "moment";

import {
    Table,
    Field,
    Schema,
    Uint32,
    Uint64,
    Utf8,
    tableFromArrays,
    Struct,
    StructRowProxy,
    StructRow,
    makeData,
    Data, RecordBatchWriter,
} from 'apache-arrow';
import pl from "nodejs-polars";


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

export const BlockFields = [
    Field.new('timestamp', new Uint64()),
    Field.new('block_num', new Uint64),
    Field.new('block_hash', new Utf8()),
    Field.new('evm_block_hash', new Utf8()),
    Field.new('evm_prev_block_hash', new Utf8()),
    Field.new('receipts_hash', new Utf8()),
    Field.new('txs_hash', new Utf8()),
    Field.new('gas_used', new Uint64()),
    Field.new('txs_amount', new Uint32()),
    Field.new('size', new Uint32)
];

export const BlockSchema = new Schema(BlockFields);

export type BlockStructType = {
    timestamp: Uint64;
    block_num: Uint64;
    block_hash: Utf8;
    evm_block_hash: Utf8;
    evm_prev_block_hash: Utf8;
    receipts_hash: Utf8;
    txs_hash: Utf8;
    gas_used: Uint64;
    txs_amount: Uint32;
    size: Uint32;
};

function makeBlockData(): Data<Struct<BlockStructType>> {
    return makeData({type: new Struct<BlockStructType>(BlockFields)});
}

function makeBlockStructRow(index: number): StructRowProxy<BlockStructType> {
    return new StructRow(makeBlockData(), index) as StructRowProxy<BlockStructType>;
}

// export const BlockSchema = {
//     timestamp: pl.UInt64,
//     block_num: pl.UInt64,
//     block_hash: pl.Utf8,  // 64 char lengh hex hash
//     evm_block_hash: pl.Utf8,
//     evm_prev_block_hash: pl.Utf8,
//     receipts_hash: pl.Utf8,
//     txs_hash: pl.Utf8,
//     gas_used: pl.UInt64,
//     txs_amount: pl.UInt32,
//     size: pl.UInt32
// };
//
// export const TransactionSchema = {
//     timestamp: pl.UInt64,
//     trx_id: pl.Utf8,  // native tx hash
//     action_ordinal: pl.UInt32,
//     hash: pl.Utf8,  // evm tx hash
//     from: pl.Utf8,
//     trx_index: pl.UInt32,
//     block_num: pl.UInt64,  // evm block num
//     to: pl.Utf8,
//     input: pl.Utf8,
//     value: pl.Utf8,
// };

// export class ParquetScroller extends BlockScroller {
//
//     private readonly conn: PolarsConnector;
//
//     private _currentReadBucket: number;
//     private _lastYielded: number;
//
//     private _currentBucket: pl.DataFrame;
//     private _currentIter: Generator;
//
//     constructor(
//         connector: PolarsConnector,
//         params: {
//             from: number,
//             to: number,
//             tag: string
//             logLevel?: string,
//             validate?: boolean,
//             scrollOpts?: {}
//         }
//     ) {
//         super();
//         this.conn = connector;
//         this.from = params.from;
//         this.to = params.to;
//         this.tag = params.tag;
//
//         this._currentReadBucket = this.conn._adjustBlockNum(this.from);
//         this._lastYielded = this.from - 1;
//     }
//
//     async init(): Promise<void> {
//         this._currentBucket = await this.conn.getLazyFrameForBlock(this.from).collect();
//         this._currentIter = this._currentBucket[Symbol.iterator]();
//     }
//
//     private async nextRow() {
//         const result = this._currentIter.next();
//         if (result.done)
//             return result.value;
//         else {
//             if (this._currentReadBucket == this.conn._adjustBlockNum(this.to))
//                 throw new Error(`Read all source data but couldnt reach ${this.to}`);
//
//             this._currentReadBucket++;
//             this._currentBucket = await this.conn.getLazyFrameForBlock(this._lastYielded + 1).collect();
//             this._currentIter = this._currentBucket[Symbol.iterator]();
//
//             const res = this._currentIter.next();
//
//             if (!res.done)
//                 throw new Error(`Is ${this.conn.getBlockBucketFor(this._lastYielded + 1)} empty?`);
//
//             return res.value;
//         }
//     }
//
//     async nextResult(): Promise<BlockData> {
//         if (!this._isInit) throw new Error('Must call init() before nextResult()!');
//
//         const block = this.conn._deltaFromRow(await this.nextRow());
//         this._lastYielded = block.block_num;
//         this._isDone = this._lastYielded == this.to;
//         return {block, actions: []};
//     }
// }

export class ArrowConnector extends Connector {

    readonly pconfig: ArrowConnectorConfig;

    private _currentWriteBucket: number = 0;

    private _blocks:  RecordBatchWriter<BlockStructType>;
    private _blocksLength: number;
    private _interBuff: {
        timestamp: bigint[];
        block_num: bigint[];
        block_hash: string[];
        evm_block_hash: string[];
        evm_prev_block_hash: string[];
        receipts_hash: string[];
        txs_hash: string[];
        gas_used: bigint[];
        txs_amount: number[];
        size: number[];
    };

    // bucket file suffix: 8 digit 0 prefixed integer based on block_num

    // block bucket naming: blocks-XXXXXXXX
    private _blockBuckets: string[] = [];

    // tx bucket naming: txs-XXXXXXXX
    private _txBuckets: string[] = [];

    private firstBlock: StorageEosioDelta;
    private lastBlock: StorageEosioDelta;

    private maxWrites = 2;
    private _wipWrites = new Set<string>();

    constructor(config: ConnectorConfig) {
        super(config);

        if (!config.arrow)
            throw new Error(`Tried to init polars connector with null config`);

        this.pconfig = config.arrow;

        if (!this.pconfig.bucketSize)
            this.pconfig.bucketSize = 1e7;

        this._initWriter();
        this._initIntermediateBuff();
    }

    private _initWriter() {
        this._blocks = new RecordBatchWriter<BlockStructType>({
            autoDestroy: false
        });
        this._blocksLength = 0;
    }

    private _initIntermediateBuff() {
        this._interBuff = {
            timestamp: [],
            block_num: [],
            block_hash: [],
            evm_block_hash: [],
            evm_prev_block_hash: [],
            receipts_hash: [],
            txs_hash: [],
            gas_used: [],
            txs_amount: [],
            size: []
        };
    }

    private _addDeltaToIntermediate(delta: StorageEosioDelta) {
        let receiptHash = EMPTY_TRIE;
        if (delta["@receiptsRootHash"])
            receiptHash = delta["@receiptsRootHash"];

        let txsHash = EMPTY_TRIE;
        if (delta["@transactionsRoot"])
            txsHash = delta["@transactionsRoot"];

        let gasUsed = 0;
        if (delta.gasUsed)
            gasUsed = parseInt(delta.gasUsed, 10);

        this._interBuff.timestamp.push(BigInt(moment.utc(delta["@timestamp"]).unix()));
        this._interBuff.block_num.push(BigInt(delta.block_num));
        this._interBuff.block_hash.push(delta["@blockHash"]);
        this._interBuff.evm_block_hash.push(delta["@evmBlockHash"]);
        this._interBuff.evm_prev_block_hash.push(delta['@evmPrevBlockHash']);
        this._interBuff.receipts_hash.push(receiptHash);
        this._interBuff.txs_hash.push(txsHash);
        this._interBuff.gas_used.push(BigInt(gasUsed));
        this._interBuff.txs_amount.push(delta.txAmount);
        this._interBuff.size.push(parseInt(delta.size, 10));
    }

    _unwrapRow(row: any[], schema: Schema): Record<string, any> {
        const unwrappedObject: Record<string, any> = {};

        schema.names.forEach((columnName, index) => {
            unwrappedObject[columnName.toString()] = row[index];
        });

        return unwrappedObject;
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

    _structProxyFromDelta(index: number, delta: StorageEosioDelta): StructRowProxy<BlockStructType> {
        let receiptHash = EMPTY_TRIE;
        if (delta["@receiptsRootHash"])
            receiptHash = delta["@receiptsRootHash"];

        let txsHash = EMPTY_TRIE;
        if (delta["@transactionsRoot"])
            txsHash = delta["@transactionsRoot"];

        let gasUsed = 0;
        if (delta.gasUsed)
            gasUsed = parseInt(delta.gasUsed, 10);

        const row = makeBlockStructRow(index);

        row.timestamp = BigInt(moment.utc(delta["@timestamp"]).unix());
        row.block_num = BigInt(delta.block_num);
        row.block_hash = delta["@blockHash"];
        row.evm_block_hash = delta["@evmBlockHash"];
        row.evm_prev_block_hash = delta['@evmPrevBlockHash'];
        row.receipts_hash = receiptHash;
        row.txs_hash = txsHash;
        row.gas_used = BigInt(gasUsed);
        row.txs_amount = delta.txAmount;
        row.size = parseInt(delta.size, 10);

        return row;
    }

    _tableFromArrays(columns: typeof this._interBuff): Table {
        return tableFromArrays({
            timestamp: BigUint64Array.from(columns.timestamp),
            block_num: BigUint64Array.from(columns.block_num),
            block_hash: columns.block_hash,
            evm_block_hash: columns.evm_block_hash,
            evm_prev_block_hash: columns.evm_prev_block_hash,
            receipts_hash: columns.receipts_hash,
            txs_hash: columns.txs_hash,
            gas_used: BigUint64Array.from(columns.gas_used),
            txs_amount: Uint32Array.from(columns.txs_amount),
            size: Uint32Array.from(columns.size)
        });
    }

    _rowFromCurrentSeries(blockNum: number): BlockRow {
        const seriesStart = this._currentWriteBucket * this.pconfig.bucketSize;
        const index = blockNum - seriesStart;

        if (index < 0 || index >= this._blocksLength)
            throw new Error(`Tried to get row out of series bounds!`);

        return  ['unimplemented'] as unknown as BlockRow;
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
        return `blocks-${this.getSuffixForBlock(blockNum)}.arrow`;
    }

    private _getTxsBucketNameFor(blockNum: number) {
        return `txs-${this.getSuffixForBlock(blockNum)}.arrow`;
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
            throw new Error(`Unimplemented`);

        const bucket = this.getBlockBucketFor(blockNum);
        if (!bucket)
            return pl.DataFrame({}).lazy();

        return pl.scanIPC(bucket);
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
            if (file.includes(`.arrow`)) {
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

        // if (this._blockBuckets.length > 0) {
        //     this.firstBlock = tableFromIPC(
        //         path.join(this.pconfig.dataDir, this._blockBuckets[0])).get(0);

        //     const lastBucketName = this._blockBuckets[this._blockBuckets.length - 1];
        //     const lastBucket = pl.scanParquet(path.join(this.pconfig.dataDir, lastBucketName));
        //     this.lastBlock = (await lastBucket.last().fetch(1)).row(0) as BlockRow;
        //     this.lastPushed = this.lastBlock[1];

        //     this._currentWriteBucket = this._bucketNameToNum(lastBucketName);

        //     if (this._adjustBlockNum(this.lastPushed + 1) > this._currentWriteBucket)
        //         this._currentWriteBucket++;

        //     else {
        //         this.logger.info(`Loading unfinished bucket ${lastBucketName}...`);
        //         const lastDf = pl.readParquet(
        //             path.join(this.pconfig.dataDir, lastBucketName));

        //         this._blocks;
        //     }
        // }

        return await super.init();
    }

    async getBlockRange(from: number, to: number): Promise<BlockData[]> {
        return [];
    }

    async getFirstIndexedBlock(): Promise<StorageEosioDelta | null> {
        return null;
        // if (!this.firstBlock) return null;

        // return this._deltaFromRow(this.firstBlock);
    }

    async getIndexedBlock(blockNum: number): Promise<StorageEosioDelta | null> {
        // const df = await this.getLazyFrameForBlock(blockNum)
        //     .filter(
        //         pl.col('block_num').eq(blockNum)
        //             ).fetch(1);

        // if (df.height == 1)
        //     return this._deltaFromFrame(df);

        // else if (df.height == 0)
        //     return null;

        // else
        //     throw new Error(`scan returned multiple results for single block_num!`);
        throw new Error(`Unimplemented`);
    }

    async getLastIndexedBlock(): Promise<StorageEosioDelta> {
        return null;
        // if (!this.lastBlock) return null;

        // return this._deltaFromRow(this.lastBlock);
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
        const blocksBuff = this._blocks.toUint8Array(true);
        this._initWriter();
        this._wipWrites.add(blockBucket);

        if (this._wipWrites.size > this.maxWrites)
            while (this._wipWrites.size > this.maxWrites) {
                this.logger.info(`awating space in write queue for ${blockBucket}`);
                await sleep(200);
            }
        else
            await sleep(0);  // pump event loop;

        this.logger.info(`start writing to ${bucketPath}, wip writes len: ${this._wipWrites.size}`);
        await fs.writeFile(bucketPath, blocksBuff);
        this.logger.info(`wrote batch ${bucketPath}, wip writes len: ${this._wipWrites.size}`);
        this._blockBuckets.push(blockBucket);
        this._wipWrites.delete(blockBucket);
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
        throw new Error(`unimplemented`);
        // for (let i = 0; i < 10; i++)
        //     this._blocks.columns[i].values = this._blocks.columns[i].values.slice(0, targetIndex);
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
        await this.finishBucket();
    }

    async pushBlock(blockInfo: IndexedBlockInfo): Promise<void> {
        this.lastBlock = blockInfo.delta;
        this._blocksLength++;
        this.lastPushed = this.lastBlock.block_num;

        this._addDeltaToIntermediate(this.lastBlock);

        if (this.lastPushed % (1e4 - 1) == 0) {
            this._blocks.write(this._tableFromArrays(this._interBuff).batches);
            this._initIntermediateBuff();
        }

        if (this.lastPushed % (this.pconfig.bucketSize - 1) == 0)
            await this.finishBucket();
    }

    forkCleanup(timestamp: string, lastNonForked: number, lastForked: number): void {
        this.purgeMemoryFrom(lastNonForked + 1);
        // this.lastPushed = lastNonForked;
        // this.lastBlock = this._rowFromCurrentSeries(lastNonForked);
    }

    blockScroll(params: {
        from: number;
        to: number;
        tag: string;
        logLevel?: string;
        validate?: boolean;
        scrollOpts?: any
    }): BlockScroller {
        return null; // return new ParquetScroller(this, params);
    }
}