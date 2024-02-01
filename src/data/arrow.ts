import {ConnectorConfig, IndexedBlockInfo, ArrowConnectorConfig} from "../types/indexer.js";
import {BlockData, BlockScroller, Connector} from "./connector.js";
import {BLOCK_GAS_LIMIT, EMPTY_TRIE} from "../utils/evm.js";
import {StorageEosioDelta} from "../types/evm.js";

import memfs from 'node:fs';
import {promises as fs} from 'node:fs';
import path from "node:path";


import moment from "moment";

import {
    Table,
    tableFromArrays,
    RecordBatch, RecordBatchFileWriter, tableFromIPC, RecordBatchFileReader,
} from 'apache-arrow';
import {finished} from "stream/promises";
import {bigintToUint8Array, MemoryStream} from "../utils/misc.js";

import {ZSTDCompress, ZSTDDecompress} from 'simple-zstd';
import {Readable} from "stream";


export enum ArrowBatchCompression {
    UNCOMPRESSED = 0,
    ZSTD = 1
};

export interface ArrowBatchGlobalHeader {
    versionConstant: string;
    batchSize: bigint;
    batchCount: bigint;
};

export interface ArrowBatchHeader {
    headerConstant: string;
    batchByteSize: bigint;
    compression: ArrowBatchCompression;
};

export interface ArrowBatchFileMetadata {
    header: ArrowBatchGlobalHeader,
    batches: {batch: ArrowBatchHeader, start: number, end: number}[]
};

export class ArrowBatchProtocol {
    /*
     * arrow-batch spec
     *
     * we need a binary format that allows us streaming new rows to a file
     * in a way the files can be arbitrarily large, but still retain fast
     * random access properties.
     *
     * arrow-batch format gives us this by sequentially appending random access
     * arrow tables of a specific batch size plus a small header before each,
     * to form a bigger table.
     *
     * file map:
     *
     *     global header: version constant + batch size (uint64) + total batch count (uint64)
     *
     *     batch #0 header: batch header constant + batch byte size (uint64) + compression (uint8)
     *     arrow random access file bytes...
     *
     *     batch #1 header
     *     arrow random access file bytes...
     *
     * constants:
     *     version constant: ascii ord `ARROW-BATCH1`
     *     compression enum:
     *         0 - uncompressed
     *         1 - zstd  # TODO: impl
     *
     * # streaming:
     * this structure can be streamed easily by reading each batch header as it comes in then
     * expect to read the new full arrow random access batch sent.
     *
     * # random access:
     * to do a random access on a disk arrow-batch file, first one would read the global header
     * then before reading any actual arrow table data, read all arrow batch headers in order by
     * seeking around on the file by the specified metadata values on the batch headers, once the
     * batch that contains the row we are looking for is reached we can read that batch and do
     * queries that only affect that small batch.
     */
    static readonly ARROW_BATCH_VERSION_CONSTANT = 'ARROW-BATCH1';
    static readonly GLOBAL_HEADER_SIZE = ArrowBatchProtocol.ARROW_BATCH_VERSION_CONSTANT.length + 8 + 8;

    static readonly ARROW_BATCH_HEADER_CONSTANT = 'ARROW-BATCH-TABLE';
    static readonly BATCH_HEADER_SIZE = ArrowBatchProtocol.ARROW_BATCH_HEADER_CONSTANT.length + 8 + 1;

    static newGlobalHeader(batchSize: bigint, batchCount: bigint): Uint8Array {
        const strBytes = new TextEncoder().encode(ArrowBatchProtocol.ARROW_BATCH_VERSION_CONSTANT);

        const batchSizeBytes = bigintToUint8Array(batchSize);
        const batchCountBytes = bigintToUint8Array(batchCount);

        const buffer = new Uint8Array(strBytes.length + batchSizeBytes.length + batchCountBytes.length);
        buffer.set(strBytes, 0);
        buffer.set(batchSizeBytes, strBytes.length);
        buffer.set(batchCountBytes, strBytes.length + batchSizeBytes.length);

        return buffer;
    }

    static newBatchHeader(batchSize: bigint, compression: ArrowBatchCompression) {
        const strBytes = new TextEncoder().encode(ArrowBatchProtocol.ARROW_BATCH_HEADER_CONSTANT);

        const batchSizeBytes = bigintToUint8Array(batchSize);
        const compressionByte = new Uint8Array([compression]);;

        const buffer = new Uint8Array(strBytes.length + batchSizeBytes.length + 1);
        buffer.set(strBytes, 0);
        buffer.set(batchSizeBytes, strBytes.length);
        buffer.set(compressionByte, strBytes.length + batchSizeBytes.length);

        return buffer;
    }

    static readGlobalHeader(buffer: Buffer): ArrowBatchGlobalHeader {
        const versionConstantLength = this.ARROW_BATCH_VERSION_CONSTANT.length;
        const versionConstantBytes = buffer.subarray(0, versionConstantLength);
        const versionConstant = new TextDecoder("utf-8").decode(versionConstantBytes);

        const batchSizeStart = versionConstantLength;
        const batchSize = buffer.readBigUInt64LE(batchSizeStart);

        const batchCountStart = batchSizeStart + 8;
        const batchCount = buffer.readBigUInt64LE(batchCountStart);

        return { versionConstant, batchSize, batchCount };
    }

    static readBatchHeader(buffer: Buffer): ArrowBatchHeader {
        const headerConstantLength = this.ARROW_BATCH_HEADER_CONSTANT.length;
        const headerConstantBytes = buffer.subarray(0, headerConstantLength);
        const headerConstant = new TextDecoder("utf-8").decode(headerConstantBytes);

        const sizeStart = headerConstantLength;
        const batchByteSize = buffer.readBigUInt64LE(sizeStart);
        const compression = buffer.readUint8(sizeStart + 8);

        return { headerConstant, batchByteSize, compression };
    }

    static async readFileMetadata(filePath: string): Promise<ArrowBatchFileMetadata> {
        const fileHandle = await fs.open(filePath, 'r');

        const globalHeaderBuff = Buffer.alloc(ArrowBatchProtocol.GLOBAL_HEADER_SIZE);
        const batchHeaderBuff = Buffer.alloc(ArrowBatchProtocol.BATCH_HEADER_SIZE);

        let offset = 0;
        await fileHandle.read(globalHeaderBuff, 0, globalHeaderBuff.length, offset);
        offset += ArrowBatchProtocol.GLOBAL_HEADER_SIZE;
        const globalHeader = ArrowBatchProtocol.readGlobalHeader(globalHeaderBuff);

        const metadata = {
            header: globalHeader,
            batches: []
        }

        for (let i = 0; i < globalHeader.batchCount; i++) {
            await fileHandle.read(batchHeaderBuff, 0, batchHeaderBuff.length, offset);
            const batch = ArrowBatchProtocol.readBatchHeader(batchHeaderBuff);
            const batchSize = parseInt(batch.batchByteSize.toString(), 10);
            offset += ArrowBatchProtocol.BATCH_HEADER_SIZE;
            metadata.batches.push({batch, start: offset, end: offset + batchSize - 1});
            offset += parseInt(batch.batchByteSize.toString(), 10);
        }

        return metadata;
    }

    static async readArrowBatchTable(filePath: string, metadata: ArrowBatchFileMetadata, batchIndex: number) {
        const batchMeta = metadata.batches[batchIndex];
        const readStream = memfs.createReadStream(filePath, {start: batchMeta.start, end: batchMeta.end});
        switch (batchMeta.batch.compression) {
            case ArrowBatchCompression.UNCOMPRESSED: {
                break;
            }
            case ArrowBatchCompression.ZSTD: {
                readStream.pipe(ZSTDDecompress())
                break;
            }
        }
        return tableFromIPC(await RecordBatchFileReader.from(readStream));
    }
}


export type BlockRow = [
    bigint,  // 64
    bigint,  // 64
    string,  // 256 hash
    string,  // 256
    string,  // 256
    string,  // 256
    string,  // 256
    bigint,  // 64
    number,  // 32
    number,  // 32
];

export class ArrowConnector extends Connector {

    readonly pconfig: ArrowConnectorConfig;

    private _currentWriteBucket: string;

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

    static readonly DEFAULT_BUCKET_SIZE = 1e7;
    static readonly DEFAULT_DUMP_SIZE = 1e5;

    private _streamIndex: number = 0;
    private _streamBuffers: Buffer[];
    private readonly MAX_BATCH_MEM_SIZE: number = 30 * 1024 * 1024;  // 30mb
    private readonly MAX_MEM_STREAMS: number = 2;

    constructor(config: ConnectorConfig) {
        super(config);

        if (!config.arrow)
            throw new Error(`Tried to init polars connector with null config`);

        this.pconfig = config.arrow;

        if (!this.pconfig.bucketSize)
            this.pconfig.bucketSize = ArrowConnector.DEFAULT_BUCKET_SIZE;

        if (!this.pconfig.dumpSize)
            this.pconfig.dumpSize = ArrowConnector.DEFAULT_DUMP_SIZE;

        this._streamBuffers = []
        for (let i = 0; i < this.MAX_MEM_STREAMS; i++)
            this._streamBuffers.push(Buffer.alloc(this.MAX_BATCH_MEM_SIZE));
    }

    private async _initArrowBatchFile() {
        const wipBucketPath = path.join(this.pconfig.dataDir, this._currentWriteBucket + '.wip');
        let alreadyExists = false;
        try {
            await fs.access(wipBucketPath);
            alreadyExists = true;

        } catch (e) {}

        if (alreadyExists)
            throw new Error(`Tried to init arrow batch file but already exists!`);

        await fs.writeFile(
            wipBucketPath,
            ArrowBatchProtocol.newGlobalHeader(
                BigInt(this.pconfig.dumpSize), BigInt(Math.floor(this.pconfig.bucketSize / this.pconfig.dumpSize)))
        );
    }

    private async _writeBatchToWip(batch: Buffer, compression: ArrowBatchCompression) {
        const currentBucket = this._currentWriteBucket + '.wip';

        const writeStream = memfs.createWriteStream(
            path.join(this.pconfig.dataDir, currentBucket), {flags: 'a'});

        let _batch: Buffer = batch;
        switch (compression) {
            case ArrowBatchCompression.UNCOMPRESSED: {
                break;
            };
            case ArrowBatchCompression.ZSTD: {
                _batch = await new Promise((resolve, reject) => {
                    const _stream = Readable.from(batch);
                    const compressor = ZSTDCompress(3);
                    const chunks = [];
                    _stream.pipe(compressor);
                    compressor.on('data', (chunk) => {
                        chunks.push(chunk);
                    });

                    compressor.on('end', () => {
                        resolve(Buffer.concat(chunks));
                    });

                    compressor.on('error', reject);
                });
                break;
            }
        }

        return new Promise<void>((resolve) => {
            writeStream.write(
                ArrowBatchProtocol.newBatchHeader(
                    BigInt(_batch.length), compression),
                () => {
                    writeStream.write(_batch, () => writeStream.end(() => resolve()));
                }
            );
        });
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

    _deltaFromTable(table: Table, index: number): StorageEosioDelta {
        const row = table.get(index).toArray() as BlockRow;
        return {
            '@timestamp': new Date(parseInt(row[0].toString(10), 10)).toISOString(),
            block_num: parseInt(row[1].toString(10), 10),
            '@global': {
                block_num: parseInt(row[1].toString(10), 10) - this.config.chain.evmBlockDelta
            },
            '@blockHash': row[2],
            '@evmBlockHash': row[3],
            '@evmPrevBlockHash': row[4],
            '@receiptsRootHash': row[5],
            '@transactionsRoot': row[6],
            gasUsed: row[7].toString(),
            gasLimit: BLOCK_GAS_LIMIT.toString(),
            txAmount: row[8],
            size: String(row[9])
        }
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

        if (this._blockBuckets.length > 0) {

            const lastBucketName = this._blockBuckets[this._blockBuckets.length - 1];
            this._currentWriteBucket = lastBucketName;

            const nextBucket = this._getBlockBucketNameFor(this.lastPushed + 1);
            if (nextBucket != this._currentWriteBucket)
                this._currentWriteBucket = nextBucket;

            else {
                this.logger.info(`Loading unfinished bucket ${lastBucketName}...`);
                const startLoadTime = performance.now();
                this.logger.info(`loaded, took ${performance.now() - startLoadTime}`);
            }
        } else {
            this._currentWriteBucket = this._getBlockBucketNameFor(this.config.chain.startBlock);
            await this._initArrowBatchFile();
            this._initIntermediateBuff();
        }

        return await super.init();
    }

    async getBlockRange(from: number, to: number): Promise<BlockData[]> {
        return [];
    }

    async getFirstIndexedBlock(): Promise<StorageEosioDelta | null> {
        if (!this.firstBlock) return null;

        return this.firstBlock;
    }

    async getIndexedBlock(blockNum: number): Promise<StorageEosioDelta | null> {
        throw new Error(`Unimplemented`);
    }

    async getLastIndexedBlock(): Promise<StorageEosioDelta> {
        if (!this.lastBlock) return null;

        return this.lastBlock;
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
        // const bucketStartNum = this._currentWriteBucket * this.pconfig.bucketSize;
        // const targetIndex = blockNum - bucketStartNum;

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

    async flush(): Promise<void> {}

    private async _writeBatchMaybeFile(interBuff: typeof this._interBuff, buffIndex: number) {
        const writeStream = new MemoryStream(this._streamBuffers[buffIndex], this.MAX_BATCH_MEM_SIZE);
        // pipe record batch writer through it
        const blocksWriter = RecordBatchFileWriter.throughNode();
        blocksWriter.pipe(writeStream);
        // write batch, flush buffers
        const table = this._tableFromArrays(interBuff);
        const batch: RecordBatch = table.batches[0];
        blocksWriter.write(batch);
        blocksWriter.end();
        await finished(blocksWriter);
        await finished(writeStream);
        await this._writeBatchToWip(
            writeStream.getBufferData(), ArrowBatchCompression.UNCOMPRESSED);

        // maybe finish bucket, and create new wip
        const nextBucket = this._getBlockBucketNameFor(this.lastPushed + 1);
        if (nextBucket !== this._currentWriteBucket) {
            await fs.rename(
                path.join(this.pconfig.dataDir, this._currentWriteBucket  + '.wip'),
                path.join(this.pconfig.dataDir, this._currentWriteBucket)
            );
            this.logger.info(`done writing ${this._currentWriteBucket}`);
            this._blockBuckets.push(this._currentWriteBucket);
            this._currentWriteBucket = nextBucket;
            await this._initArrowBatchFile();
        }
    }

    async pushBlock(blockInfo: IndexedBlockInfo): Promise<void> {
        this.lastBlock = blockInfo.delta;
        this.lastPushed = this.lastBlock.block_num;

        this._addDeltaToIntermediate(this.lastBlock);

        if ((this.lastPushed + 1) % this.pconfig.dumpSize == 0) {
            const interBuffs = this._interBuff;
            this._initIntermediateBuff();
            this._writeBatchMaybeFile(interBuffs, this._streamIndex).then();

            this._streamIndex++;
            if (this._streamIndex >= this.MAX_MEM_STREAMS)
                this._streamIndex = 0;
        }
    }

    forkCleanup(timestamp: string, lastNonForked: number, lastForked: number): void {
        this.purgeMemoryFrom(lastNonForked + 1);
        this.lastPushed = lastNonForked;
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
        return null;
    }
}