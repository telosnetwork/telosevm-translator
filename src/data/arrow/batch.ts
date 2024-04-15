import fs, {promises as pfs, readFileSync} from "node:fs";
import {ArrowConnectorConfig} from "../../types/indexer.js";
import path from "node:path";
import {format, Logger, loggers, transports} from "winston";
import {ArrowBatchProtocol, ArrowTableMapping, decodeRowValue} from "./protocol.js";
import {ROOT_DIR} from "../../utils/indexer.js";

import {Worker} from 'node:worker_threads';
import {WriterControlRequest, WriterControlResponse} from "./write-worker.js";
import {isWorkerLogMessage, WorkerLogMessage} from "../../utils/misc.js";
import {Table} from "apache-arrow";
import EventEmitter from "events";
import {BIGINT_1} from "@ethereumjs/util";
import {existsSync} from "fs";


interface TableBufferInfo {
    columns: Map<
        string,  // column name
        any[]
    >
}

type RowBuffers = Map<
    string,  // table name
    TableBufferInfo
>;

export interface ArrowBatchContextDef {
    root: {
        name?: string;
        ordinal: string;
        map: ArrowTableMapping[];
    },
    others: {[key: string]: ArrowTableMapping[]}
}

function generateMappingsFromDefs(definition: ArrowBatchContextDef) {
    const rootOrdField: ArrowTableMapping = {name: definition.root.ordinal, type: 'u64'};
    const rootMap = [rootOrdField, ...definition.root.map];

    const mappigns = {
        ...definition.others,
        root: rootMap
    };

    return new Map<string, ArrowTableMapping[]>(Object.entries(mappigns));
}

export class ArrowBatchContext {
    static readonly DEFAULT_BUCKET_SIZE = 1e7;
    static readonly DEFAULT_DUMP_SIZE = 1e5;
    static readonly DEFAULT_TABLE_CACHE = 10;

    readonly config: ArrowConnectorConfig;
    definition: ArrowBatchContextDef;
    readonly logger: Logger;

    events = new EventEmitter();

    // updated by reloadOnDiskBuckets, map adjusted num -> table name -> file name
    protected tableFileMap: Map<number, Map<string, string>>;

    // setup by parsing context definitions, defines data model
    protected tableMappings: Map<string, ArrowTableMapping[]>;

    protected _firstOrdinal: bigint;
    protected _lastOrdinal: bigint;

    constructor(
        config: ArrowConnectorConfig,
        logger: Logger
    ) {
        this.config = config;
        this.logger = logger;

        if (!this.config.writerLogLevel)
            this.config.writerLogLevel = 'INFO';

        if (!this.config.bucketSize)
            this.config.bucketSize = ArrowBatchContext.DEFAULT_BUCKET_SIZE;

        if (!this.config.dumpSize)
            this.config.dumpSize = ArrowBatchContext.DEFAULT_DUMP_SIZE;

        const contextDefsPath = path.join(this.config.dataDir, 'context.json');
        if (existsSync(contextDefsPath)) {
            const definition = JSON.parse(
                readFileSync(contextDefsPath).toString());

            this.definition = definition;
            this.tableMappings = generateMappingsFromDefs(definition);
        }
    }

    async init(startOrdinal: number | bigint) {
        try {
            await pfs.mkdir(this.config.dataDir, {recursive: true});
        } catch (e) {
            this.logger.error(e.message);
        }

        await this.reloadOnDiskBuckets();
    }

    getOrdinal(ordinal: number | bigint): number {
        ordinal = BigInt(ordinal);
        return Number(ordinal / BigInt(this.config.bucketSize));
    }

    getOrdinalSuffix(ordinal: number | bigint): string {
        return String(this.getOrdinal(ordinal)).padStart(8, '0');
    }

    private bucketToOrdinal(tableBucketName: string): number {
        if (tableBucketName.includes('.wip'))
            tableBucketName = tableBucketName.replace('.wip', '');

        const match = tableBucketName.match(/\d+/);
        return match ? parseInt(match[0], 10) : NaN;
    }

    private async loadTableFileMap(bucket: string) {
        const bucketFullPath = path.join(this.config.dataDir, bucket);
        const tableFiles = (
            await pfs.readdir(
                path.join(this.config.dataDir, bucket), {withFileTypes: true}))
            .filter(p => p.isFile() && p.name.endsWith('.ab'))
            .map(p => p.name);

        const tableFilesMap = new Map();
        for (const tableName of this.tableMappings.keys()) {
            let name = tableName;
            if (name === 'root')
                name = this.definition.root.name;

            const file = tableFiles.find(file => file == `${name}.ab`);
            if (file) {
                tableFilesMap.set(
                    tableName,
                    path.join(bucketFullPath, file)
                );
            }
        }
        this.tableFileMap.set(this.bucketToOrdinal(bucket), tableFilesMap);
    }

    async reloadOnDiskBuckets() {
        this.tableFileMap = new Map();
        const sortNameFn = (a: string, b: string) => {
            const aNum = this.bucketToOrdinal(a);
            const bNum = this.bucketToOrdinal(b);
            if (aNum < bNum)
                return -1;
            if (aNum > bNum)
                return 1;
            return 0;
        };

        const bucketDirs = (
            await pfs.readdir(
                this.config.dataDir, {withFileTypes: true}))
            .filter(p => p.isDirectory())
            .map(p => p.name)
            .sort(sortNameFn);

        await Promise.all(
            bucketDirs.map(bucket => this.loadTableFileMap(bucket)));
    }
}

export class ArrowBatchReader extends ArrowBatchContext {

    private tableCache = new Map<string, Table>();
    private cacheOrder: string[] = [];

    // intermediate holds the current table we are building
    protected _intermediateBuffers: RowBuffers = new Map<string, TableBufferInfo>();

    // on flush operations, data from intermediate gets pushed to auxiliary and intermediate is reset
    // auxiliary will get cleared as flush operations finish
    protected _auxiliaryBuffers: RowBuffers = new Map<string, TableBufferInfo>();

    protected _firstTable: Table;
    protected _lastTable: Table;

    private isFirstUpdate: boolean = true;

    constructor(
        config: ArrowConnectorConfig,
        logger: Logger
    ) {
        super(config, logger);

        this._intermediateBuffers = this._initBuffer();
        this._initIntermediate();
    }

    get firstOrdinal(): bigint {
        return this._firstOrdinal;
    }

    get lastOrdinal(): bigint {
        return this._lastOrdinal;
    }

    protected _initBuffer() {
        const buffers = new Map<string, TableBufferInfo>();
        for (const [tableName, tableMapping] of this.tableMappings.entries()) {
            buffers.set(
                tableName, {columns: new Map<string, any[]>()});

            const tableBuffers = buffers.get(tableName);
            for (const mapping of tableMapping)
                tableBuffers.columns.set(mapping.name, []);
        }
        return buffers;
    }

    protected _initIntermediate() {
        this._auxiliaryBuffers = this._intermediateBuffers;
        this._intermediateBuffers = this._initBuffer();
        this.logger.debug(`initialized buffers for ${[...this._intermediateBuffers.keys()]}`);
    }

    async init(startOrdinal: number | bigint) {
        await super.init(startOrdinal);

        if (this.tableFileMap.size > 0) {
            const lowestBucket = [...this.tableFileMap.keys()]
                .sort()[0];

            const highestBucket = [...this.tableFileMap.keys()]
                .sort()
                .reverse()[0];

            const rootFirstPath = this.tableFileMap.get(lowestBucket).get('root');
            const firstMetadata = await ArrowBatchProtocol.readFileMetadata(rootFirstPath);
            const firstTable = await ArrowBatchProtocol.readArrowBatchTable(
                rootFirstPath, firstMetadata, 0
            );
            if (firstTable.numRows > 0) {
                const firstRow = firstTable.get(0).toArray();
                this._firstOrdinal = firstRow[0];
                this._firstTable = firstTable;
            }

            const rootLastPath = this.tableFileMap.get(highestBucket).get('root');
            const lastMetadata = await ArrowBatchProtocol.readFileMetadata(rootLastPath);
            const lastTable = await ArrowBatchProtocol.readArrowBatchTable(
                rootLastPath, lastMetadata, lastMetadata.batches.length - 1
            );

            if (lastTable.numRows > 0) {
                const lastRow = lastTable.get(lastTable.numRows - 1).toArray();
                this._lastOrdinal = lastRow[0];
                this._lastTable = lastTable;
            }
        }
    }
    beginFlush() {
        // make sure auxiliary is empty (block concurrent flushes)
        if (this.auxiliarySize != 0)
            throw new Error(`beginFlush called but auxiliary buffers not empty, is system overloaded?`)

        // push intermediate to auxiliary and clear it
        this._initIntermediate();
    }

    updateOrdinal(ordinal: number | bigint) {
        // first validate ordering
        ordinal = BigInt(ordinal);

        if (!this.isFirstUpdate) {
            const expected = this._lastOrdinal + BIGINT_1;
            if (ordinal != expected)
                throw new Error(`Expected argument ordinal to be ${expected.toLocaleString()} but was ${ordinal.toLocaleString()}`)
        } else
            this.isFirstUpdate = false;

        this._lastOrdinal = ordinal;

        if (!this._firstOrdinal)
            this._firstOrdinal = this._lastOrdinal;

        // maybe start flush
        if (this.intermediateSize === this.config.dumpSize)
            this.beginFlush();
    }

    protected getColumn(tableName: string, columnName: string, auxiliary: boolean = false) {
        const tableBuffers = auxiliary ? this._auxiliaryBuffers : this._intermediateBuffers;
        return tableBuffers.get(tableName).columns.get(columnName);
    }

    get auxiliarySize(): number {
        return this.getColumn('root', this.definition.root.ordinal, true).length;
    }

    get auxiliaryLastOrdinal(): bigint {
        return this.getColumn('root', this.definition.root.ordinal, true)[this.auxiliarySize - 1];
    }

    get intermediateSize(): number {
        return this.getColumn('root', this.definition.root.ordinal).length;
    }

    get intermediateLastOrdinal(): bigint {
        return this.getColumn('root', this.definition.root.ordinal)[this.auxiliarySize - 1];
    }

    getRow(tableName: string, index: number, auxiliary: boolean = false) {
        const tableBuffers = auxiliary ? this._auxiliaryBuffers : this._intermediateBuffers;
        const mappings = this.tableMappings.get(tableName);
        const tableBuff = tableBuffers.get(tableName);
        return mappings.map(
            m => decodeRowValue(
                tableName, m, tableBuff.columns.get(m.name)[index]));
    }

    async getRootRow(ordinal: bigint) {
        const ordinalField = this.definition.root.ordinal;

        // is row in intermediate buffers?
        const rootInterBuffs = this._intermediateBuffers.get('root');
        if (rootInterBuffs.columns.get(ordinalField).length > 0) {
            const oldestOnIntermediate = rootInterBuffs.columns.get(ordinalField)[0];
            const isOnIntermediate = ordinal >= oldestOnIntermediate && ordinal <= this.intermediateLastOrdinal
            if (isOnIntermediate) {
                const index = ordinal - oldestOnIntermediate;
                return this.getRow('root', Number(index));
            }
        }

        // is row in auxiliary buffers?
        const rootAuxBuffs = this._auxiliaryBuffers.get('root');
        if (rootAuxBuffs.columns.get(ordinalField).length > 0) {
            const oldestOnAuxiliary = rootAuxBuffs.columns.get(ordinalField)[0];
            const isOnAuxiliary = ordinal >= oldestOnAuxiliary && ordinal <= this.auxiliaryLastOrdinal
            if (isOnAuxiliary) {
                const index = ordinal - oldestOnAuxiliary;
                return this.getRow('root', Number(index), true);
            }
        }

        // is row on disk?
        const adjustedOrdinal = this.getOrdinal(ordinal);
        if (this.tableFileMap.has(adjustedOrdinal)) {
            const filePath = this.tableFileMap.get(adjustedOrdinal).get('root');
            const metadata = await ArrowBatchProtocol.readFileMetadata(filePath);
            const firstTable = await ArrowBatchProtocol.readArrowBatchTable(
                filePath, metadata, 0);

            const diskStartOrdinal: bigint = firstTable.get(0).toArray()[0];
            const relativeIndex = ordinal - diskStartOrdinal;
            const batchIndex = Number(relativeIndex / BigInt(this.config.dumpSize));
            const tableIndex = Number(relativeIndex % BigInt(this.config.dumpSize));

            const cacheKey = `${adjustedOrdinal}-${batchIndex}`;

            let table: Table = firstTable;
            if (!this.tableCache.has(cacheKey)) {
                if (batchIndex != 0)
                    table = await ArrowBatchProtocol.readArrowBatchTable(
                        filePath, metadata, batchIndex);

                // maybe add to cache if not present
                if (!this.tableCache.has(cacheKey)) {
                    // maybe trim cache
                    if (this.tableCache.size > ArrowBatchReader.DEFAULT_TABLE_CACHE) {
                        const oldest = this.cacheOrder.shift();
                        this.tableCache.delete(oldest);
                    }

                    this.tableCache.set(cacheKey, table);
                }
            } else
                table = this.tableCache.get(cacheKey);

            const row = table.get(tableIndex).toArray();
            this.tableMappings.get('root').forEach((m, i) => {
                row[i] = decodeRowValue('root', m, row[i]);
            });
            return row;
        }

        throw new Error(`Could not fetch ordinal ${ordinal}`);
    }

}

export class ArrowBatchWriter extends ArrowBatchReader {

    private _currentWriteBucket: string;

    private writeWorkers = new Map<string, {
        worker: Worker,
        status: 'running' | 'stopped',
        tid: number,
        tasks: Map<number, {ref: any, stack: Error}>
    }>();
    private workerLoggers = new Map<string, Logger>();

    constructor(
        config: ArrowConnectorConfig,
        definition: ArrowBatchContextDef,
        logger: Logger
    ) {
        super(config, logger);

        this.definition = definition;
        this.tableMappings = generateMappingsFromDefs(definition);

        [...this.tableMappings.entries()].forEach(
            ([name, mappings]) => {
                const workerLogOptions = {
                    exitOnError: false,
                    level: this.config.writerLogLevel,
                    format: format.combine(
                        format.metadata(),
                        format.colorize(),
                        format.timestamp(),
                        format.printf((info: any) => {
                            return `${info.timestamp} [WORKER-${name.toUpperCase()}] [${info.level}] : ${info.message} ${Object.keys(info.metadata).length > 0 ? JSON.stringify(info.metadata) : ''}`;
                        })
                    ),
                    transports: [new transports.Console({level: this.config.writerLogLevel})]
                }
                const workerLogger = loggers.add(`worker-${name}`, workerLogOptions);
                workerLogger.debug(`logger for worker ${name} initialized with level ${this.config.writerLogLevel}`);
                this.workerLoggers.set(name, workerLogger);

                let alias = undefined;
                if (name === 'root' && definition.root.name)
                    alias = definition.root.name;

                const worker = new Worker(
                    path.join(ROOT_DIR, 'build/data/arrow/write-worker.js'),
                    {
                        workerData: {
                            tableName: name,
                            alias,
                            tableMappings: mappings,
                            compression: this.config.compression,
                            logLevel: this.config.writerLogLevel
                        }
                    }
                );
                worker.on('message', (msg) => this.writersMessageHandler(msg));
                worker.on('error', (error) => {
                    throw error;
                });
                worker.on('exit', (msg) => {
                    this.writeWorkers.get(name).status = 'stopped';
                });
                this.writeWorkers.set(name, {
                    worker, status: 'running',
                    tid: 0,
                    tasks: new Map<number, {ref: any, stack: Error}>()
                });
            });
    }

    private sendMessageToWriter(name: string, msg: Partial<WriterControlRequest>, ref?: any) {
        if (name === this.definition.root.name)
            name = 'root';

        const workerInfo = this.writeWorkers.get(name);
        if (workerInfo.status !== 'running')
            throw new Error(
                `Tried to call method on writer worker but its ${workerInfo.status}`);

        const error = new Error();
        workerInfo.tasks.set(workerInfo.tid, {ref, stack: error});
        msg.tid = workerInfo.tid;

        workerInfo.tid++;
        workerInfo.worker.postMessage(msg);
    }

    private writersMessageHandler(msg: WriterControlResponse | WorkerLogMessage) {

        // catch log messages
        if (isWorkerLogMessage(msg)) {
            this.workerLoggers.get(msg.name).log(msg.log);
            return;
        }

        const workerInfo = this.writeWorkers.get(msg.name);

        if (msg.status !== 'ok') {
            this.logger.error(`error from worker ${msg.name}!`);
            this.logger.error(`orginal ref:\n${JSON.stringify(workerInfo.tasks.get(msg.tid).ref, null, 4)}`)
            this.logger.error(`original stack trace:\n${workerInfo.tasks.get(msg.tid).stack}`);
            throw msg.error;
        }

        if (workerInfo.status !== 'running')
            throw new Error(
                `Received msg from writer worker but it has an unexpected status: ${workerInfo.status}`);

        workerInfo.tasks.delete(msg.tid);

        if (msg.method === 'flush') {
            const auxBuffs = this._auxiliaryBuffers.get(msg.name);

            // clear all table column arrays (fields)
            [...auxBuffs.columns.keys()].forEach(
                column => auxBuffs.columns.set(column, [])
            );

            // flush is done when all table columns have been cleared on aux buffer
            let isDone = true;
            [...this._auxiliaryBuffers.values()].forEach(
                table => isDone = isDone && table.columns.get([...table.columns.keys()][0]).length == 0
            );
            if (isDone) {
                global.gc && global.gc();
                this.events.emit('flush');
            }
        }
    }

    async init(startOrdinal: number | bigint) {
        await super.init(startOrdinal);

        // write context defintion
        await pfs.writeFile(
            path.join(this.config.dataDir, 'context.json'),
            JSON.stringify(this.definition, null, 4)
        );

        this._currentWriteBucket = this.getOrdinalSuffix(this._lastOrdinal ?? startOrdinal);
    }

    async deinit() {
        await Promise.all(
            [...this.writeWorkers.values()]
                .map(workerInfo => workerInfo.worker.terminate())
        );
    }

    get wipBucketPath(): string {
        return path.join(this.config.dataDir, this._currentWriteBucket + '.wip');
    }

    beginFlush() {
        // make sure auxiliary is empty (block concurrent flushes)
        if (this.auxiliarySize != 0)
            throw new Error(`beginFlush called but auxiliary buffers not empty, is system overloaded?`)

        const maybeOldBucket = this.wipBucketPath;
        this._currentWriteBucket = this.getOrdinalSuffix(this._lastOrdinal);
        if (maybeOldBucket !== this.wipBucketPath)
            fs.renameSync(maybeOldBucket, maybeOldBucket.replace('.wip', ''));

        // maybe create target dir
        if (!fs.existsSync(this.wipBucketPath))
            fs.mkdirSync(this.wipBucketPath, {recursive: true});

        const isUnfinished = this.intermediateSize < this.config.dumpSize;

        // push intermediate to auxiliary and clear it
        this._initIntermediate();

        // send flush message to writer-workers
        [...this.tableMappings.keys()].forEach(tableName =>
            this.sendMessageToWriter(tableName, {
                method: 'flush',
                params: {
                    writeDir: this.wipBucketPath,
                    unfinished: isUnfinished
                }
            })
        );
    }

    addRow(tableName: string, row: any[], ref?: any) {
        if (tableName === this.definition.root.name)
            tableName = 'root';

        const tableBuffers = this._intermediateBuffers.get(tableName);
        const mappings = this.tableMappings.get(tableName);
        for (const [i, mapping] of mappings.entries())
            tableBuffers.columns.get(mapping.name).push(row[i]);

        this.sendMessageToWriter(tableName, {
           method: 'addRow',
           params: row
        }, ref);
    }
}