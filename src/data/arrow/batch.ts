import fs, {promises as pfs} from "node:fs";
import {ArrowConnectorConfig} from "../../types/indexer.js";
import path from "node:path";
import {createLogger, format, Logger, loggers, transports} from "winston";
import {ArrowBatchCompression, ArrowBatchProtocol, ArrowTableMapping} from "./protocol.js";
import {ROOT_DIR} from "../../utils/indexer.js";

import {Worker} from 'node:worker_threads';
import {WriterControlRequest, WriterControlResponse} from "./write-worker.js";
import {isWorkerLogMessage, WorkerLogMessage} from "../../utils/misc.js";

export class ArrowBatchContext {

    static readonly DEFAULT_BUCKET_SIZE = 1e7;
    static readonly DEFAULT_DUMP_SIZE = 1e5;

    readonly config: ArrowConnectorConfig;
    readonly logger: Logger;

    private _currentWriteBucket: string;
    private tableFileMap: Map<number, Map<string, string>>;
    private tableMappings: Map<string, ArrowTableMapping[]> = new Map();

    private lastOrdinal: bigint;

    private writeWorkers = new Map<string, {
        worker: Worker,
        status: 'running' | 'stopped',
        taskCount: number
    }>();
    private workerLoggers = new Map<string, Logger>();

    constructor(
        config: ArrowConnectorConfig,
        types: {[key: string]: ArrowTableMapping[]},
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

        Object.entries(types).forEach(
            ([name, mappings]) => {
                this.tableMappings.set(name, mappings);

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

                const worker = new Worker(
                    path.join(ROOT_DIR, 'build/data/arrow/write-worker.js'),
                    {
                        workerData: {
                            tableName: name,
                            tableMappings: mappings,
                            arrowBatchConfig: {
                                dumpSize: BigInt(this.config.dumpSize),
                                batchSize: BigInt(this.config.bucketSize / this.config.dumpSize),
                                compression: ArrowBatchCompression.UNCOMPRESSED
                            },
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
                    worker, status: 'running', taskCount: 0
                });
            });
    }

    private sendMessageToWriter(name: string, msg: WriterControlRequest) {
        const workerInfo = this.writeWorkers.get(name);
        if (workerInfo.status !== 'running')
            throw new Error(
                `Tried to call method on writer worker but its ${workerInfo.status}`);

        workerInfo.taskCount++;
        workerInfo.worker.postMessage(msg);
    }

    private writersMessageHandler(msg: WriterControlResponse | WorkerLogMessage) {

        // catch log messages
        if (isWorkerLogMessage(msg)) {
            this.workerLoggers.get(msg.name).log(msg.log);
            return;
        }

        if (msg.status !== 'ok')
            throw msg.error;

        const workerInfo = this.writeWorkers.get(msg.name);

        if (workerInfo.status !== 'running')
            throw new Error(
                `Received msg from writer worker but it has an unexpected status: ${workerInfo.status}`);

        workerInfo.taskCount--;
    }

    async init(startOrdinal: number | bigint) {
        try {
            await pfs.mkdir(this.config.dataDir, {recursive: true});
        } catch (e) {
            this.logger.error(e.message);
        }

        await this.reloadOnDiskBuckets();

        if (this.tableFileMap.size > 0) {
            const highestBucket = [...this.tableFileMap.keys()]
                .sort()
                .reverse()[0];

            for (const [tableName, tablePath] of this.tableFileMap.get(highestBucket).entries()) {
                const metadata = await ArrowBatchProtocol.readFileMetadata(tablePath);
                const lastBatchIndex = metadata.batches.length - 1;
                const table = await ArrowBatchProtocol.readArrowBatchTable(tablePath, metadata, lastBatchIndex);
                const rows = table.numRows;
            }
        } else
            this._currentWriteBucket = this.getOrdinalSuffix(startOrdinal);
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
            const file = tableFiles.find(file => file == `${tableName}.ab`);
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

    private getWIPBucketPath() {
        return path.join(this.config.dataDir, this._currentWriteBucket + '.wip');
    }

    private maybeInitBucket() {
        const wipBucketPath = this.getWIPBucketPath();

        if (!fs.existsSync(wipBucketPath))
            fs.mkdirSync(wipBucketPath, {recursive: true});
    }

    updateOrdinal(ordinal: number | bigint) {
        this.lastOrdinal = BigInt(ordinal);

        if ((this.lastOrdinal + BigInt(1)) % BigInt(this.config.dumpSize) == BigInt(0)) {
            this.maybeInitBucket();
            for (const tableName of this.tableMappings.keys())
                this.sendMessageToWriter(tableName, {
                    method: 'flush',
                    params: {writeDir: this.getWIPBucketPath()}
                });
        }
    }

    addRow(tableName: string, row: any[]) {
        this.sendMessageToWriter(tableName, {
           method: 'addRow',
           params: row
        });
    }
}