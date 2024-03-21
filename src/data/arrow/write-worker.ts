import fs from "node:fs";
import path from "node:path";
import {RecordBatchFileWriter, Table, tableFromArrays} from "apache-arrow";
import {ArrowBatchCompression, ArrowBatchProtocol, ArrowTableMapping, encodeRowValue, getArrayFor} from "./protocol.js";

import { parentPort, workerData } from 'node:worker_threads';
import {compressSync} from "zstd.ts";
import {format, LogEntry, Logger, loggers} from "winston";
import {WorkerTransport} from "../../utils/misc.js";


export type ArrowBatchWorkerConfig = {
    dumpSize: bigint, batchSize: bigint,
    compression: ArrowBatchCompression
};

let {
    tableName, tableMappings, arrowBatchConfig, logLevel
}: {

    tableName: string,
    tableMappings: ArrowTableMapping[],
    arrowBatchConfig:  ArrowBatchWorkerConfig,

    logLevel: string

} = workerData;

const {
    dumpSize, batchSize, compression
} = arrowBatchConfig;

const loggingOptions = {
    exitOnError: false,
    level: logLevel,
    format: format.json(),
    transports: [
        new WorkerTransport(
            (log: LogEntry) => {
                parentPort.postMessage({
                    name: tableName,
                    method: 'workerLog',
                    log
                });
            },
        {})
    ]
}
const logger: Logger = loggers.add(`worker-internal-${tableName}`, loggingOptions);

const intermediateBuffers = {};
function _initBuffer() {
    for (const mapping of tableMappings)
        intermediateBuffers[mapping.name] = [];
    logger.debug(`initialized buffers for ${Object.keys(intermediateBuffers)}`);
}

function _generateArrowBatchTable(): Table {
    const arrays = {};
    for (const mapping of tableMappings)
        arrays[mapping.name] = getArrayFor(mapping).from(intermediateBuffers[mapping.name]);
    return tableFromArrays(arrays);
}

function _initDiskBuffer(currentFile: string) {
    fs.writeFile(
        currentFile,
        ArrowBatchProtocol.newGlobalHeader(dumpSize, batchSize),
        () => logger.debug(`wrote global header on ${currentFile}`)
    );
}

function flush(params: {writeDir: string}) {
    const currentFile = path.join(params.writeDir, `${tableName}.ab`);

    logger.debug(`generating arrow table from intermediate...`);
    const startTableGen = performance.now();
    const table = _generateArrowBatchTable();
    logger.debug(`table generated, took: ${performance.now() - startTableGen}`);

    logger.debug(`serializing table...`);
    const startSerialize = performance.now();
    const recordWriter = RecordBatchFileWriter.writeAll(table);
    let serializedBatch = Buffer.from(recordWriter.toUint8Array(true));
    logger.debug(`serialized, took: ${performance.now() - startSerialize}`);

    switch (compression) {
        case ArrowBatchCompression.UNCOMPRESSED: {
            break;
        }
        case ArrowBatchCompression.ZSTD: {
            serializedBatch = compressSync({input: serializedBatch});
            break;
        }
    }

    if (!fs.existsSync(currentFile))
        _initDiskBuffer(currentFile);

    // header
    logger.debug(`writing batch to disk...`);
    const startWrite = performance.now();
    fs.appendFileSync(
        currentFile, ArrowBatchProtocol.newBatchHeader(BigInt(serializedBatch.length), compression));

    // content
    fs.appendFileSync(currentFile, serializedBatch);
    logger.debug(`${serializedBatch.length.toLocaleString()} bytes written to disk, took: ${performance.now() - startWrite}`);

    _initBuffer();
}

function addRow(row: any[]) {
    const typedRow = tableMappings.map(
        (fieldInfo, index) => encodeRowValue(tableName, fieldInfo, row[index]));

    tableMappings.forEach(
        (fieldInfo, index) => {
            const fieldColumn = intermediateBuffers[fieldInfo.name];
            fieldColumn.push(typedRow[index])
        });
}

export interface WriterControlRequest {
    method: 'addRow' | 'flush'
    params?: any
}

export interface WriterControlResponse {
    name: string,
    status: 'ok' | 'error',
    error?: any
}

const handlers = {
  addRow, flush
};

_initBuffer();

parentPort.on('message', (msg: WriterControlRequest) => {
    const resp: WriterControlResponse = {
        name: tableName,
        status: 'ok'
    }
    try {
        if (!(msg.method in handlers))
            throw new Error(`Unknown method \'${msg.method}\'!`);

        handlers[msg.method](msg.params);
    } catch (e) {
        resp.status = 'error';
        resp.error = e;
    }

    parentPort.postMessage(resp);
})