import {format, loggers, transports} from "winston";
import {ArrowBatchReader, extendedStringify, FlushReq} from "@guilledk/arrowbatch-nodejs";
import {
    ChainConfigSchema,
    ConnectorConfigSchema,
} from "../types/config.js";
import {ElasticConnector} from "../data/elastic/elastic.js";
import {expect} from "chai";
import {ArrowConnector} from "../data/arrow/arrow.js";
import {initFeatureManager} from "../features.js";

const logLevel = 'debug';

const loggingOptions = {
    exitOnError: false,
    level: logLevel,
    format: format.combine(
        format.metadata(),
        format.colorize(),
        format.timestamp(),
        format.printf((info: any) => {
            return `${info.timestamp} [PID:${process.pid}] [${info.level}] : ${info.message} ${Object.keys(info.metadata).length > 0 ? JSON.stringify(info.metadata) : ''}`;
        })
    ),
    transports: [
        new transports.Console({
            level: logLevel
        })
    ]
}
const logger = loggers.add('translator', loggingOptions);

const chainConfig = ChainConfigSchema.parse({
    chainName: 'telos-mainnet-tevmc',
    chainId: 40,
    startBlock: 180698860,
    evmBlockDelta: 36
});

initFeatureManager('2.0.0');

const elasticConfig = ConnectorConfigSchema.parse({
    chain: chainConfig,
    elastic: {
        node: 'http://127.0.0.1:9200'
    }
});

const arrowConfig = ConnectorConfigSchema.parse({
    chain: chainConfig,
    arrow: {
        dataDir: './arrow-data-1',
        // writerLogLevel: 'debug',
    }
});

const elasticConnector = new ElasticConnector(elasticConfig);
await elasticConnector.init();

const arrowConnector = new ArrowConnector(arrowConfig, true);
// @ts-ignore
const arrowReader: ArrowBatchReader = arrowConnector.context;

async function verifyRange(from: bigint, to: bigint) {
    // const scroller = elasticConnector.blockScroll({
    //     from, to, logLevel: 'info', tag: 'es-verify', validate: false
    // });
    // await scroller.init();

    // for await (const block of scroller) {
    for (let i = from; i <= to; i++) {
        const esBlock = await elasticConnector.getIndexedBlock(i);
        const arrBlock = await arrowConnector.getIndexedBlock(i);

        const strEsBlock = extendedStringify(esBlock);
        const strArrBlock = extendedStringify(arrBlock);

        expect(JSON.parse(strEsBlock)).to.be.equal(JSON.parse(strArrBlock));
        expect(arrBlock).to.be.deep.eq(esBlock);
        if (i % 1000n == 0n)
            logger.debug(`verified ${i.toLocaleString()}`)
    }
}

function onFlushHandler (flushInfo: FlushReq['params']) {
    logger.info(`writer flushed: ${JSON.stringify(flushInfo)}`);

    arrowReader.reloadOnDiskBuckets().then(async () => {
        // @ts-ignore
        const [meta, _] = await arrowReader.cache.getMetadataFor(Number(flushInfo.adjusted_ordinal), 'root');
        const batchMeta = meta.meta.batches[Number(flushInfo.batch_index)].batch;
        const from = batchMeta.startOrdinal;
        const to = batchMeta.lastOrdinal;

        await verifyRange(from, to);
        logger.info(`verified range ${from}-${to}`);
    });
}

await arrowConnector.init();

await arrowReader.validate();

const startOrdinal: bigint = arrowReader.firstOrdinal;
const lastOnDiskOrdinal: bigint = arrowReader.lastOrdinal;

logger.info(`verifying on disk tables ${startOrdinal} to ${lastOnDiskOrdinal}`);
await verifyRange(startOrdinal, lastOnDiskOrdinal);
logger.info('on disk validation passed, connecting to ws stream...');
await arrowReader.beginSync(
    5000, {onFlush: onFlushHandler}
)
