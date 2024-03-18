import {IndexerConfig, DEFAULT_CONF} from './types/indexer.js';
import {TEVMIndexer} from './indexer.js';
import {readFileSync} from "node:fs";
import cloneDeep from "lodash.clonedeep";
import {mergeDeep} from "./utils/misc.js";
import { Command } from 'commander';

const program = new Command();

program
    .option('-c, --config [path to config.json]', 'Path to config.json file', 'config.json')
    .option('-t, --trim-from [block num]', 'Delete blocks in db from [block num] onwards', undefined)
    .option('-s, --skip-integrity-check', 'Skip initial db check', false)
    .option('-o, --only-db-check', 'Perform initial db check and exit', false)
    .option('-p, --gaps-purge', 'In case db integrity check fails purge db from last valid block', false)
    .option('-S, --skip-start-block-check', 'Skip initial get_block query to configured endpoint', false)
    .option('-r, --skip-remote-check', 'Skip initial get_info query to configured remoteEndpoint', false)
    .option('-R, --reindex-into [index prefix]',
        'Use configured es index as source and regenerate data + hashes into a different index', undefined)
    .action(async (options) => {
        const conf: IndexerConfig = cloneDeep(DEFAULT_CONF);

        try {
            const userConf = JSON.parse(readFileSync(options.config).toString());
            mergeDeep(conf, userConf);
        } catch (e) { }

        if (options.skipIntegrityCheck)
            conf.runtime.skipIntegrityCheck = options.skipIntegrityCheck;

        if (options.onlyDBCheck)
            conf.runtime.onlyDBCheck = options.onlyDBCheck;

        if (options.gapsPurge)
            conf.runtime.gapsPurge = options.gapsPurge;

        if (options.skipStartBlockCheck)
            conf.runtime.skipStartBlockCheck = options.skipStartBlockCheck;

        if (options.skipRemoteCheck)
            conf.runtime.skipRemoteCheck = options.skipRemoteCheck;

        if (options.reindexInto)
            conf.runtime.reindexInto = options.reindexInto;

        if (options.trimFrom)
            conf.runtime.trimFrom = parseInt(options.trimFrom, 10);

        if (process.env.LOG_LEVEL)
            conf.logLevel = process.env.LOG_LEVEL;

        if (process.env.READER_LOG_LEVEL)
            conf.readerLogLevel = process.env.READER_LOG_LEVEL;

        if (process.env.CHAIN_NAME)
            conf.chainName = process.env.CHAIN_NAME;

        if (process.env.CHAIN_ID)
            conf.chainId = parseInt(process.env.CHAIN_ID, 10);

        if (process.env.TELOS_ENDPOINT)
            conf.endpoint = process.env.TELOS_ENDPOINT;

        if (process.env.TELOS_REMOTE_ENDPOINT)
            conf.remoteEndpoint = process.env.TELOS_REMOTE_ENDPOINT;

        if (process.env.TELOS_WS_ENDPOINT)
            conf.wsEndpoint = process.env.TELOS_WS_ENDPOINT;

        if (process.env.EVM_BLOCK_DELTA)
            conf.evmBlockDelta = parseInt(process.env.EVM_BLOCK_DELTA, 10);

        if (process.env.EVM_PREV_HASH)
            conf.evmPrevHash = process.env.EVM_PREV_HASH;

        if (process.env.EVM_VALIDATE_HASH)
            conf.evmValidateHash = process.env.EVM_VALIDATE_HASH;

        if (process.env.INDEXER_START_BLOCK)
            conf.startBlock = parseInt(process.env.INDEXER_START_BLOCK, 10);

        if (process.env.INDEXER_STOP_BLOCK)
            conf.stopBlock = parseInt(process.env.INDEXER_STOP_BLOCK, 10);

        if (process.env.BROADCAST_HOST)
            conf.broadcast.wsHost = process.env.BROADCAST_HOST;

        if (process.env.BROADCAST_PORT)
            conf.broadcast.wsPort = parseInt(process.env.BROADCAST_PORT, 10);

        if (process.env.ELASTIC_NODE)
            conf.elastic.node = process.env.ELASTIC_NODE;

        if (process.env.ELASTIC_DUMP_SIZE)
            conf.perf.elasticDumpSize = parseInt(process.env.ELASTIC_DUMP_SIZE, 10);

        if (process.env.ELASTIC_DOCS_PER_INDEX)
            conf.elastic.docsPerIndex = parseInt(process.env.ELASTIC_DOCS_PER_INDEX, 10);

        if (process.env.ELASTIC_USERNAME)
            conf.elastic.auth.username = process.env.ELASTIC_USERNAME;

        if (process.env.ELASTIC_PASSWORD)
            conf.elastic.auth.password = process.env.ELASTIC_PASSWORD;

        if (process.env.ELASTIC_TIMEOUT)
            conf.elastic.requestTimeout = parseInt(process.env.ELASTIC_TIMEOUT, 10);

        if (process.env.READER_WORKER_AMOUNT)
            conf.perf.readerWorkerAmount = parseInt(process.env.READER_WORKER_AMOUNT, 10);

        if (process.env.EVM_WORKER_AMOUNT)
            conf.perf.evmWorkerAmount = parseInt(process.env.EVM_WORKER_AMOUNT, 10);

        const indexer = new TEVMIndexer(conf);
        try {
            await indexer.launch();
        } catch(e) {
            if (e.message.includes('Gap found in database at ') &&
                e.message.includes(', but --gaps-purge flag not passed!')) {
                // @ts-ignore
                indexer.logger.error(
                    'Translator integrity check failed, ' +
                    'not gonna start unless --gaps-purge is passed or ' +
                    'config option runtime.gapsPurge == true'
                );
                process.exitCode = 47;
            } else
                throw e;
        }
    });

program.parse(process.argv);