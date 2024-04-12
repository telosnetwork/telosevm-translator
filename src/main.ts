import { IndexerConfig, DEFAULT_CONF } from './types/indexer.js';
import { TEVMIndexer } from './indexer.js';
import { readFileSync } from 'node:fs';
import cloneDeep from 'lodash.clonedeep';
import { mergeDeep } from './utils/misc.js';
import { Command } from 'commander';
import { cleanEnv, str, num, bool } from 'envalid';

const program = new Command();

program
    .option('-c, --config [path to config.json]', 'Path to config.json file', 'config.json')
    .option('-t, --trim-from [block num]', 'Delete blocks in db from [block num] onwards', undefined)
    .option('-s, --skip-integrity-check', 'Skip initial db check', false)
    .option('-o, --only-db-check', 'Perform initial db check and exit', false)
    .option('-p, --gaps-purge', 'In case db integrity check fails purge db from last valid block', false)
    .option('-S, --skip-start-block-check', 'Skip initial get_block query to configured endpoint', false)
    .option('-r, --skip-remote-check', 'Skip initial get_info query to configured remoteEndpoint', false)
    .option(
        '-R, --reindex-into [index prefix]',
        'Use configured es index as source and regenerate data + hashes into a different index',
        undefined
    )
    .action(async (options) => {
        const env = cleanEnv(process.env, {
            LOG_LEVEL: str({ default: DEFAULT_CONF.logLevel }),
            READER_LOG_LEVEL: str({ default: DEFAULT_CONF.readerLogLevel }),
            CHAIN_NAME: str({ default: DEFAULT_CONF.chainName }),
            CHAIN_ID: num({ default: DEFAULT_CONF.chainId }),
            TELOS_ENDPOINT: str({ default: DEFAULT_CONF.endpoint }),
            TELOS_REMOTE_ENDPOINT: str({ default: DEFAULT_CONF.remoteEndpoint }),
            TELOS_WS_ENDPOINT: str({ default: DEFAULT_CONF.wsEndpoint }),
            EVM_BLOCK_DELTA: num({ default: DEFAULT_CONF.evmBlockDelta }),
            EVM_PREV_HASH: str({ default: DEFAULT_CONF.evmPrevHash }),
            EVM_VALIDATE_HASH: str({ default: DEFAULT_CONF.evmValidateHash }),
            INDEXER_START_BLOCK: num({ default: DEFAULT_CONF.startBlock }),
            INDEXER_STOP_BLOCK: num({ default: DEFAULT_CONF.stopBlock }),
            BROADCAST_HOST: str({ default: DEFAULT_CONF.broadcast.wsHost }),
            BROADCAST_PORT: num({ default: DEFAULT_CONF.broadcast.wsPort }),
            ELASTIC_NODE: str({ default: DEFAULT_CONF.elastic.node }),
            ELASTIC_USERNAME: str({ default: DEFAULT_CONF.elastic.auth.username }),
            ELASTIC_PASSWORD: str({ default: DEFAULT_CONF.elastic.auth.password }),
            ELASTIC_TIMEOUT: num({ default: DEFAULT_CONF.elastic.requestTimeout }),
            ELASTIC_DUMP_SIZE: num({ default: DEFAULT_CONF.perf.elasticDumpSize }),
            ELASTIC_DOCS_PER_INDEX: num({ default: DEFAULT_CONF.elastic.docsPerIndex }),
            ELASTIC_SHARD_NUM: num({ default: DEFAULT_CONF.elastic.numberOfShards }),
            ELASTIC_REPLICA_NUM: num({ default: DEFAULT_CONF.elastic.numberOfReplicas }),
            READER_WORKER_AMOUNT: num({ default: DEFAULT_CONF.perf.readerWorkerAmount }),
            EVM_WORKER_AMOUNT: num({ default: DEFAULT_CONF.perf.evmWorkerAmount }),
            IRREVERSIBLE_ONLY: bool({ default: DEFAULT_CONF.irreversibleOnly }),
            BLOCK_HISTORY_SIZE: num({ default: DEFAULT_CONF.blockHistorySize }),
        });

        const conf: IndexerConfig = cloneDeep(DEFAULT_CONF);

        try {
            const userConf = JSON.parse(readFileSync(options.config).toString());
            mergeDeep(conf, userConf);
        } catch (e) {}

        conf.runtime.skipIntegrityCheck = options.skipIntegrityCheck;
        conf.runtime.onlyDBCheck = options.onlyDBCheck;
        conf.runtime.gapsPurge = options.gapsPurge;
        conf.runtime.skipStartBlockCheck = options.skipStartBlockCheck;
        conf.runtime.skipRemoteCheck = options.skipRemoteCheck;
        conf.runtime.reindexInto = options.reindexInto;
        conf.runtime.trimFrom = options.trimFrom ? parseInt(options.trimFrom, 10) : undefined;

        conf.logLevel = env.LOG_LEVEL;
        conf.readerLogLevel = env.READER_LOG_LEVEL;
        conf.chainName = env.CHAIN_NAME;
        conf.chainId = env.CHAIN_ID;
        conf.endpoint = env.TELOS_ENDPOINT;
        conf.remoteEndpoint = env.TELOS_REMOTE_ENDPOINT;
        conf.wsEndpoint = env.TELOS_WS_ENDPOINT;
        conf.evmBlockDelta = env.EVM_BLOCK_DELTA;
        conf.evmPrevHash = env.EVM_PREV_HASH;
        conf.evmValidateHash = env.EVM_VALIDATE_HASH;
        conf.startBlock = env.INDEXER_START_BLOCK;
        conf.stopBlock = env.INDEXER_STOP_BLOCK;
        conf.broadcast.wsHost = env.BROADCAST_HOST;
        conf.broadcast.wsPort = env.BROADCAST_PORT;
        conf.elastic.node = env.ELASTIC_NODE;
        conf.elastic.auth.username = env.ELASTIC_USERNAME;
        conf.elastic.auth.password = env.ELASTIC_PASSWORD;
        conf.elastic.requestTimeout = env.ELASTIC_TIMEOUT;
        conf.elastic.docsPerIndex = env.ELASTIC_DOCS_PER_INDEX;
        conf.perf.readerWorkerAmount = env.READER_WORKER_AMOUNT;
        conf.perf.evmWorkerAmount = env.EVM_WORKER_AMOUNT;
        conf.perf.elasticDumpSize = env.ELASTIC_DUMP_SIZE;
        conf.irreversibleOnly = env.IRREVERSIBLE_ONLY;
        conf.blockHistorySize = env.BLOCK_HISTORY_SIZE;
        conf.elastic.numberOfShards = env.ELASTIC_SHARD_NUM;
        conf.elastic.numberOfReplicas = env.ELASTIC_REPLICA_NUM;

        const indexer = new TEVMIndexer(conf);
        try {
            await indexer.launch();
        } catch (e) {
            if (
                e.message.includes('Gap found in database at ') &&
                e.message.includes(', but --gaps-purge flag not passed!')
            ) {
                // @ts-ignore
                indexer.logger.error(
                    'Translator integrity check failed, ' +
                    'not gonna start unless --gaps-purge is passed or ' +
                    'config option runtime.gapsPurge == true'
                );
                process.exitCode = 47;
            } else {
                throw e;
            }
        }
    });

program.parse(process.argv);