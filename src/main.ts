import {IndexerConfig, DEFAULT_CONF} from './types/indexer.js';
import {TEVMIndexer} from './indexer.js';
import {readFileSync} from "node:fs";

// import heapdump from 'heapdump';
// 
// heapdump.writeSnapshot(function(err, filename) {
//   console.log('dump written to', filename);
// });


let conf: IndexerConfig = DEFAULT_CONF;
try {
    conf = { ...DEFAULT_CONF, ...JSON.parse(readFileSync('config.json').toString()) };
} catch (e) { }

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

if (process.env.EVM_DEPLOY_BLOCK)
    conf.evmDeployBlock = parseInt(process.env.EVM_DEPLOY_BLOCK, 10);

if (process.env.EVM_PREV_HASH)
    conf.evmPrevHash = process.env.EVM_PREV_HASH;

if (process.env.EVM_START_BLOCK)
    conf.evmStartBlock = parseInt(process.env.EVM_START_BLOCK, 10);

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

if (process.env.ELASTIC_USERNAME)
    conf.elastic.auth.username = process.env.ELASTIC_USERNAME;

if (process.env.ELASTIC_PASSWORD)
    conf.elastic.auth.password = process.env.ELASTIC_PASSWORD;

if (process.env.ELASTIC_TIMEOUT)
    conf.elastic.requestTimeout = parseInt(process.env.ELASTIC_TIMEOUT, 10);

if (process.env.WORKER_AMOUNT)
    conf.perf.workerAmount = parseInt(process.env.WORKER_AMOUNT, 10);

new TEVMIndexer(conf).launch();
