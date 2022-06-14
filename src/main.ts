import { TEVMIndexer } from './indexer';

const conf = require('../config.json');

if (process.env.TELOS_ENDPOINT)
    conf.endpoint = process.env.TELOS_ENDPOINT;

if (process.env.TELOS_WS_ENDPOINT)
    conf.wsEndpoint = process.env.TELOS_WS_ENDPOINT;

if (process.env.INDEXER_START_BLOCK)
    conf.startBlock = parseInt(process.env.INDEXER_START_BLOCK, 10);

if (process.env.INDEXER_STOP_BLOCK)
    conf.stopBlock = parseInt(process.env.INDEXER_STOP_BLOCK, 10);

if (process.env.ELASTIC_NODE)
    conf.elastic.node = process.env.ELASTIC_NODE;

if (process.env.ELASTIC_USERNAME)
    conf.elastic.auth.username = process.env.ELASTIC_USERNAME;

if (process.env.ELASTIC_PASSWORD)
    conf.elastic.auth.password = process.env.ELASTIC_PASSWORD;

new TEVMIndexer(conf).launch();
