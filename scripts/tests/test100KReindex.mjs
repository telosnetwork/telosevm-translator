import {Client} from "@elastic/elasticsearch";
import path from "path";
import {readFileSync} from "fs";
import {sleep} from "../../build/utils/indexer.js";
import {TEVMIndexer} from "../../build/indexer.js";
import {assert, expect} from "chai";
import {StorageEosioActionSchema, StorageEosioDeltaSchema} from "../../build/types/evm.js";
import {
    getElasticActions, getElasticDeltas,
    SCRIPTS_DIR,
} from "../utils.mjs";
import moment from "moment";

const testStartTime = new Date().getTime();
const esConfig = {
    host: 'http://127.0.0.1:9200',
    verification: {
        delta: 'telos-mainnet-verification-delta-v1.5-00000031',
        action: 'telos-mainnet-verification-action-v1.5-00000031'
    }
}
const esClient = new Client({node: esConfig.host});
try {
    await esClient.ping();
} catch (e) {
    console.error('Could not ping elastic, is it up?');
    process.exit(1);
}

const translatorTestConfig = {
    template: 'config.mainnet.json',
    startBlock: 312087081,
    totalBlocks: 10000,
    evmPrevHash: '501f3fe6f64c7d858a57e914ac130af2661e530f47ed9492606a44e1baecd6b6',
};
const templatesDirPath = path.join(SCRIPTS_DIR, '../config-templates');
const translatorConfig = JSON.parse(
    readFileSync(path.join(templatesDirPath, translatorTestConfig.template)).toString()
);

const startBlock = translatorTestConfig.startBlock;
const endBlock = startBlock + translatorTestConfig.totalBlocks;

translatorConfig.chainName = 'telos-mainnet-verification';
translatorConfig.startBlock = startBlock;
translatorConfig.stopBlock = endBlock;
translatorConfig.evmPrevHash = translatorTestConfig.evmPrevHash;

if (translatorTestConfig.totalBlocks < translatorConfig.perf.elasticDumpSize)
    translatorConfig.perf.elasticDumpSize = translatorTestConfig.totalBlocks;

const adjustedNum = Math.floor(startBlock / translatorConfig.elastic.docsPerIndex);
const numericIndexSuffix = String(adjustedNum).padStart(8, '0');
const reindexPrefix = 'telos-mainnet-reindex';
const reindexDeltaIndexName = `telos-mainnet-reindex-${translatorConfig.elastic.subfix.delta}-${numericIndexSuffix}`;
const reindexActionIndexName = `telos-mainnet-reindex-${translatorConfig.elastic.subfix.transaction}-${numericIndexSuffix}`;

// try to delete generated data in case it exists
try {
    await esClient.indices.delete({index: reindexDeltaIndexName});
} catch (e) {
}
try {
    await esClient.indices.delete({index:  reindexActionIndexName});
} catch (e) {
}

// launch translator and verify generated data
let isTranslatorDone = false;
const translator = new TEVMIndexer(translatorConfig);

let esQueriesDone = 0;
let esDocumentsChecked = 0;

const translatorLaunchTime = new Date().getTime();
let translatorReindexTime;
await translator.reindex(reindexPrefix);
translator.connector.events.on('write', async (writeInfo) => {
    // get all block documents in write range
    const deltas = await getElasticDeltas(esClient, reindexDeltaIndexName, writeInfo.from, writeInfo.to);
    const verifiedDeltas = await getElasticDeltas(esClient, esConfig.verification.delta, writeInfo.from, writeInfo.to);
    assert(deltas.length === verifiedDeltas.length, 'getElasticDelta length difference');

    deltas.forEach((delta, index) => {
        const verifiedDelta = StorageEosioDeltaSchema.parse(verifiedDeltas[index]);
        const parsedDelta = StorageEosioDeltaSchema.parse(delta);

        expect(parsedDelta).to.be.deep.equal(verifiedDelta);
        esDocumentsChecked++;
    });

    // get all tx documents in write range
    const actions = await getElasticActions(esClient,  reindexActionIndexName, writeInfo.from, writeInfo.to);
    esQueriesDone++;
    const verifiedActions = await getElasticActions(esClient, esConfig.verification.action, writeInfo.from, writeInfo.to);
    esQueriesDone++;
    assert(actions.length === verifiedActions.length, 'getElasticActions length difference');

    actions.forEach((action, index) => {
        const verifiedActionDoc = verifiedActions[index];
        const verifiedAction = StorageEosioActionSchema.parse(verifiedActionDoc);
        const parsedAction = StorageEosioActionSchema.parse(action);

        expect(parsedAction).to.be.deep.equal(verifiedAction);
        esDocumentsChecked++;
    });

    const now = new Date().getTime();

    const currentTestTimeElapsed = moment.duration(now - testStartTime, 'ms').humanize();
    const currentReindexTimeElapsed = moment.duration(now - translatorLaunchTime, 'ms').humanize();

    const checkedBlocksCount = writeInfo.to - startBlock;
    const progressPercent = (((checkedBlocksCount / translatorTestConfig.totalBlocks) * 100).toFixed(2) + '%').padStart(6, ' ');
    const currentProgress = (writeInfo.to - startBlock).toLocaleString();

    console.log('-'.repeat(32));
    console.log('Test stats:');
    console.log(`last checked range ${writeInfo.from.toLocaleString()} - ${writeInfo.to.toLocaleString()}`);
    console.log(`progress: ${progressPercent}, ${currentProgress} blocks`);
    console.log(`total test time:    ${currentTestTimeElapsed}`);
    console.log(`total reindex time: ${currentReindexTimeElapsed}`);
    console.log(`es queries done: ${esQueriesDone}`);
    console.log(`es docs checked: ${esDocumentsChecked}`);
    console.log('-'.repeat(32));

    if (writeInfo.to === endBlock) {
        isTranslatorDone = true;
        translatorReindexTime = new Date().getTime();
    }
});

while (!isTranslatorDone)
    await sleep(200);

const totalTestTimeElapsed = translatorReindexTime - testStartTime;
const translatorReindexTimeElapsed = translatorReindexTime - translatorLaunchTime;

console.log('Test passed!');
console.log(`total time elapsed: ${moment.duration(totalTestTimeElapsed, 'ms').humanize()}`);
console.log(`sync time:          ${moment.duration(translatorReindexTimeElapsed, 'ms').humanize()}`);
console.log(`es queries done:    ${esQueriesDone}`);
console.log(`es docs checked:    ${esDocumentsChecked}`);

await esClient.close();