import {ConnectorConfig} from "../../types/indexer.js";
import {
    isStorableDocument,
    StorageEosioActionSchema,
    StorageEosioDeltaSchema
} from "../../types/evm.js";
import {sampleActionDocument, sampleDeltaDocument, sampleIndexedBlock} from "../samples.js";

import {assert, expect} from "chai";
import {ElasticConnector} from "../../data/elastic";


describe('Elastic Connector', function() {

    it('is document', function() {
        assert(
            !isStorableDocument({create: {_index: 'dummy-chain-delta-000', _id: `dummy-chain-block-1`}}),
            'create document should not be storable!'
        );
        assert(
            !isStorableDocument({index: {_index: 'dummy-chain-error-000'}}),
            'index document should not be storable!'
        );
        assert(
            isStorableDocument(StorageEosioDeltaSchema.parse(sampleDeltaDocument)),
            'StorageEosioDelta sample should be storable!'
        );
        assert(
            isStorableDocument(StorageEosioActionSchema.parse(sampleActionDocument)),
            'StorageEosioAction sample should be storable!'
        );
        assert(
            isStorableDocument(sampleIndexedBlock({
                delta: {
                    block_num: 1, "@global": {block_num: 1}
                }
            }, {chainStartTime: new Date()}).delta),
            'sample indexed block is not storable'
        );
    });

    it('fork cleanup', async function () {
        // test variables
        const startTime = new Date();
        const blockNumDelta = 2;
        const extraBlocks = 2;
        const lastNonForked = 10;
        const lastForked = 14;

        expect(startTime).to.be.instanceof(Date);
        expect(blockNumDelta).to.be.gte(0);
        expect(extraBlocks).to.be.gte(0);
        expect(lastNonForked).to.be.gte(1);
        expect(lastForked).to.be.gt(lastNonForked);

        // create dummy un-initialized connector
        const config: ConnectorConfig = {
            chain: {
                "chainName": "dummy-chain",
                "chainId": 41,
                "startBlock": lastNonForked - extraBlocks,
                "stopBlock": 4294967295,
                "evmBlockDelta": blockNumDelta,
                "evmPrevHash": "",
                "evmValidateHash": "",
                "irreversibleOnly": false
            },
            elastic: {
                "node": "http://127.0.0.1:9200",
                "auth": {
                    "username": "elastic",
                    "password": "password"
                },
                "requestTimeout": 5 * 1000,
                "docsPerIndex": 10000000,
                "scrollSize": 6000,
                "scrollWindow": "1m",
                "numberOfShards": 1,
                "numberOfReplicas": 0,
                "refreshInterval": -1,
                "codec": "default",
                "dumpSize": 1000,
                "suffix": {
                    "delta": "delta-v1.5",
                    "transaction": "action-v1.5",
                    "error": "error-v1.5",
                    "fork": "fork-v1.5"
                }
            }
        }

        const conn = new ElasticConnector(config);

        // calculate expected values from variables
        const totalForked = lastForked - lastNonForked;
        const totalBlocksPushed = totalForked + extraBlocks + 1;
        const totalOperationsBeforeCleanup = totalBlocksPushed * 2;  // opDrain length is always * 2
        const totalBlocksAfterCleanup = totalBlocksPushed - totalForked;
        // add one cause there should be a fork log document pushed
        const totalOperationsAfterCleanup = (totalBlocksAfterCleanup + 1) * 2;
        const forkTime = new Date(startTime.getTime() + (lastForked * 500)).toISOString();

        const forkIndex = `${config.chain.chainName}-${config.elastic.suffix.fork}-${conn.getSuffixForBlock(lastNonForked)}`;
        expect(
            lastForked,
            `in order for deltaIndex generated to make sense lastForked has to be < docsPerIndex config`
        ).to.be.lt(config.elastic.docsPerIndex);
        const deltaIndex = `${config.chain.chainName}-${config.elastic.suffix.delta}-${conn.getSuffixForBlock(lastForked)}`;

        // populate internal connector ds with documents
        for (let i = lastNonForked - extraBlocks; i <= lastForked; i++) {
            await conn.pushBlock(sampleIndexedBlock({
                delta: {
                    block_num: i,
                    "@global": {
                        block_num: i - blockNumDelta
                    }
                }
            }, {chainStartTime: startTime}));
        }

        expect(
            totalBlocksPushed, 'wrong total pushed on connector'
        ).to.be.eq(conn.totalPushed);
        expect(
            totalBlocksPushed, 'wrong blockDrain length pre-cleanup'
        ).to.be.eq(conn.blockDrain.length);
        expect(
            totalOperationsBeforeCleanup, 'wrong opDrain length pre-cleanup'
        ).to.be.eq(conn.opDrain.length);

        // trigger fork cleanup
        conn.forkCleanup(
            forkTime,
            lastNonForked,
            lastForked
        );

        // validate post-cleanup state
        expect(conn.lastPushed, 'conn.lastPushed left in wrong state!').to.be.eq(lastNonForked);
        expect(conn.blockDrain.length, 'blockDrain length mismatch!').to.be.eq(totalBlocksAfterCleanup);
        expect(conn.opDrain.length, 'opDrain length mismatch!').to.be.eq(totalOperationsAfterCleanup);

        // verify op sequence except last which should be fork log document
        for(let i = 0; i < totalOperationsAfterCleanup - 2; i += 2) {
            const op = conn.opDrain[i];
            const doc = conn.opDrain[i+1];

            const correctBlockNum = config.chain.startBlock + Math.floor(i / 2);
            const correctEVMBlockNum = correctBlockNum - blockNumDelta;

            assert(isStorableDocument(doc), `post-cleanup found a non storable document at index ${i}`);
            expect(doc.block_num, 'block document out of order!').to.be.eq(correctBlockNum);
            expect(doc['@global'].block_num, 'block document out of order!').to.be.eq(correctEVMBlockNum);
            expect(op, `post-cleanup op document `).to.be.deep.eq(
                {create: {_index: deltaIndex, _id: `${config.chain.chainName}-block-${doc.block_num}`}}
            );
        }

        // last operation should be a fork log document
        const forkOperation = conn.opDrain[conn.opDrain.length - 2];
        const forkDocument = conn.opDrain[conn.opDrain.length - 1];
        expect(forkOperation, 'fork operation document mismatch!').to.be.deep.eq({index: {_index: forkIndex}});
        expect(forkDocument, 'fork document mismatch!').to.be.deep.eq({
            timestamp: forkTime,
            lastNonForked, lastForked
        });
    });
});
