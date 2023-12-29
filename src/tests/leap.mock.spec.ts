import {describeMockChainTests, TestContext} from "./utils.js";
import cloneDeep from "lodash.clonedeep";
import {DEFAULT_CONF} from "../types/indexer.js";
import {TelosEVMCreate, AntelopeTransfer} from "leap-mock";
import {expect} from "chai";

const quantity = '420.0000 TLOS';

const translatorConfig = cloneDeep(DEFAULT_CONF);
translatorConfig.evmBlockDelta = 0;
translatorConfig.perf.stallCounter = 2;
translatorConfig.perf.elasticDumpSize = 1;
translatorConfig.perf.readerWorkerAmount = 1;
translatorConfig.perf.evmWorkerAmount = 1;


describeMockChainTests(
    'Leap Mock',
    translatorConfig,
    {
        'simple fork': {
            sequence: [
                2, 3, 4, 5,
                3, 4, 5, 6
            ],
            chainConfig: {
                jumps: [[5, 3]]
            }
        },
        'double fork': {
            sequence: [
                2, 3, 4, 5,
                3, 4, 5, 6, 6, 7
            ],
            chainConfig: {
                jumps: [[5, 3], [6, 6]]
            }
        },
        'simple reconnect': {
            sequence: [2, 3, 4, 5],
            chainConfig: {
                pauses: [[3, 1]]
            }
        },
        'long reconnect': {
            sequence: [2, 3, 4, 5, 6, 7, 8],
            chainConfig: {
                pauses: [[4, 10]]
            }
        },
        'multi reconnect': {
            sequence: [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
            chainConfig: {
                pauses: [[3, 1], [10, 1]]
            }
        },
        'evm deposit document test': {
            sequence: [2, 3, 4],
            chainConfig: {
                txs: {
                    2: [new TelosEVMCreate({account: 'alice'})],
                    3: [new AntelopeTransfer({from: 'alice', to: 'eosio.evm', quantity})]
                }
            },
            testFn: async function (testCtx: TestContext): Promise<void> {
                expect(testCtx.elastic.actions.length).to.be.eq(1);
            }
        }
    }
);