import {ControllerContext} from "leap-mock/controllerUtils.js";
import {ControllerConfig} from "leap-mock/controller.js";
import {getRandomPort} from "leap-mock/utils.js";
import {DEFAULT_CONF} from "../types/indexer.js";
import {portFromEndpoint} from "../utils/misc.js";
import {expectTranslatorSequence} from "./utils.js";
import cloneDeep from 'lodash.clonedeep';


describe('Leap Mock', async function () {
    const config: ControllerConfig = {controlPort: await getRandomPort()};
    const context = new ControllerContext(config);

    const translatorConfig = cloneDeep(DEFAULT_CONF);
    translatorConfig.evmBlockDelta = 0;
    translatorConfig.evmDeployBlock = 2;
    translatorConfig.startBlock = 2;
    translatorConfig.perf.stallCounter = 2;
    translatorConfig.perf.elasticDumpSize = 1;
    translatorConfig.perf.readerWorkerAmount = 1;
    translatorConfig.perf.evmWorkerAmount = 1;

    const shipPort = portFromEndpoint(translatorConfig.wsEndpoint);
    const httpPort = portFromEndpoint(translatorConfig.endpoint);

    before(async function ()  {
        await context.bootstrap();
    });
    beforeEach(async function () {
        await context.startTest(this.currentTest.title);
    });
    afterEach(async function () {
        await context.endTest(this.currentTest.title);
    });
    after(async function () { await context.teardown() });

    const testForkName = 'simple fork';
    context.registerTestChain(testForkName, {
        shipPort, httpPort,
        jumps: [[5, 3]]
    });
    it(testForkName, async function () {
        return await expectTranslatorSequence(
            translatorConfig,
            [
                2, 3, 4, 5,
                3, 4, 5, 6
            ]
        );
    });

    const testForkDoubleName = 'double fork';
    context.registerTestChain(testForkDoubleName, {
        shipPort, httpPort,
        jumps: [[5, 3], [6, 6]]
    });
    it(testForkDoubleName, async function () {
        return await expectTranslatorSequence(
            translatorConfig,
            [
                2, 3, 4, 5,
                3, 4, 5, 6, 6, 7
            ]
        )
    });

    const testReconName = 'simple reconnect';
    context.registerTestChain(testReconName, {
        shipPort, httpPort,
        pauses: [[3, 1]]
    });
    it(testReconName, async function () {
        return await expectTranslatorSequence(
            translatorConfig,
            [2, 3, 4, 5]
        );
    });

    const testReconLongName = 'long reconnect';
    context.registerTestChain(testReconLongName, {
        shipPort, httpPort,
        pauses: [[4, 10]]
    });
    it(testReconLongName, async function () {
        return await expectTranslatorSequence(
            translatorConfig,
            [2, 3, 4, 5, 6, 7, 8]
        );
    });

    const testReconMultiName = 'multi reconnect';
    context.registerTestChain(testReconMultiName, {
        shipPort, httpPort,
        pauses: [[3, 1], [10, 1]]});
    it(testReconMultiName, async function () {
        return await expectTranslatorSequence(
            translatorConfig, [
                2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]);
    });
});
