import {readFileSync} from "node:fs";
import path from "node:path";

import {SCRIPTS_DIR} from "../build/utils/indexer.js";

import { program } from 'commander';
import {
    generateTranslatorConfig,
    translatorESReindexVerificationTest,
    translatorESReplayVerificationTest
} from "../build/utils/testing.js";

function loadTestParamsAndGenConfig(testConfigPath, testParamsPath) {
    const testParams = JSON.parse(readFileSync(testParamsPath).toString());
    return [testParams, generateTranslatorConfig(testParams)[0]];
}

program
    .command('reindex <name>')
    .description('Run a reindex test')
    .action(async (name) => {
        const testParamsPath = path.join(SCRIPTS_DIR, `tests/${name}/reindex.json`);
        const testConfigPath = path.join(SCRIPTS_DIR, `tests/${name}/gen-config.json`);
        const [testParams, config] = loadTestParamsAndGenConfig(testConfigPath, testParamsPath);
        await translatorESReindexVerificationTest(testParams, config);
    });


program
    .command('replay <name>')
    .description('Run a replay test')
    .action(async (name) => {
        const testParamsPath = path.join(SCRIPTS_DIR, `tests/${name}/replay.json`);
        const testConfigPath = path.join(SCRIPTS_DIR, `tests/${name}/gen-config.json`);
        const [testParams, config] = loadTestParamsAndGenConfig(testConfigPath, testParamsPath);
        await translatorESReplayVerificationTest(testParams, config);
    });

program.parse(process.argv);
