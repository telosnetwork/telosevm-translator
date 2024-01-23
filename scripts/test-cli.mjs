import {readFileSync} from "node:fs";
import path from "node:path";

import {SCRIPTS_DIR} from "../build/utils/indexer.js";

import { program } from 'commander';
import {
    generateTranslatorConfig,
    translatorESReindexVerificationTest,
    translatorESReplayVerificationTest
} from "../build/utils/testing.js";
import {existsSync, unlinkSync, writeFileSync} from "fs";

function loadTestParamsAndGenConfig(testConfigPath, testParamsPath) {
    const testParams = JSON.parse(readFileSync(testParamsPath).toString());

    let config = undefined;
    if (existsSync(testConfigPath))
        config = JSON.parse(readFileSync(testConfigPath).toString());
    else
        config = generateTranslatorConfig(testParams)[0];

    writeFileSync(
        testConfigPath,
        JSON.stringify(config, null, 4),
        'utf-8'
    );

    return [testParams, config];
}

program
    .command('reindex <name>')
    .description('Run a reindex test')
    .action(async (name) => {
        const testParamsPath = path.join(SCRIPTS_DIR, `tests/${name}/reindex.json`);
        const testConfigPath = path.join(SCRIPTS_DIR, `tests/${name}/gen-config.json`);
        const [testParams, config] = loadTestParamsAndGenConfig(testConfigPath, testParamsPath);

        try {
            await translatorESReindexVerificationTest(testParams, config);
        } catch (e) {
            unlinkSync(testConfigPath);
            throw e;
        }
    });


program
    .command('replay <name>')
    .description('Run a replay test')
    .action(async (name) => {
        const testParamsPath = path.join(SCRIPTS_DIR, `tests/${name}/replay.json`);
        const testConfigPath = path.join(SCRIPTS_DIR, `tests/${name}/gen-config.json`);
        const [testParams, config] = loadTestParamsAndGenConfig(testConfigPath, testParamsPath);

        try {
            await translatorESReplayVerificationTest(testParams, config);
        } catch (e) {
            unlinkSync(testConfigPath);
            throw e;
        }
    });

program.parse(process.argv);
