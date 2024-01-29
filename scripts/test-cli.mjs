import {readFileSync} from "node:fs";
import path from "node:path";

import {SCRIPTS_DIR} from "../build/utils/indexer.js";

import { program } from 'commander';
import {
    generateTranslatorConfig,
    translatorESReindexVerificationTest,
    translatorESReplayVerificationTest
} from "../build/utils/testing.js";
import {writeFileSync, writeSync} from "fs";

function loadTestParamsAndGenConfig( testParamsPath) {
    const testParams = JSON.parse(readFileSync(testParamsPath).toString());
    const translatorConfig = generateTranslatorConfig(testParams)[0];
    writeFileSync(path.join(path.dirname(testParamsPath), 'config.json'), JSON.stringify(translatorConfig, null, 4));
    return [testParams, translatorConfig];
}

program
    .command('reindex <name>')
    .description('Run a reindex test')
    .option('-p, --polars', 'Use polars config', false)
    .action(async (name, options) => {
        let confFile = 'reindex.json';
        if (options.polars)
            confFile = 'reindex-polars.json';

        const testParamsPath = path.join(SCRIPTS_DIR, `tests/${name}/${confFile}`);
        const [testParams, config] = loadTestParamsAndGenConfig(testParamsPath);
        await translatorESReindexVerificationTest(testParams, config);
    });


program
    .command('replay <name>')
    .description('Run a replay test')
    .action(async (name) => {
        const testParamsPath = path.join(SCRIPTS_DIR, `tests/${name}/replay.json`);
        const [testParams, config] = loadTestParamsAndGenConfig(testParamsPath);
        await translatorESReplayVerificationTest(testParams, config);
    });

program.parse(process.argv);
