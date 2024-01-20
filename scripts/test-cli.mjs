import {readFileSync} from "node:fs";
import path from "node:path";

import {SCRIPTS_DIR} from "../build/utils/indexer.js";

import { program } from 'commander';
import {translatorESReindexVerificationTest, translatorESReplayVerificationTest} from "../build/utils/testing.js";

program
    .command('reindex-test <name>')
    .description('Run a reindex test')
    .action(async (name) => {
        const testPath = path.join(SCRIPTS_DIR, `tests/${name}.json`);
        const testParams = JSON.parse(readFileSync(testPath).toString());
        await translatorESReindexVerificationTest(testParams);
    });


program
    .command('replay-test <name>')
    .description('Run a replay test')
    .action(async (name) => {
        const testPath = path.join(SCRIPTS_DIR, `test/${name}.json`);
        const testParams = JSON.parse(readFileSync(testPath).toString());
        await translatorESReplayVerificationTest(testParams);
    });

program.parse(process.argv);
