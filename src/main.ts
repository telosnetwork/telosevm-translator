import {TEVMIndexer} from './indexer.js';
import {readFileSync} from "node:fs";
import { Command } from 'commander';
import {TranslatorConfig, TranslatorConfigSchema} from "./types/config.js";

const program = new Command();

interface TranslatorCMDOptions {
    config: string;
    onlyDBCheck?: boolean;
}

program
    .option('-c, --config [path to config.json]', 'Path to config.json file', 'config.json')
    .option('-o, --only-db-check', 'Perform initial db check and exit', false)
    .action(async (options: TranslatorCMDOptions) => {
        let conf: TranslatorConfig = TranslatorConfigSchema.parse(
            JSON.parse(readFileSync(options.config).toString())
        );

        if (options.onlyDBCheck)
            conf.runtime.onlyDBCheck = options.onlyDBCheck;

        if (process.env.LOG_LEVEL)
            conf.logLevel = process.env.LOG_LEVEL;

        if (process.env.READER_LOG_LEVEL)
            conf.readerLogLevel = process.env.READER_LOG_LEVEL;

        const indexer = new TEVMIndexer(conf);
        try {
            await indexer.launch();
        } catch (e) {
            if (
                e.message.includes('Gap found in database at ') &&
                e.message.includes(', but --gaps-purge flag not passed!')
            ) {
                // @ts-ignore
                indexer.logger.error(
                    'Translator integrity check failed, ' +
                    'not gonna start unless --gaps-purge is passed or ' +
                    'config option runtime.gapsPurge == true'
                );
                process.exitCode = 47;
            } else {
                throw e;
            }
        }
    });

program.parse(process.argv);