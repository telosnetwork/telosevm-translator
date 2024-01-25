import {DEFAULT_CONF, TranslatorConfig} from './types/indexer.js';
import {TEVMIndexer} from './indexer.js';
import {readFileSync} from "node:fs";
import cloneDeep from "lodash.clonedeep";
import {mergeDeep} from "./utils/misc.js";
import { Command } from 'commander';

const program = new Command();

interface TranslatorCMDOptions {
    config: string;
    onlyDBCheck?: boolean;
}

program
    .option('-c, --config [path to config.json]', 'Path to config.json file', 'config.json')
    .option('-o, --only-db-check', 'Perform initial db check and exit', false)
    .action(async (options: TranslatorCMDOptions) => {
        const conf: TranslatorConfig = cloneDeep(DEFAULT_CONF);

        try {
            const userConf = JSON.parse(readFileSync(options.config).toString());
            mergeDeep(conf, userConf);
        } catch (e) {}

        if (options.onlyDBCheck)
            conf.runtime.onlyDBCheck = options.onlyDBCheck;

        if (process.env.LOG_LEVEL)
            conf.logLevel = process.env.LOG_LEVEL;

        if (process.env.READER_LOG_LEVEL)
            conf.readerLogLevel = process.env.READER_LOG_LEVEL;

        if (process.env.BROADCAST_HOST)
            conf.broadcast.wsHost = process.env.BROADCAST_HOST;

        if (process.env.BROADCAST_PORT)
            conf.broadcast.wsPort = parseInt(process.env.BROADCAST_PORT, 10);

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