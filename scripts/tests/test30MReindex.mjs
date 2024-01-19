import {translatorESReindexVerificationTest} from "../utils.mjs";
import {Command} from "commander";


/*
 * Reindex data over 3 different indexes, used to test BlockScroller index change mechanic
 */
const program = new Command();
program
    .option('-b, --totalBlocks [totalBlocks]', 'Set a specific amount of blocks to sync', '29999963')
    .option('-p, --purge', 'Set a specific amount of blocks to sync', false)
    .option('-L, --limit [limit]', 'Number of documents per elasticdump import batch', '4000')
    .option('-T, --timeout [timeout m]', 'Timeout in minutes', '100')
    .action(async (options) => {
        await translatorESReindexVerificationTest({
            title: '30Mil Index Change Test',
            timeout: parseInt(options.timeout, 10) * 60,
            esDumpName: 'telos-mainnet-30M-ebr',
            elastic: {
                host: 'http://127.0.0.1:9200',
                esDumpLimit: parseInt(options.limit, 10),
                purge: options.purge
            },
            srcPrefix: 'telos-mainnet-verification',
            dstPrefix: 'telos-mainnet-reindex',
            indexVersion: 'v1.5',
            template: 'config.mainnet.json',
            startBlock: 36,
            totalBlocks: parseInt(options.totalBlocks, 10),
            evmPrevHash: '',
            evmValidateHash: '36fe7024b760365e3970b7b403e161811c1e626edd68460272fcdfa276272563',
            esDumpSize: 10000,
            scrollSize: 10000,
            scrollWindow: '1m'
        });
    });

program.parse(process.argv);