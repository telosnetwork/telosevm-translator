import {translatorESReindexVerificationTest} from "../utils.mjs";
import {Command} from "commander";


/*
 * Reindex 3 Million blocks right after mainnet evm deploy block, should contain most types of transactions.
 */
const program = new Command();
program
    .option('-b, --totalBlocks [totalBlocks]', 'Set a specific amount of blocks to sync', '3261468')
    .option('-p, --purge', 'Delete target indexes at the start', false)
    .option('-L, --limit [limit]', 'Number of documents per elasticdump import batch', '4000')
    .option('-T, --timeout [timeout m]', 'Timeout in minutes', '15')
    .action(async (options) => {
        await translatorESReindexVerificationTest({
            title: '3Mil Near Deploy',
            timeout: parseInt(options.timeout, 10) * 60,
            esDumpName: 'telos-mainnet-3mil-deploy',
            elastic: {
                host: 'http://127.0.0.1:9200',
                esDumpLimit:  parseInt(options.limit, 10),
                purge: options.purge
            },
            srcPrefix: 'telos-mainnet-verification',
            dstPrefix: 'telos-mainnet-reindex',
            indexVersion: 'v1.5-ebr-jt',
            template: 'config.mainnet.json',
            startBlock: 180698860,
            totalBlocks: parseInt(options.totalBlocks, 10),
            evmPrevHash: '757720a8e51c63ef1d4f907d6569dacaa965e91c2661345902de18af11f81063',
            esDumpSize: 10000,
            scrollSize: 10000,
            scrollWindow: '1m'
        });
    });

program.parse(process.argv);
