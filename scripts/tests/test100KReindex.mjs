import {translatorESReindexVerificationTest} from "../utils.mjs";
import {Command} from "commander";


/*
 * Reindex a 100k block range 312mil blocks in, this range was selected due to transaction density.
 */
const program = new Command();
program
    .option('-b, --totalBlocks [totalBlocks]', 'Set a specific amount of blocks to sync', '99999')
    .option('-p, --purge', 'Delete target indexes at the start', false)
    .option('-L, --limit [limit]', 'Number of documents per elasticdump import batch', '4000')
    .option('-T, --timeout [timeout m]', 'Timeout in minutes', '10')
    .action(async (options) => {
        await translatorESReindexVerificationTest({
            title: '100k late range',
            timeout: parseInt(options.timeout, 10) * 60,  // 20 minutes in seconds
            esDumpName: 'telos-mainnet-312m-100k',
            elastic: {
                host: 'http://127.0.0.1:9200',
                esDumpLimit: parseInt(options.limit, 10),
                purge: options.purge
            },
            srcPrefix: 'telos-mainnet-verification',
            dstPrefix: 'telos-mainnet-reindex',
            indexVersion: 'v1.5-ebr-jt',
            template: 'config.mainnet.json',
            startBlock: 312087081,
            totalBlocks: parseInt(options.totalBlocks, 10),
            evmPrevHash: '',
            evmValidateHash: '',
            esDumpSize: 10000,
            scrollSize: 10000,
            scrollWindow: '1m'
        });
    });

program.parse(process.argv);