import {Command} from "commander";
import {translatorESVerificationTest} from "../utils.mjs";

/*
 * Welcome to the 100K Toxic Blocks verification test
 *
 *     (See 1 Million Toxic Blocks description, basically same but adapted to
 *      be more lightweight so that it can run on gh CI, also uses a different
 *      block range)
 */
const program = new Command();
program
    .option('-b, --totalBlocks [totalBlocks]', 'Set a specific amount of blocks to sync, max 1,000,000')
    .option('-L, --limit [limit]', 'Number of documents per elasticdump import batch')
    .option('-l, --enableLatencyToxic', 'Apply random jitter to nodeos connection')
    .option('-d, --enableRandomDropsToxic', 'Apply random resets & timeouts to nodeos connection')
    .action(async (options) => {

        const toxi = {}
        if (options.enableLatencyToxic) {
            toxi.toxics = {};
            toxi.toxics.latency = {
                latency: 0, jitter: 500
            };
        }

        if (options.enableRandomDropsToxic) {
            if (!toxi.toxics)
                toxi.toxics = {};
            toxi.toxics.drops = true;
        }

        await translatorESVerificationTest({
            title: '100K Toxic Blocks',
            timeout: 20 * 60,  // 20 minutes in seconds
            resources: [
                {
                    type: 'esdump',
                    url: `http://storage.telos.net/test-resources/telos-mainnet-v1.5-312087080-100k-elasticdump.tar.zst`,
                    destinationPath: 'telos-mainnet-v1.5-312087080-100k-elasticdump.tar.zst',
                    decompressPath: 'telos-mainnet-v1.5-312087080-100k-elasticdump'
                },
                {
                    type: 'snapshot',
                    url: 'https://ops.store.eosnation.io/telos-snapshots/snapshot-2023-11-26-14-telos-v6-0312087079.bin.zst',
                    destinationPath: 'snapshot-2023-11-26-14-telos-v6-0312087079.bin.zst',
                    decompressPath: 'snapshot-2023-11-26-14-telos-v6-0312087079.bin'
                },
                {
                    type: 'nodeos',
                    url: 'http://storage.telos.net/test-resources/nodeos-mainnet-312087080-100k.tar.zst',
                    destinationPath: 'nodeos-mainnet-312087080-100k.tar.zst',
                    decompressPath: 'nodeos-mainnet-312087080-100k',
                    replay: true
                }
            ],
            elastic: {
                host: 'http://127.0.0.1:9200',
                verification: {
                    delta: 'telos-mainnet-verification-delta-v1.5-00000031',
                    action: 'telos-mainnet-verification-action-v1.5-00000031'
                },
                esDumpLimit: options.limit ? parseInt(options.limit, 10) : 4000
            },
            nodeos: {
                httpPort: 8888,  toxiHttpPort: 8889,
                shipPort: 29999, toxiShipPort: 30000
            },
            toxi,
            translator: {
                template: 'config.mainnet.json',
                startBlock: 312087081,
                totalBlocks: options.totalBlocks ? parseInt(options.totalBlocks, 10) : 100000 - 1,
                evmPrevHash: '501f3fe6f64c7d858a57e914ac130af2661e530f47ed9492606a44e1baecd6b6',
                stallCounter: 30
            }
        });
    });

program.parse(process.argv);