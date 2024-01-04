import {Command} from "commander";
import {translatorESVerificationTest} from "../utils.mjs";

/*
 * Welcome to the 1 Million Toxic Blocks verification test
 *
 * This test requires the following services running:
 *
 *     - elasticsearch @ 9200
 *     - toxiproxy @ 8474
 *
 * This test downloads the following files if not present already
 * (default location: build/tests/resources):
 *
 *     - telos-mainnet v6 snapshot from 2022-05-29 around block 218 million
 *     - block data & config files to start up a node with state for block range 218-219 million
 *     - generated es data from delta & actions indexes of a correct run
 *
 * Configurable params:
 *
 *     - totalBlocks: number of blocks to sync, max: 1,000,000
 *     - enableLatencyToxic: use toxiproxy to apply random jitter
 *     - enableRandomDropsToxic: use toxiproxy to do random timeouts and connection resets
 *
 * Overview:
 *
 *     - 1) Download & decompress required resources
 *     - 2) Start loading verification data into elastic
 *     - 3) Start replaying nodeos block data
 *     - 4) Await until both processes are done
 *     - 5) Apply toxiproxy configuration
 *     - 6) Launch translator and verify generated data
 */
const program = new Command();
program
    .option('-b, --totalBlocks [totalBlocks]', 'Set a specific amount of blocks to sync, max 1,000,000')
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
            title: '1 Million Toxic Blocks',
            timeout: 60 * 60,  // 60 minutes in seconds
            resources: [
                {
                    type: 'esdump',
                    url: `http://storage.telos.net/test-resources/telos-mainnet-v1.5-218m-219m-elasticdump.tar.zst`,
                    destinationPath: 'telos-mainnet-v1.5-218m-219m-elasticdump.tar.zst',
                    decompressPath: 'telos-mainnet-v1.5-218m-219m-elasticdump'
                },
                {
                    type: 'snapshot',
                    url: 'https://ops.store.eosnation.io/telos-snapshots/snapshot-2022-05-29-14-telos-v6-0217795040.bin.zst',
                    destinationPath: 'snapshot-2022-05-29-14-telos-v6-0217795040.bin.zst',
                    decompressPath: 'snapshot-2022-05-29-14-telos-v6-0217795040.bin'
                },
                {
                    type: 'nodeos',
                    url: 'http://storage.telos.net/test-resources/nodeos-mainnet-218m-219m.tar.zst',
                    destinationPath: 'nodeos-mainnet-218m-219m.tar.zst',
                    decompressPath: 'nodeos-mainnet-218m-219m',
                    replay: true
                }
            ],
            elastic: {
                host: 'http://127.0.0.1:9200',
                verification: {
                    delta: 'telos-mainnet-verification-delta-v1.5-00000021',
                    action: 'telos-mainnet-verification-action-v1.5-00000021'
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
                startBlock: 218000036,
                totalBlocks: options.totalBlocks ? parseInt(options.totalBlocks, 10) : 1000000,
                evmPrevHash: '77e77f63f70518490670d74b85a64560094d22d3a9924480458e478fdbfe4cd2',
                stallCounter: 2
            }
        });
    });

program.parse(process.argv);