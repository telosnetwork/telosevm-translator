import { program } from 'commander';
import {
    startElasticseachDocker, stopElasticsearchDocker,
    startKibanaDocker, stopKibanaDocker,
    startToxiProxy, stopToxiProxy,
    startTranslatorDocker, stopTranslatorDocker
} from "../build/utils/docker.js";

program
    .command('start <name>')
    .description('Starts the specified docker container')
    .action(async (name) => {
        switch (name) {
            case "translator": {
                await startTranslatorDocker();
                break;
            }
            case "elastic": {
                await startElasticseachDocker();
                break;
            }
            case "kibana": {
                await startKibanaDocker();
                break;
            }
            case "toxiproxy": {
                await startToxiProxy();
                break;
            }
        }
    });

program
    .command('stop <name>')
    .description('Stops the specified docker container')
    .action(async (name) => {
        switch (name) {
            case "translator": {
                await stopTranslatorDocker();
                break;
            }
            case "elastic": {
                await stopElasticsearchDocker();
                break;
            }
            case "kibana": {
                await stopKibanaDocker();
                break;
            }
            case "toxiproxy": {
                await stopToxiProxy();
                break;
            }
        }
    });

program.parse(process.argv);