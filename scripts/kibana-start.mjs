import {runCommand} from "./utils.mjs";

const launchExitCode = await runCommand(
    'docker',
    [
        'run',
        '-d',
        '--rm',
        '--network=host',
        '--name=telosevm-translator-kibana',
        '--env', 'ELASTICSEARCH_HOSTS=http://localhost:9200',
        'kibana:8.11.3'
    ],
    console.log
);
console.log(`Launch process exited with code ${launchExitCode}`);
