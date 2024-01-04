import {buildDocker, runDocker} from "./utils.mjs";

await buildDocker('elastic', 'docker/elastic');
await runDocker('elastic', [
    '-d',
    '--rm',
    '--network=host',
    '--env', 'xpack.security.enabled=false'
]);