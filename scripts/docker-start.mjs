import {runDocker} from "./utils.mjs";

await runDocker(
    'main',
    'telosevm-translator',
    ['-d', '--rm', '--network=host']
);