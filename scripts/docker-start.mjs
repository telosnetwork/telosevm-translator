import {packageInfo} from "../build/utils/indexer.js";
import {runCommand} from "./utils.mjs";

const launchExitCode = await runCommand(
    'docker',
    [
        'run',
        '-d',  // detach
        '--rm',  // remove after use
        '--network=host',
        `--name=telosevm-translator-${packageInfo.version}`,
        `telosevm-translator:${packageInfo.version}`
    ],
    console.log
);
console.log(`Launch process exited with code ${launchExitCode}`);
