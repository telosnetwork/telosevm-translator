import {runCommand} from "./utils.mjs";
import {packageInfo} from "../build/utils/indexer.js";

const buildExitCode = await runCommand(
    'docker',
    ['build', '-t', `telosevm-translator:${packageInfo.version}`, '.'],
    console.log
);
console.log(`Build exited with code ${buildExitCode}`);