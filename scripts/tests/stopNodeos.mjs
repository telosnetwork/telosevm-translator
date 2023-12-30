import {runCommand} from "../utils.mjs";
import {packageInfo} from "../../build/utils/indexer.js";

const stopExitCode = await runCommand(
    'docker', ['kill', `telosevm-translator-${packageInfo.version}-nodeos`],
    console.log
);
console.log(`Stop process exited with code ${stopExitCode}`);
