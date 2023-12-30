import {runCommand} from "./utils.mjs";

const stopExitCode = await runCommand(
    'docker', ['stop', 'telosevm-translator-kibana'],
    console.log
);
console.log(`Stop process exited with code ${stopExitCode}`);