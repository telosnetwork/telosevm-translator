import {runCommand} from "./utils.mjs";


const stopExitCode = await runCommand(
    'docker', ['stop', 'telosevm-translator-elastic'],
    console.log
);
console.log(`Stop process exited with code ${stopExitCode}`);