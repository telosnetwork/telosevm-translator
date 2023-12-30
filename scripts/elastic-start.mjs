import {runCommand} from "./utils.mjs";


const buildExitCode = await runCommand(
    'docker',
    ['build', '-t', `telosevm-translator:elastic`, 'docker/elastic'],
    console.log
);
console.log(`Build exited with code ${buildExitCode}`);

const launchExitCode = await runCommand(
    'docker',
   [
       'run',
       '-d',
        '--rm',
        '--network=host',
        '--name=telosevm-translator-elastic',
        '--env', 'xpack.security.enabled=false',
        'telosevm-translator:elastic'
   ],
    console.log
);
console.log(`Launch process exited with code ${launchExitCode}`);