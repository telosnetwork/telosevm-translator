import {spawn} from "child_process";
import {packageInfo} from "../build/utils/indexer.js";

// Spawn the process
const process = spawn(
    'docker',
    ['build', '-t', `telosevm-translator:${packageInfo.version}`, '.']
);

// Handle standard output
process.stdout.on('data', (data) => {
    const lines = data.toString().split(/\r?\n/);
    lines.forEach((line) => {
        if (line) {
            console.log(line);
        }
    });
});

// Handle standard error
process.stderr.on('data', (data) => {
    const lines = data.toString().split(/\r?\n/);
    lines.forEach((line) => {
        if (line) {
            console.error(line);
        }
    });
});

// Handle process exit
process.on('close', (code) => {
    console.log(`Process exited with code ${code}`);
});