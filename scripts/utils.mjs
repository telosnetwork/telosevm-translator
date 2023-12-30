process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection at:', promise, 'reason:', reason);
    // Print the stack trace if available
    if (reason && reason.stack) {
        console.error(reason.stack);
    }
    // Exit the process with error code 3
    process.exit(3);
});

import path from "path";
import fs from "fs";

export const SCRIPTS_DIR = path.dirname(fileURLToPath(import.meta.url));
export const TEST_RESOURCES_DIR = path.join(SCRIPTS_DIR, '../build/tests/resources');

export function makeDirectory(directoryPath) {
    const resolvedPath = path.resolve(directoryPath);

    if (!fs.existsSync(resolvedPath)) {
        fs.mkdirSync(resolvedPath, { recursive: true });
        console.log(`Directory created: ${resolvedPath}`);
    } else {
        console.log(`Directory already exists: ${resolvedPath}`);
    }
}

import { spawn } from 'child_process';
import {fileURLToPath} from "node:url";


// Function to run a command in bg
export function runCommandInBackground(command, args, cbs) {
    const child = spawn(command, args);

    // Handle standard output data
    child.stdout.on('data', (data) => {if (cbs.message) cbs.message(data);});

    // Handle standard error data
    child.stderr.on('data', (data) => {if (cbs.message) cbs.message(data);});

    // Handle error
    child.on('error', (error) => {
        console.error(`Error on background command!: ${error.message}`);
        console.error(error.stack);
        if (cbs.error)
            cbs.error(error)
    });

    // Handle close
    child.on('close', (code) => {
        if (cbs.close)
            cbs.close(code);
    });

    return child;
}

// Function to run a command and stream its output as utf-8
export async function runCommand(command, args, printFn) {
    const displayMessage = (msg) => {
        printFn(msg.toString().trimEnd());
    };
    return new Promise((resolve, reject) => {
        const child = runCommandInBackground(
            command, args, {message: displayMessage, error: reject, close: resolve}
        );
    });
}

import zlib from 'zlib';
import lzma from 'lzma-native';
import zstd from 'node-zstandard';

export function decompressFile(filePath, outputPath) {
    const fileExtension = filePath.split('.').pop();

    switch (fileExtension) {
        case 'gz':
            // Using zlib for .gz files
            fs.createReadStream(filePath)
                .pipe(zlib.createGunzip())
                .pipe(fs.createWriteStream(outputPath));
            break;

        case 'xz':
            // Using lzma-native for .xz files
            fs.createReadStream(filePath)
                .pipe(lzma.createDecompressor())
                .pipe(fs.createWriteStream(outputPath));
            break;

        case 'zst':
        case 'zstd':
            // Using node-zstandard for .zstd or .zst files
            zstd.decompress(filePath, outputPath);
            break;

        default:
            console.log('Unsupported file format');
    }
}
