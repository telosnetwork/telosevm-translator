import { copyFileSync } from 'node:fs';
import path from 'path';
import fs from 'fs';
import {SCRIPTS_DIR} from "./utils.mjs";

function makeDirectory(directoryPath) {
    const resolvedPath = path.resolve(directoryPath);

    if (!fs.existsSync(resolvedPath)) {
        fs.mkdirSync(resolvedPath, { recursive: true });
        console.log(`Directory created: ${resolvedPath}`);
    } else {
        console.log(`Directory already exists: ${resolvedPath}`);
    }
}

makeDirectory(path.join(SCRIPTS_DIR, '../build/tests/resources'));

// include package in build
const src = path.join(SCRIPTS_DIR, '../package.json');
const dest = path.join(SCRIPTS_DIR, '../build/package.json');

copyFileSync(src, dest);


