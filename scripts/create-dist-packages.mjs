import path from 'node:path';
import {fileURLToPath} from "node:url";
import fs, { copyFileSync } from 'node:fs';


export const SCRIPTS_DIR = path.dirname(fileURLToPath(import.meta.url));
``
function makeDirectory(directoryPath) {
    const resolvedPath = path.resolve(directoryPath);

    if (!fs.existsSync(resolvedPath)) {
        fs.mkdirSync(resolvedPath, { recursive: true });
        console.log(`Directory created: ${resolvedPath}`);
    }
}

makeDirectory(path.join(SCRIPTS_DIR, '../build/tests/resources'));

// include package in build
const src = path.join(SCRIPTS_DIR, '../package.json');
const dest = path.join(SCRIPTS_DIR, '../build/package.json');

copyFileSync(src, dest);


