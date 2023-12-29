import { copyFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'path';
import fs from 'fs';

function makeDirectory(directoryPath) {
    const resolvedPath = path.resolve(directoryPath);

    if (!fs.existsSync(resolvedPath)) {
        fs.mkdirSync(resolvedPath, { recursive: true });
        console.log(`Directory created: ${resolvedPath}`);
    } else {
        console.log(`Directory already exists: ${resolvedPath}`);
    }
}

const currentDir = path.dirname(fileURLToPath(import.meta.url));

makeDirectory(path.join(currentDir, '../build/tests/resources'));

// include package in build
const src = path.join(currentDir, '../package.json');
const dest = path.join(currentDir, '../build/package.json');

copyFileSync(src, dest);


