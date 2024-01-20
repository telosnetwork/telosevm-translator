import { fileURLToPath } from 'node:url';
import path from 'node:path';
import fs from 'node:fs';

// currentDir == build/utils dir
const currentDir = path.dirname(fileURLToPath(import.meta.url));

export const ROOT_DIR = path.join(currentDir, '../..')
export const SRC_DIR = path.join(ROOT_DIR, 'src');
export const SCRIPTS_DIR = path.join(ROOT_DIR, 'scripts');
export const CONFIG_TEMPLATES_DIR = path.join(ROOT_DIR, 'config-templates');
export const TEST_RESOURCES_DIR = path.join(ROOT_DIR, 'build/tests/resources');

const packageJsonFile = path.join(ROOT_DIR, 'package.json');
export const packageInfo = JSON.parse(fs.readFileSync(packageJsonFile, 'utf-8'));

export const sleep = (ms: number) => new Promise(res => setTimeout(res, ms));