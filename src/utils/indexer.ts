import { fileURLToPath } from 'node:url';
import path from 'node:path';
import fs from 'node:fs';
import EventEmitter from "events";

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

export async function waitEvent(emitter: EventEmitter, event: string): Promise<void> {
    new Promise(resolve => emitter.once(event, resolve));
}


import cloneDeep from "lodash.clonedeep";
import {TranslatorConfig} from "../types/indexer.js";
import {mergeDeep} from "./misc.js";

export function prepareTranslatorConfig(cfg: TranslatorConfig): TranslatorConfig {
    const config = cloneDeep(cfg);

    // if stop block not present fill -1
    if (typeof cfg.source.chain.stopBlock === 'undefined')
        config.source.chain.stopBlock = -1;

    // fill dst chain with source chain as default
    const dstChain = cloneDeep(cfg.source.chain);

    // if user provided dst chain config apply on top
    if (cfg.target.chain)
        mergeDeep(dstChain, cfg.target.chain);

    config.target.chain = dstChain;

    return config
}