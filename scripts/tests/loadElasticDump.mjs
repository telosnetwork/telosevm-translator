import path from "path";
import {fileURLToPath} from "node:url";
import {existsSync, readFileSync} from "fs";
import {Client} from "@elastic/elasticsearch";
import { spawn } from 'child_process';
import {Command} from "commander";

// Function to run a command and stream its output
async function runCommand(command, args, listener) {
    return new Promise((resolve, reject) => {
        const child = spawn(command, args);

        // Handle standard output data
        child.stdout.on('data', listener);

        // Handle standard error data
        child.stderr.on('data', listener);

        // Handle error
        child.on('error', (error) => {
            console.error(`Error: ${error.message}`);
            reject(error);
        });

        // Handle close
        child.on('close', (code) => {
            console.log(`Child process exited with code ${code}`);
            resolve(code);
        });
    });
}

const currentDir = path.dirname(fileURLToPath(import.meta.url));
export const testResourcesDir = path.join(currentDir, '../../build/tests/resources');

async function maybeLoadElasticDump(dumpName, elasticHost) {
    const dumpPath = path.join(testResourcesDir, dumpName);
    if (!existsSync(dumpPath))
        throw new Error(`elasticdump directory not found at ${dumpPath}`);

    const manifestPath = path.join(dumpPath, 'manifest.json');
    if (!existsSync(manifestPath))
        throw new Error(`elasticdump manifest not found at ${manifestPath}`);

    const es = new Client({node: elasticHost});

    const manifest = JSON.parse(readFileSync(manifestPath).toString());
    for (const [indexName, indexInfo] of Object.entries(manifest)) {

        // // TODO: temporal delete every time to test
        // try {
        //     await es.indices.delete({index: indexName});
        // } catch (e) { console.log(e); }

        const mappingPath = path.join(dumpPath, indexInfo.mapping);
        const dataPath = path.join(dumpPath, indexInfo.data);
        if ((!existsSync(mappingPath)) || (!existsSync(dataPath)))
            throw new Error(`index file not found ${mappingPath} or ${dataPath}`);

        // expect index_not_found_exception
        try {
            await es.cat.indices({
                index: indexName, format: 'json'
            });
            console.log(`skipping ${indexName} as its already present...`);
            continue;
        } catch (e) {}

        await es.indices.create({index: indexName});

        const displayProcOutput = (msg) => console.log(msg.toString());
        const mappingArgs = [`--input=${mappingPath}`, `--output=${elasticHost}/${indexName}`, '--type=mapping'];
        await runCommand('elasticdump', mappingArgs, displayProcOutput);

        const dataArgs = [
            `--input=${dataPath}`, `--output=${elasticHost}/${indexName}`, '--type=data', '--limit=1000'
        ];
        await runCommand('elasticdump', dataArgs, displayProcOutput);
    }
}

const program = new Command();

program
    .option('-d, --dumpName [dumpName]', 'Path to elasticdump directory relative to resources')
    .option('-e, --elasticHost [elasticHost]', 'Elasticsearch client connection info')
    .action(async (options) => {
        await maybeLoadElasticDump(options.dumpName, options.elasticHost);
    });

console.log('test');