import path from "path";
import {existsSync, readFileSync} from "fs";
import {Client} from "@elastic/elasticsearch";
import {Command} from "commander";
import {runCommand, TEST_RESOURCES_DIR} from "../utils.mjs";


async function maybeLoadElasticDump(dumpName, elasticHost) {
    const dumpPath = path.join(TEST_RESOURCES_DIR, dumpName);
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

        const mappingArgs = [`--input=${mappingPath}`, `--output=${elasticHost}/${indexName}`, '--type=mapping'];
        await runCommand('elasticdump', mappingArgs, console.log);

        const dataArgs = [
            `--input=${dataPath}`, `--output=${elasticHost}/${indexName}`, '--type=data', '--limit=1000'
        ];
        await runCommand('elasticdump', dataArgs, console.log);
    }
}

const program = new Command();

program
    .argument('<dumpName>', 'Path to elasticdump directory relative to resources')
    .argument('<elasticHost>', 'Elasticsearch client connection info')
    .action(async (dumpName, elasticHost) => {
        await maybeLoadElasticDump(dumpName, elasticHost);
    });

program.parse(process.argv);