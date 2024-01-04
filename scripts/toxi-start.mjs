import {runCustomDocker} from "./utils.mjs";

await runCustomDocker('ghcr.io/shopify/toxiproxy', 'toxiproxy', [
    '-d',
    '--rm',
    '--network=host'
]);
