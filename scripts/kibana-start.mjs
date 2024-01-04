import {runCustomDocker} from "./utils.mjs";

await runCustomDocker('kibana:8.11.3', 'kibana', [
        '-d',
        '--rm',
        '--network=host',
        '--env', 'ELASTICSEARCH_HOSTS=http://localhost:9200'
]);
