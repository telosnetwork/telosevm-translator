import {buildDocker, runDocker, TEST_RESOURCES_DIR} from "./utils.mjs";
import path from "path";

const esDataMountPath = path.join(TEST_RESOURCES_DIR, 'elastic-data');

await buildDocker('elastic', 'docker/elastic');
await runDocker('elastic', [
    '-d',
    '--rm',
    '--mount', `type=bind,source=${esDataMountPath},target=/home/elasticsearch/data`,
    '--network=host',
    '--env', 'xpack.security.enabled=false'
]);