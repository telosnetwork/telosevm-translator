

// assume elastic, nodeos & toxiproxy are running

import {maybeLoadElasticDump} from "./utils.js";
import {Client} from "@elastic/elasticsearch";

const elasticHost = 'http://localhost:9200';
const toxiproxyHost = 'http://localhost:8474';

describe('toxi proxy', async function () {
    const es = new Client({node: elasticHost});

    before(async function() {
        await maybeLoadElasticDump('telos-mainnet-v1.5-218m-219m-elasticdump', elasticHost);
    })

    it('1 million block sync behind toxiproxy', async function() {

    });
});