const { Client, ApiResponse } = require('@elastic/elasticsearch');

const url = 'http://test1.us.telos.net:9200';
const chainName = 'telos-mainnet'
const elasticConf = {
    "node": url,
    "auth": {
        "username": "elastic",
        "password": "password"
    },
    "subfix": {
        "delta": "delta-v1.5",
        "transaction": "action-v1.5",
        "error": "error-v1.5"
    }
};

const es = new Client(elasticConf);

async function getFirstIndexedBlock() {
    try {
        const result = await es.search({
            index: `${chainName}-${elasticConf.subfix.delta}-*`,
            size: 1,
            sort: [
                {"block_num": { "order": "asc"} }
            ]
        });

        return result?.hits?.hits[0]?._source;

    } catch (error) {
        return null;
    }
}

async function getLastIndexedBlock() {
    try {
        const result = await es.search({
            index: `${chainName}-${elasticConf.subfix.delta}-*`,
            size: 1,
            sort: [
                {"block_num": { "order": "desc"} }
            ]
        });

        return result?.hits?.hits[0]?._source;

    } catch (error) {
        return null;
    }
}

const gapCheck = async (
    lowerBound: number,
    upperBound: number,
    interval: number
) => {
    const results = await es.search({
        index: `${chainName}-${elasticConf.subfix.delta}-*`,
        aggs: {
            "block_histogram": {
                "histogram": {
                    "field": "@global.block_num",
                    "interval": interval,
                    "min_doc_count": 1
                },
                "aggs": {
                    "min_block": {
                        "min": {
                            "field": "@global.block_num"
                        }
                    },
                    "max_block": {
                        "max": {
                            "field": "@global.block_num"
                        }
                    }
                }
            }
        },
        size: 0,
        query: {
            "bool": {
                "must": [
                    {
                        "range": {
                            "@global.block_num": {
                                "gte": lowerBound,
                                "lte": upperBound
                            }
                        }
                    }
                ]
            }
        }
    });

    // FIRST GAP CHECK
    for (const bucket of results.aggregations.block_histogram.buckets) {
        const lower = bucket.min_block.value;
        const upper = bucket.max_block.value;
        const total = bucket.doc_count;
        const totalRange = (upper - lower) + 1;
        const hasGap = totalRange != total;

        if (hasGap)
            return [lower, upper];
    }
    return null;
}

const main = async () => {
    // @ts-ignore
    const lowerBound = (await getFirstIndexedBlock())['@global'].block_num;
    // @ts-ignore
    const upperBound = (await getLastIndexedBlock())['@global'].block_num;

    let interval = 10000000;
    let gap: Array<number> = [lowerBound, upperBound];

    while (interval >= 10 && gap != null) {
        gap = await gapCheck(gap[0], gap[1], interval);
        console.log(`checked ${JSON.stringify(gap)} with interval ${interval}, ${gap}`);
        interval /= 10;
    }

    let lower = gap[0];
    let upper = gap[1];
    gap = await gapCheck(lower, upper, 10);
    while(gap != null) {
        console.log(gap);
        lower += 1;
        gap = await gapCheck(lower, upper, 10);
    }
    lower -= 1;

    gap = await gapCheck(lower, upper, 10);
    while(gap != null) {
        console.log(gap);
        upper -= 1;
        gap = await gapCheck(lower, upper, 10);
    }
    upper += 1;

    console.log(`found gap at ${lower + 1}`);
}

main();
