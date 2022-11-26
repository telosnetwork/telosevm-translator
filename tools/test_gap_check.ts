const { Client } = require('@elastic/elasticsearch');

const url = 'http://localhost:9200';
const chainName = 'telos-mainnet'
const elasticConf = {
    "node": url,
    "auth": {
        "username": "elastic",
        "password": "password"
    },
    "docsPerIndex": 1000000,
    "subfix": {
        "delta": "delta-v1.5",
        "transaction": "action-v1.5",
        "error": "error-v1.5"
    }
};

const es = new Client(elasticConf);

async function getOrderedIndices() {
    const deltaIndices: Array<ElasticIndex> = await es.cat.indices({
        index:  `${chainName}-${elasticConf.subfix.delta}-*`,
        format: 'json'
    });
    deltaIndices.sort((a, b) => {
        const aNum = indexToSuffixNum(a.index);
        const bNum = indexToSuffixNum(b.index);
        if (aNum < bNum)
            return -1;
        if (aNum > bNum)
            return 1;
        return 0;
    });
    return deltaIndices;
}

async function getFirstIndexedBlock() {
    const firstIndex = (await getOrderedIndices()).shift().index;
    try {
        const result = await es.search({
            index: firstIndex,
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
    const lastIndex = (await getOrderedIndices()).pop().index;
    try {
        const result = await es.search({
            index: lastIndex,
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
                    "min_doc_count": 0
                },
                "aggs": {
                    "min_block": {
                        "min": {
                            "field": "@global.block_num"
                        },
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

    const len = results.aggregations.block_histogram.buckets.length;
    for (let i = 0; i < len; i++) {

        const bucket = results.aggregations.block_histogram.buckets[i];
        const lower = bucket.min_block.value;
        const upper = bucket.max_block.value;
        const total = bucket.doc_count;
        const totalRange = (upper - lower) + 1;
        let hasGap = totalRange != total;

        if (len > 1 && i < (len - 1)) {
            const nextBucket = results.aggregations.block_histogram.buckets[i+1];
            hasGap = hasGap || (nextBucket.key - upper) != 1;
        }

        if (hasGap)
            return [lower, upper];
    }
    return null;
}

export async function fullGapCheck() {
    // @ts-ignore
    const lowerBound = (await getFirstIndexedBlock())['@global'].block_num;
    // @ts-ignore
    const upperBound = (await getLastIndexedBlock())['@global'].block_num;

    console.log(`performing gap check from ${lowerBound} to ${upperBound}`);

    let interval = 10000000;
    let gap: Array<number> = [lowerBound, upperBound];

    while (interval >= 10 && gap != null) {
        gap = await gapCheck(gap[0], gap[1], interval);
        console.log(`checked ${JSON.stringify(gap)} with interval ${interval}, ${gap}`);
        interval /= 10;
    }

    if (gap == null)
        return null;

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

    return lower + 1;
}

function getSubfix(blockNum: number) {
    return String(Math.floor(blockNum / elasticConf.docsPerIndex)).padStart(8, '0');
}

function indexToSuffixNum(index: string) {
    const spltIndex = index.split('-');
    const suffix = spltIndex[spltIndex.length - 1];
    return parseInt(suffix);
}

async function purgeIndicesNewerThan(blockNum: number) {
    const targetSuffix = getSubfix(blockNum);
    const targetNum = parseInt(targetSuffix);

    const deleteList = [];

    const deltaIndices = await es.cat.indices({
        index:  `${chainName}-${elasticConf.subfix.delta}-*`,
        format: 'json'
    });

    console.log(JSON.stringify(deltaIndices, null, 4));

    for (const deltaIndex of deltaIndices)
        if (indexToSuffixNum(deltaIndex.index) > targetNum)
            deleteList.push(deltaIndex.index);

    const actionIndices = await es.cat.indices({
        index:  `${chainName}-${elasticConf.subfix.transaction}-*`,
        format: 'json'
    });

    for (const actionIndex of actionIndices)
        if (indexToSuffixNum(actionIndex.index) > targetNum)
            deleteList.push(actionIndex.index);

    console.log('indices to delete: ');
    console.log(JSON.stringify(deleteList, null, 4));

    // const resp = await es.indices.delete({
    //     index: deleteList
    // });
    // console.log(JSON.stringify(resp, null, 4));

    return deleteList;
}

interface ElasticIndex {
    "health": string;
    "status": string;
    "index": string;
    "uuid": string;
    "pri": string;
    "rep": string;
    "docs.count": string;
    "docs.deleted": string;
    "store.size": string;
    "pri.store.size": string;
}

const main = async () => {
    const gap = await fullGapCheck();
    console.log(`found gap at ${gap}`);

    if (gap == null)
        return;

    await purgeIndicesNewerThan(gap);
}

main();
