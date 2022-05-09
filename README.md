# telosevm elasticsearch indexer

Consume `nodeos` state history endpoint and take and EVM dump.


### launch elastic:

    docker run \
        -it \
        --rm \
        --network=host \
        --mount type=bind,source="$(pwd)"/elastic,target=/usr/share/elasticsearch/data \
        telosevm-indexer:elastic

### launch kibana:

    docker run \
        -it \
        --rm \
        --network=host \
        --env ELASTICSEARCH_HOSTS="http://localhost:9200" \
        --env ELASTICSEARCH_USERNAME="elastic" \
        --env ELASTICSEARCH_PASSWORD="password" \
        docker.elastic.co/kibana/kibana:7.16.3


### install dependencies

    npm i


### compile and run:

    npx tsc && node build/main.js
