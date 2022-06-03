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
        docker.elastic.co/kibana/kibana:8.2.1


### install dependencies

    npm i


### compile and run:

    npx tsc && node build/main.js
