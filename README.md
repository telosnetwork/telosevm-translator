# telosevm elasticsearch indexer

Consume `nodeos` state history endpoint and take and EVM dump.


### launch elastic:

    docker run \
        -it \
        --rm \
        --network=host \
        --mount type=bind,source="$(pwd)"/elastic,target=/usr/share/elasticsearch/data \
        --env "ELASTIC_USERNAME=elastic" \
        --env "ELASTIC_PASSWORD=password" \
        --env "xpack.security.enabled=false" \
        telosevm-indexer:elastic

### launch kibana:

    docker run \
        -it \
        --rm \
        --network=host \
        --env "ELASTICSEARCH_HOSTS=http://localhost:9200" \
        --env "ELASTICSEARCH_USERNAME=elastic" \
        --env "ELASTICSEARCH_PASSWORD=password" \
        docker.elastic.co/kibana/kibana:7.17.4


### install dependencies

    npm i


### compile and run:

    npx tsc && node build/main.js
