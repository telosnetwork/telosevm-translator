from node:18-bullseye

run mkdir -p /indexer/build

copy src/ /indexer/src 
copy package.json /indexer
copy tsconfig.json /indexer
copy config.json /indexer

copy hyperion-seq /hyperion-sequential-reader

workdir /indexer

run yarn install
run npx tsc

cmd ["node", "--max-old-space-size=4096", "build/main.js"]
