FROM node:lts-bullseye

RUN npm i pm2 -g

RUN mkdir -p /indexer/build

COPY src/ /indexer/src 
COPY package.json /indexer
COPY tsconfig.json /indexer
COPY config.json /indexer

WORKDIR /indexer

RUN npm i
RUN npx tsc

CMD ["pm2-runtime", "start", "build/main.js"]
