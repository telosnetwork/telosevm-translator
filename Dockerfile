from node:18-bullseye

copy . /translator

workdir /translator

run npm run build

cmd ["npm", "run", "start"]
