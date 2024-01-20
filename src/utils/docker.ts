import {packageInfo, SRC_DIR, TEST_RESOURCES_DIR} from "./indexer.js";
import {spawn} from "child_process";
import path from "node:path";

export const DOCKER_DIR = path.join(SRC_DIR, '../docker');

// Function to run a command in bg
export function runCommandInBackground(command: string, args: string[], cbs: {
    message?: (msg: any) => void;
    error?: (err: any) => void;
    close?: (code: number) => void;
}) {
    const child = spawn(command, args);

    // Handle standard output data
    child.stdout.on('data', (data) => {if (cbs.message) cbs.message(data);});

    // Handle standard error data
    child.stderr.on('data', (data) => {if (cbs.message) cbs.message(data);});

    // Handle error
    child.on('error', (error) => {
        console.error(`Error on background command!: ${error.message}`);
        console.error(error.stack);
        if (cbs.error)
            cbs.error(error)
    });

    // Handle close
    child.on('close', (code) => {
        if (cbs.close)
            cbs.close(code);
    });

    return child;
}

// Function to run a command and stream its output as utf-8
export async function runCommand(command: string, args: string[], printFn: (msg: any) => void) {
    const displayMessage = (msg) => {
        printFn(msg.toString().trimEnd());
    };
    return new Promise((resolve, reject) => {
        const child = runCommandInBackground(
            command, args, {message: displayMessage, error: reject, close: resolve}
        );
    });
}
export class DockerHelper {
    private static readonly DOCKER_PREFIX = `telosevm-translator-${packageInfo.version}`;

    static async buildDocker(name: string, buildDir: string): Promise<void> {
        const imageTag = `${DockerHelper.DOCKER_PREFIX}:${name}`;
        const buildExitCode = await runCommand(
            'docker',
            ['build', '-t', imageTag, buildDir],
            console.log
        );
        console.log(`Build for ${name} exited with code ${buildExitCode}`);
    }

    static async runCustomDocker(image: string, name: string, runArgs: string[], cmd?: string): Promise<void> {
        const finalRunArgs = ['run', `--name=${DockerHelper.DOCKER_PREFIX}-${name}`, ...runArgs, image];
        if (cmd) {
            finalRunArgs.push(cmd);
        }
        const runExitCode = await runCommand('docker', finalRunArgs, console.log);
        console.log(`Run docker ${name} process exited with code ${runExitCode}`);
    }

    static async runDocker(name: string, runArgs: string[], cmd?: string): Promise<void> {
        await DockerHelper.runCustomDocker(`${DockerHelper.DOCKER_PREFIX}:${name}`, name, runArgs, cmd);
    }

    static async stopCustomDocker(name: string): Promise<void> {
        const stopExitCode = await runCommand(
            'docker', ['stop', `${DockerHelper.DOCKER_PREFIX}-${name}`],
            console.log
        );
        console.log(`Stop docker ${name} process exited with code ${stopExitCode}`);
    }

    static async stopDocker(name: string): Promise<void> {
        await DockerHelper.stopCustomDocker(name);
    }

    static async killDocker(name: string): Promise<void> {
        const killExitCode = await runCommand(
            'docker', ['kill', `${DockerHelper.DOCKER_PREFIX}-${name}`],
            console.log
        );
        console.log(`Kill docker ${name} process exited with code ${killExitCode}`);
    }
}

// Translator

export async function startTranslatorDocker() {
    await DockerHelper.buildDocker('main', '.');
    await DockerHelper.runDocker(
        'telosevm-translator',
        ['-d', '--rm', '--network=host']
    );
}

export async function stopTranslatorDocker() {
    await DockerHelper.killDocker('main')
}

// Elasticsearch

export async function startElasticseachDocker() {
    const esDataMountPath = path.join(TEST_RESOURCES_DIR, 'elastic-data');

    await DockerHelper.buildDocker('elastic', 'docker/elastic');
    await DockerHelper.runDocker('elastic', [
        '-d',
        '--rm',
        '--mount', `type=bind,source=${esDataMountPath},target=/home/elasticsearch/data`,
        '--network=host'
    ]);
}

export async function stopElasticsearchDocker() {
    await DockerHelper.stopDocker('elastic');
}

// Kibana

export async function startKibanaDocker() {
    await DockerHelper.runCustomDocker('kibana:8.11.3', 'kibana', [
        '-d',
        '--rm',
        '--network=host',
        '--env', 'ELASTICSEARCH_HOSTS=http://localhost:9200'
    ]);
}

export async function stopKibanaDocker() {
    await DockerHelper.stopDocker('kibana');
}

// Toxi Proxy

export async function startToxiProxy() {
    await DockerHelper.runCustomDocker('ghcr.io/shopify/toxiproxy', 'toxiproxy', [
        '-d',
        '--rm',
        '--network=host'
    ]);
}

export async function stopToxiProxy() {
    await DockerHelper.stopDocker('toxiproxy')
}