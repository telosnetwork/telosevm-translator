import os from "node:os";
import fs from "node:fs";
import path from "node:path";
import {makeDirectory} from "./resources.js";
import {packageInfo, sleep, TEST_RESOURCES_DIR} from "./indexer.js";
import {runCommand, runCommandInBackground} from "./docker.js";

export interface NodeosOptions {
    runtimeDir: string;
    dataDir: string;
    snapshot: string;
    replay: boolean;
}

export async function launchNodeos(
    dataSource: string,
    snapshotName: string,
    logFile: string,
    options: {
        replay: boolean;
        nodeosRootDir: string;
    }
) {
    let nodeosRootDir;
    if (options.nodeosRootDir)
        nodeosRootDir = options.nodeosRootDir;
    else
        nodeosRootDir = path.join(os.tmpdir(), 'nodeos');

    if (fs.existsSync(nodeosRootDir))
        throw new Error(`${nodeosRootDir} already exists.`)

    makeDirectory(nodeosRootDir);

    const dataSourcePath = path.join(TEST_RESOURCES_DIR, dataSource);
    if (!fs.existsSync(dataSourcePath))
        throw new Error(`${dataSource} not found at ${dataSourcePath} !`);

    console.log(`Copying ${dataSource} to ${nodeosRootDir}`);
    fs.cpSync(dataSourcePath, nodeosRootDir, {recursive: true});

    const hostSnapshotPath = path.join(TEST_RESOURCES_DIR, snapshotName);
    const guestSnapshotPath = path.join(nodeosRootDir, snapshotName);
    console.log(`Copying ${hostSnapshotPath} to ${guestSnapshotPath}`);
    fs.cpSync(hostSnapshotPath, guestSnapshotPath);

    const nodeosCmd = [
        'nodeos',
        '--config-dir=/root/target',
        '--data-dir=/root/target/data',
        '--disable-replay-opts',
        `--snapshot=/root/target/${snapshotName}`
    ];

    if (options.replay)
        nodeosCmd.push('--replay-blockchain');

    nodeosCmd.push(
        '>>',
        `/root/target/${logFile}`,
        '2>&1'
    );

    const containerCmd = ['/bin/bash', '-c', nodeosCmd.join(' ')];

    console.log(`Launching nodeos with cmd:\n${nodeosCmd}`)
    const launchExitCode = await runCommand(
        'docker',
        [
            'run',
            '-d',  // detach
            '--rm',  // remove after use
            '--network=host',
            '--mount', `type=bind,source=${nodeosRootDir},target=/root/target`,
            `--name=telosevm-translator-${packageInfo.version}-nodeos`,
            'guilledk/py-leap:leap-subst-4.0.4-1.0.1',
            ...containerCmd
        ], console.log
    );
    if (launchExitCode !== 0)
        throw new Error(`Nodeos launch ended with non zero exit code ${launchExitCode}`);
}

export async function initializeNodeos(nodeosOptions: NodeosOptions) {
    let isNodeosUp = false;
    const runtimeDir = path.join(os.tmpdir(), nodeosOptions.runtimeDir);
    const logFile = 'nodeos.log';
    const logfileHostPath = path.join(runtimeDir, logFile);
    await launchNodeos(
        nodeosOptions.dataDir,
        nodeosOptions.snapshot,
        logFile,
        {nodeosRootDir: runtimeDir, replay: nodeosOptions.replay}
    );
    const nodeosTail = runCommandInBackground(
        'tail', ['-f', logfileHostPath],
        {
            message: (msg) => {
                const msgStr = msg.toString().trimEnd();
                if (msgStr.includes('acceptor_.listen()'))
                    isNodeosUp = true;
                console.log('nodeos-replay: ' + msg.toString().trimEnd())
            },
            error: (error) => {
                console.error(`Nodeos log tail process error: ${error.message}`);
                console.error(error.stack);
                throw error;
            },
            close: (code) => {
                if (code !== 0)
                    console.error(`Nodeos log tail process closed with non zero exit code: ${code}`);
                else
                    console.log('Nodeos log tail process closed with exit code 0');
            }
        }
    )
    while (!isNodeosUp) await sleep(1000);
    nodeosTail.kill('SIGTERM');
}
