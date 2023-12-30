import {packageInfo} from "../../build/utils/indexer.js";
import {Command} from "commander";
import * as os from "os";
import fs from "fs";
import {makeDirectory, runCommand, TEST_RESOURCES_DIR} from "../utils.mjs";
import path from "path";


const program = new Command();

program
    .argument('<dataSource>', 'nodeos data source relative to test resources dir')
    .argument('<snapshotPath>', 'path to snapshot to get init info from, relative to dataSource dir')
    .option('-n, --nodeosRootDir [nodeosRootDir]', 'Path root dir that gonna be bind mounted to /root/target in nodeos')
    .action(async (dataSource, snapshotPath, options) => {

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

        const nodeosCmd = [
            '/bin/bash', '-c',
                ['nodeos',
                    '--config-dir=/root/target',
                    '--data-dir=/root/target/data',
                    '--replay-blockchain',
                    '--disable-replay-opts',
                    `--snapshot=/root/target/${snapshotPath}`,
                    '>>',
                    '/root/target/nodeos.log',
                    '2>&1'
                ].join(' ')
        ];

        console.log(`launching nodeos with cmd:\n${nodeosCmd}`)
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
                ...nodeosCmd
            ], console.log
        );

        console.log(`Launch process exited with code ${launchExitCode}`);
    });

program.parse(process.argv);