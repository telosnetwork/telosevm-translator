import Downloader from "nodejs-file-downloader";
import {Command} from "commander";
import {existsSync} from "fs";
import path from "path";
import {decompressFile} from "../utils.mjs";
import zstd from "node-zstandard";

const program = new Command();

program
    .argument('<targetUrl>', 'url to download from')
    .argument('<destinationPath>', 'path to save the file to')
    .option('-f, --force', 'Download even if exists and overwrite')
    .option('-d, --decompress', 'If its a tarfile, also decompress in same dir')
    .action(async (targetUrl, destinationPath, options) => {
        if (existsSync(destinationPath) && !options.force) {
            console.log(`${destinationPath} exists and --force not passed, skipping...`);
            process.exit(0);
        }

        const downloader = new Downloader({
            url: targetUrl,
            directory: path.dirname(destinationPath),
            fileName: path.basename(destinationPath),
            cloneFiles: false,
            onProgress(percentage, chunk, remainingSize) {
                console.log(`${percentage.padEnd(7, ' ')}% Remaining bytes: ${remainingSize}`);
            }
        });
        try {
            const downloadResult = await downloader.download();
            console.log('Download succeded!')
            console.log(downloadResult);

            const downloadPath = downloadResult.filePath;
            let finalPath = downloadPath;

            if (options.decompress) {
                const parsedOutputPath = path.parse(downloadPath);
                const extensionlessPath = parsedOutputPath.name;
                if (downloadPath.includes('.tar')) {
                    console.log(`${downloadPath} is a tarfile, attempting decompress...`);
                    decompressFile(downloadPath, extensionlessPath);
                    finalPath = extensionlessPath;
                } else if (downloadPath.includes('.zst')) {
                    console.log(`${downloadPath} is a zstandard compress file, attempting decompress...`);
                    zstd.decompress(downloadPath, extensionlessPath);
                    finalPath = extensionlessPath;
                }
            }

            console.log(`Downloaded resource to ${finalPath}`);
            process.exit(0);

        } catch (e) {
            console.error('Download failed:');
            console.error(JSON.stringify(e, null, 4));
            process.exit(1);
        }
    });

program.parse(process.argv);
