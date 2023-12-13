import {spawn} from "child_process";


// Spawn the process
const process = spawn(
    'docker',
    ['build', '-t', `telosevm-translator:elastic`, 'docker/elastic']
);

// Handle standard output
process.stdout.on('data', (data) => {
    const lines = data.toString().split(/\r?\n/);
    lines.forEach((line) => {
        if (line) {
            console.log(line);
        }
    });
});

// Handle standard error
process.stderr.on('data', (data) => {
    const lines = data.toString().split(/\r?\n/);
    lines.forEach((line) => {
        if (line) {
            console.error(line);
        }
    });
});

// Handle process exit
process.on('close', (code) => {
    console.log(`Build exited with code ${code}`);

    // Spawn the process
    const launchProcess = spawn(
        'docker',
       [
           'run',
           '-d',
            '--rm',
            '--network=host',
            '--name=telosevm-translator-elastic',
            '--env', 'xpack.security.enabled=false',
            'telosevm-translator:elastic'
       ]
    );

    // Handle standard output
    launchProcess.stdout.on('data', (data) => {
        const lines = data.toString().split(/\r?\n/);
        lines.forEach((line) => {
            if (line) {
                console.log(line);
            }
        });
    });

    // Handle standard error
    launchProcess.stderr.on('data', (data) => {
        const lines = data.toString().split(/\r?\n/);
        lines.forEach((line) => {
            if (line) {
                console.error(line);
            }
        });
    });

    // Handle process exit
    launchProcess.on('close', (code) => {
        console.log(`Process exited with code ${code}`);
    });
});
