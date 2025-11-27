const fs = require('fs');
const files = require('./campaigns.json').files; // Replace 'files.json' with the actual path
const { spawn } = require('child_process');

function today(date_run = '', replaceSTring = "") {
    let yourDate = ''
    if (date_run == '') {
        yourDate = new Date()
    } else {
        yourDate = new Date(date_run)

    }
    yourDate.toISOString().split('T')[0]
    const offset = yourDate.getTimezoneOffset()

    yourDate = new Date(yourDate.getTime() - (offset * 60 * 1000))
    const date = yourDate.toISOString().split('T')[0]
    return date.split('-').join(replaceSTring)
}

function getdate(date_run) {
    return today(date_run)
}

function run(param) {
    console.log(param)
    let date_to_run = ''
    if (param == 'today') {
        date_to_run = today()
        param = today('', '-')
    } else {
        date_to_run = getdate(param)
    }

    console.log('date_to_run', date_to_run, param)
    try {

        files.forEach(f => {

            fs.rename(`${f['folder']}\\${String(f['fileName'].replace('<YYYYMMDD>', date_to_run))}`, `${f['folder']}\\${f['subfolder']}\\${String(f['fileName'].replace('<YYYYMMDD>', date_to_run))}`, (err) => {
                if (err) {
                    console.log('error', err)
                    // if (err) throw err;
                };
                console.log('File was moved to destination');
            });
        });

        const date1 = new Date();
        const date2 = new Date(param);
        const diffTime = Math.abs(date2 - date1);
        const diffDays = Math.floor(diffTime / (1000 * 60 * 60 * 24));

        console.log('res', diffDays, date2)

        const pythonProcess = spawn('python', ['run.py', diffDays]);
        pythonProcess.stdout.on('data', (data) => {
            console.log(`Python output: ${data.toString()}`);
        });
        pythonProcess.stderr.on('data', (data) => {
            console.error(`Python error: ${data.toString()}`);
        });

        pythonProcess.on('close', (code) => {
            console.log(`Child process exited with code ${code}`);
        });
    } catch (error) {
        console.log(error);

    }
}

run(String(process.argv[2]))