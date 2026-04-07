const fs = require('fs');
const files = require('./campaigns.json').files; // Replace 'files.json' with the actual path
const path = require('path');
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

function strftimeJS(format, date = new Date()) {
    const map = {
        "%Y": date.getFullYear().toString(),
        "%m": String(date.getMonth() + 1).padStart(2, "0"),
        "%d": String(date.getDate()).padStart(2, "0"),
        "%H": String(date.getHours()).padStart(2, "0"),
        "%M": String(date.getMinutes()).padStart(2, "0"),
        "%S": String(date.getSeconds()).padStart(2, "0"),
        "%y": String(date.getFullYear()).slice(-2),
        "%b": date.toLocaleString("en", { month: "short" }),
        "%B": date.toLocaleString("en", { month: "long" }),
        "%a": date.toLocaleString("en", { weekday: "short" }),
        "%A": date.toLocaleString("en", { weekday: "long" }),
    };

    return format.replace(/%[YmdHMSybBaA]/g, match => map[match] || match);
}


function getdate(date_run) {
    return today(date_run)
}

function isSimilar(file1 = '', fileName) {
    const file2 = path.parse(fileName).name;
    if (file1.startsWith(file2))
        return true;
    return false;
}

function run(param) {
    console.log(param)
    let date_to_run = today()
    if (param == 'today' || param == null || param == "") {
        param = today('', '-')
    } else {
        date_to_run = getdate(param)
    }

    console.log('date_to_run', date_to_run, param)
    try {
        files.forEach(f => {
            if (f['active']) {

                date_to_run = strftimeJS(f['date_format'], new Date(param));
                
                let file;
                let fileName = `${String(f['fileName'].replace('<YYYYMMDD>', date_to_run))}`;

                if (fs.existsSync(`${f['folder']}\\${fileName}`)) {
                    console.log('File exists:', fileName);
                    file = fileName;
                } else {
                    // Read all files in the directory
                    const files = fs.readdirSync(`${f['folder']}`);
                    const similarFiles = files.filter(file => isSimilar(file, fileName));
                    if (similarFiles.length > 0) {
                        file = similarFiles[0];
                    }
                }

                if (file) {
                    console.log('Similar file found:', file);
                    fs.rename(
                        `${f['folder']}\\${file}`
                        , `${f['folder']}\\${f['subfolder']}\\${file}`
                        , (err) => {
                            if (err) {
                                console.log('error', err)
                                // if (err) throw err;
                            };
                            console.log('File was moved to destination');
                        });
                } else {
                    console.log('File does not exist:', fileName, file);
                }
            } else {
                console.log('Skipping inactive file configuration:', f['fileName']);
            }
        });

        const date1 = new Date();
        const date2 = new Date(param);
        const diffTime = Math.abs(date2 - date1);
        const diffDays = Math.floor(diffTime / (1000 * 60 * 60 * 24));

        console.log('res', diffDays, date2)

        const pythonProcess = spawn('python', ['run_new.py', diffDays]);
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
