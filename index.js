const fs = require('fs');


function today(replaceSTring="") {
    let yourDate = new Date('2025-11-19')
    yourDate.toISOString().split('T')[0]
    const offset = yourDate.getTimezoneOffset()

    yourDate = new Date(yourDate.getTime() - (offset * 60 * 1000))
    const date = yourDate.toISOString().split('T')[0]
    return date.split('-').join(replaceSTring)
}

console.log(today())

fs.rename(`${server_from_new_Sales}\\${camp}\\${rec}`, `${server_from_old_Sales}\\${destination}\\${rec}`, (err) => {
    if (err) {
        // console.log('error')
        fs.appendFile('errors.txt', `\n${today()} error happened in copy files for campaing ${camp}: ${err}`, function (err) {
            if (err) throw err;
            console.log('Saved!');
        });
        if (err) throw err;
    };
    console.log('File was moved to destination');
});