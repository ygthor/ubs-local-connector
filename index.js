import { readDBF, syncToServer, testServerResponse } from './functions.js';
import dotenv from 'dotenv';
dotenv.config();


async function main() {
    // testServerResponse();
    syncAll();
    // singleSync()

}

async function syncAll(){
    let directory_arr = [
        'UBSACC2015',
        'UBSSTK2015'
    ];
    let dbf_arr = [
        // Customer
        'arcust',
        'icarea',

        // Supplier
        'apvend',
        'icarea',
        
        // Item
        'Icitem',

        // Transaction
        'ictran',
        'artran',

        // Payment
        'arpay',
        'arpost',
        'gldata',
        'glbatch',
        'glpost',
    ];

    for(let i = 0; i < directory_arr.length; i++) {
        let directory_name = directory_arr[i];
        let directory_path = 'C:/' + directory_name + '/DATA/';
        for(let j = 0; j < dbf_arr.length; j++) {
            let file_name = dbf_arr[j] + '.dbf';
            try {
                var data = await readDBF(directory_path + '/' + file_name);
                syncToServer({
                    directory: directory_name,
                    filename: file_name,
                    data: data
                });
            } catch (error) {
                console.error(`Error processing ${file_name}:`, error);
            }
        }
    }

}


// for Test purpose
async function singleSync() {  
    let directory_name = 'UBSACC2015';
    let directory_path = 'C:/' + directory_name + '/DATA/';
    let file_name = 'userpin.dbf';
    var data = await readDBF(directory_path + '/' + file_name);
    syncToServer({
        directory: directory_name,
        filename: 'userpin.dbf',
        data: data
    });
}


main();


