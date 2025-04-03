// functions.js
import { DBFFile } from 'dbffile';
import axios from 'axios';


export async function readDBF(dbfFilePath) {
    try {
        let dbf = await DBFFile.open(dbfFilePath);
        let data = [];
        for await (const record of dbf) {
            data.push(record);  // Push each record to the array
        }
        return data;  // Return the records as an array
    } catch (error) {
        console.error('Error reading DBF file:', error);
        throw error;  // Rethrow the error to be handled elsewhere
    }
}

export async function testServerResponse() {
    const url = process.env.SERVER_URL + '/api/test/response';
    try {
        const response = await axios.post(url);
        console.log('Response Data:', response.data);
    } catch (error) {
        console.error('Error fetching data:', error);
    }
}

export async function syncToServer({ filename, data, directory }) {
    const url = process.env.SERVER_URL + '/api/sync/local';

    try {
        const response = await axios.post(url,{
            directory:directory,
            filename: filename,
            data: data
        });
        console.log('Response Data:', response.data);
        
    } catch (error) {
        console.error('Error fetching data:', error);
    }
}



