// functions.js
import { DBFFile } from 'dbffile';
import axios from 'axios';

export async function readDBF(dbfFilePath) {
    try {
        const dbf = await DBFFile.open(dbfFilePath);
            
        const fields = dbf.fields.map(field => ({
            name: field.name,
            type: field.type,
            size: field.size,
            decs: field.decimals
        }));

        const data = [];
        for await (const record of dbf) {
            data.push(record);
        }

        return {
            structure: fields,
            rows: data
        };
    } catch (error) {
        console.error('Error reading DBF file:', error);
        throw error;
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



