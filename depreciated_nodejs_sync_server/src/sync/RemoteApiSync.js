const axios = require('axios');

class RemoteApiSync {
    constructor() {
        this.baseURL = process.env.REMOTE_API_BASE_URL;
        this.apiKey = process.env.REMOTE_API_KEY;
        this.token = process.env.REMOTE_API_TOKEN;
        this.batchSize = parseInt(process.env.BATCH_SIZE) || 100;
        
        // Configure axios instance
        this.client = axios.create({
            baseURL: this.baseURL,
            timeout: 30000,
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${this.token}`,
                'X-API-Key': this.apiKey
            }
        });
    }

    // Post single record
    async postRecord(endpoint, data) {
        try {
            const response = await this.client.post(endpoint, data);
            return {
                success: true,
                data: response.data,
                status: response.status
            };
        } catch (error) {
            return {
                success: false,
                error: error.response?.data || error.message,
                status: error.response?.status || 500
            };
        }
    }

    // Post batch of records
    async postBatch(endpoint, records) {
        const results = [];
        
        // Split into batches
        for (let i = 0; i < records.length; i += this.batchSize) {
            const batch = records.slice(i, i + this.batchSize);
            
            try {
                // Try batch endpoint first
                const batchEndpoint = `${endpoint}/batch`;
                const response = await this.client.post(batchEndpoint, { records: batch });
                
                results.push({
                    success: true,
                    processed: batch.length,
                    data: response.data
                });
                
            } catch (error) {
                // If batch fails, try individual records
                console.log(`Batch failed, trying individual records for ${endpoint}`);
                
                for (const record of batch) {
                    const result = await this.postRecord(endpoint, record);
                    results.push({
                        ...result,
                        record: record
                    });
                }
            }
        }
        
        return results;
    }

    // Update existing record
    async updateRecord(endpoint, id, data) {
        try {
            const response = await this.client.put(`${endpoint}/${id}`, data);
            return {
                success: true,
                data: response.data,
                status: response.status
            };
        } catch (error) {
            return {
                success: false,
                error: error.response?.data || error.message,
                status: error.response?.status || 500
            };
        }
    }

    // Check if record exists on remote
    async checkRecordExists(endpoint, id) {
        try {
            const response = await this.client.get(`${endpoint}/${id}`);
            return {
                exists: true,
                data: response.data
            };
        } catch (error) {
            if (error.response?.status === 404) {
                return { exists: false };
            }
            throw error;
        }
    }
}

module.exports = RemoteApiSync;