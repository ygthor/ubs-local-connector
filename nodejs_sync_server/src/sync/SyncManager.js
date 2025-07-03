const LocalDataReader = require('../readers/LocalDataReader');
const DataMapper = require('../transformers/DataMapper');
const RemoteApiSync = require('./RemoteApiSync');
const { tableMappings } = require('../config/mapping-config');
const fs = require('fs').promises;

class SyncManager {
    constructor() {
        this.reader = new LocalDataReader();
        this.mapper = new DataMapper();
        this.apiSync = new RemoteApiSync();
        this.lastSyncFile = process.env.LAST_SYNC_TIMESTAMP_FILE || './last-sync.json';
        this.dryRun = process.env.DRY_RUN === 'true';
    }

    // Load last sync timestamps
    async loadLastSyncTimes() {
        try {
            const data = await fs.readFile(this.lastSyncFile, 'utf8');
            return JSON.parse(data);
        } catch (error) {
            return {}; // First run
        }
    }

    // Save last sync timestamps
    async saveLastSyncTimes(syncTimes) {
        await fs.writeFile(this.lastSyncFile, JSON.stringify(syncTimes, null, 2));
    }

    // Sync single table
    async syncTable(tableName) {
        const mapping = tableMappings[tableName];
        if (!mapping) {
            console.log(`No mapping found for table: ${tableName}`);
            return { skipped: true, reason: 'No mapping configured' };
        }

        console.log(`\n=== Syncing ${tableName} ===`);
        
        try {
            // Get last sync time for this table
            const lastSyncTimes = await this.loadLastSyncTimes();
            const lastSync = lastSyncTimes[tableName];
            
            // Read modified records
            const records = await this.reader.getModifiedRecords(
                tableName,
                lastSync,
                mapping.idField,
                mapping.lastModifiedField
            );

            if (records.length === 0) {
                console.log(`No new records to sync for ${tableName}`);
                return { processed: 0, success: true };
            }

            console.log(`Found ${records.length} records to sync`);

            // Transform records
            const transformedRecords = this.mapper.transformRecords(records, mapping.transformer);
            
            if (this.dryRun) {
                console.log('DRY RUN - Would sync:', transformedRecords.slice(0, 3));
                return { processed: records.length, success: true, dryRun: true };
            }

            // Send to remote API
            console.log(mapping.endpoint)
            const results = await this.apiSync.postBatch(mapping.endpoint, transformedRecords);
            
            // Count successes
            const successful = results.filter(r => r.success).length;
            const failed = results.length - successful;

            // Update last sync time
            lastSyncTimes[tableName] = new Date().toISOString();
            await this.saveLastSyncTimes(lastSyncTimes);

            console.log(`Sync completed: ${successful} success, ${failed} failed`);

            return {
                processed: records.length,
                successful,
                failed,
                success: failed === 0,
                results
            };

        } catch (error) {
            console.error(`Error syncing ${tableName}:`, error);
            return {
                success: false,
                error: error.message
            };
        }
    }

    // Sync all configured tables
    async syncAll() {
        console.log('=== Starting Data Sync ===');
        
        const syncResults = {};
        
        try {
            await this.reader.connect();
            
            // Get available tables
            const availableTables = await this.reader.getUbsTables();
            console.log('Available tables:', availableTables);
            
            // Sync each configured table
            for (const [tableName, mapping] of Object.entries(tableMappings)) {
                if (!availableTables.includes(tableName)) {
                    console.log(`Table ${tableName} not found in database`);
                    syncResults[tableName] = { skipped: true, reason: 'Table not found' };
                    continue;
                }
                
                syncResults[tableName] = await this.syncTable(tableName);
            }
            
            return {
                success: true,
                tables: syncResults,
                timestamp: new Date().toISOString()
            };
            
        } catch (error) {
            console.error('Sync process failed:', error);
            return {
                success: false,
                error: error.message,
                timestamp: new Date().toISOString()
            };
        } finally {
            await this.reader.disconnect();
        }
    }
}

module.exports = SyncManager;