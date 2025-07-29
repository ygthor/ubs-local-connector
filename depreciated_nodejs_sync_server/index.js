require('dotenv').config();
const SyncManager = require('./src/sync/SyncManager');

async function main() {
    console.log('=== UBS Data Sync to Remote Server ===');
    
    const syncManager = new SyncManager();
    
    try {
        const result = await syncManager.syncAll();
        
        if (result.success) {
            console.log('\n=== Sync Summary ===');
            for (const [table, tableResult] of Object.entries(result.tables)) {
                if (tableResult.skipped) {
                    console.log(`⏭️  ${table}: ${tableResult.reason}`);
                } else if (tableResult.success) {
                    console.log(`✅ ${table}: ${tableResult.processed} processed`);
                } else {
                    console.log(`❌ ${table}: ${tableResult.error}`);
                }
            }
        } else {
            console.error('❌ Sync failed:', result.error);
        }
        
    } catch (error) {
        console.error('❌ Sync error:', error);
        process.exit(1);
    }
}

if (require.main === module) {
    main();
}