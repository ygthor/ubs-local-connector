const { createLocalConnection } = require('../config/database');

class LocalDataReader {
    constructor() {
        this.connection = null;
    }

    async connect() {
        this.connection = await createLocalConnection();
    }

    async disconnect() {
        if (this.connection) {
            await this.connection.end();
        }
    }

    // Get all UBS tables
    async getUbsTables() {
        const [tables] = await this.connection.execute(
            `SHOW TABLES WHERE Tables_in_${process.env.LOCAL_DB_NAME} LIKE '${process.env.TABLE_PREFIX}%'`
        );
        return tables.map(row => Object.values(row)[0]);
    }

    // Get records modified since last sync
    async getModifiedRecords(tableName, lastSyncTime, idField, lastModifiedField) {
        let sql = `SELECT * FROM \`${tableName}\``;
        let params = [];

        if (lastSyncTime && lastModifiedField) {
            sql += ` WHERE \`${lastModifiedField}\` > ?`;
            params.push(lastSyncTime);
        }

        sql += ` ORDER BY \`${idField}\``;

        const [rows] = await this.connection.execute(sql, params);
        return rows;
    }

    // Get all records from table
    async getAllRecords(tableName) {
        const [rows] = await this.connection.execute(`SELECT * FROM \`${tableName}\``);
        return rows;
    }

    // Get specific records by IDs
    async getRecordsByIds(tableName, idField, ids) {
        if (ids.length === 0) return [];

        const placeholders = ids.map(() => '?').join(',');
        const sql = `SELECT * FROM \`${tableName}\` WHERE \`${idField}\` IN (${placeholders})`;
        
        const [rows] = await this.connection.execute(sql, ids);
        return rows;
    }
}

module.exports = LocalDataReader;