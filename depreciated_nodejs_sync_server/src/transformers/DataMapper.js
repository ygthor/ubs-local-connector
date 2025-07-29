const { fieldMappings } = require('../config/mapping-config');
const moment = require('moment');

class DataMapper {
    constructor() {
        this.mappings = fieldMappings;
    }

    // Transform single record based on mapping
    transformRecord(record, mappingName) {
        const mapping = this.mappings[mappingName];
        if (!mapping) {
            throw new Error(`Mapping '${mappingName}' not found`);
        }

        const transformed = {};

        // Apply field mappings
        for (const [localField, remoteField] of Object.entries(mapping)) {
            if (record.hasOwnProperty(localField)) {
                transformed[remoteField] = this.transformValue(
                    record[localField], 
                    localField, 
                    remoteField,
                    mappingName
                );
            }
        }

        // Apply custom transformations
        return this.applyCustomTransformations(transformed, record, mappingName);
    }

    // Transform value based on field type and requirements
    transformValue(value, localField, remoteField, mappingName) {
        // Handle null/undefined
        if (value === null || value === undefined) {
            return null;
        }

        // Date transformations
        if (localField.includes('date') || localField.includes('time')) {
            return this.transformDate(value);
        }

        // Number transformations
        if (typeof value === 'number') {
            return this.transformNumber(value, localField);
        }

        // String transformations
        if (typeof value === 'string') {
            return this.transformString(value, localField);
        }

        return value;
    }

    // Date transformation
    transformDate(dateValue) {
        if (!dateValue) return null;
        
        // Handle different date formats from DBF
        if (typeof dateValue === 'string') {
            // Try to parse various formats
            const formats = ['YYYY-MM-DD', 'YYYYMMDD', 'MM/DD/YYYY', 'DD/MM/YYYY'];
            for (const format of formats) {
                const parsed = moment(dateValue, format, true);
                if (parsed.isValid()) {
                    return parsed.format('YYYY-MM-DD HH:mm:ss');
                }
            }
        }
        
        return moment(dateValue).format('YYYY-MM-DD HH:mm:ss');
    }

    // Number transformation
    transformNumber(numValue, fieldName) {
        // Handle decimal precision for currency fields
        if (fieldName.includes('price') || fieldName.includes('amount')) {
            return parseFloat(numValue).toFixed(2);
        }
        
        // Handle integer fields
        if (fieldName.includes('qty') || fieldName.includes('count')) {
            return parseInt(numValue);
        }
        
        return numValue;
    }

    // String transformation
    transformString(strValue, fieldName) {
        // Trim whitespace (common in DBF files)
        let cleaned = strValue.trim();
        
        // Handle specific field transformations
        if (fieldName.includes('email')) {
            cleaned = cleaned.toLowerCase();
        }
        
        if (fieldName.includes('phone')) {
            // Remove non-numeric characters except +
            cleaned = cleaned.replace(/[^\d+]/g, '');
        }
        
        return cleaned || null;
    }

    // Apply custom business logic transformations
    applyCustomTransformations(transformed, originalRecord, mappingName) {
        switch (mappingName) {
            case 'customer-mapping':
                return this.transformCustomer(transformed, originalRecord);
            case 'product-mapping':
                return this.transformProduct(transformed, originalRecord);
            case 'transaction-mapping':
                return this.transformTransaction(transformed, originalRecord);
            default:
                return transformed;
        }
    }

    // Customer-specific transformations
    transformCustomer(transformed, original) {
        // Combine address fields if needed
        if (original.cust_addr1 && original.cust_addr2) {
            transformed.full_address = `${original.cust_addr1}, ${original.cust_addr2}`;
        }

        // Set default status if missing
        if (!transformed.status) {
            transformed.status = 'active';
        }

        // Add computed fields
        transformed.sync_source = 'ubs_system';
        transformed.last_sync_at = new Date().toISOString();

        return transformed;
    }

    // Product-specific transformations
    transformProduct(transformed, original) {
        // Calculate stock status
        if (original.prod_qty !== undefined) {
            transformed.stock_status = original.prod_qty > 0 ? 'in_stock' : 'out_of_stock';
        }

        // Format price
        if (transformed.unit_price) {
            transformed.unit_price = parseFloat(transformed.unit_price);
        }

        return transformed;
    }

    // Transaction-specific transformations
    transformTransaction(transformed, original) {
        // Format transaction date
        if (transformed.transaction_date) {
            transformed.transaction_date = this.transformDate(transformed.transaction_date);
        }

        // Calculate derived fields
        if (original.total_amount) {
            transformed.total_amount = parseFloat(original.total_amount);
            transformed.tax_amount = transformed.total_amount * 0.1; // Assuming 10% tax
        }

        return transformed;
    }

    // Transform array of records
    transformRecords(records, mappingName) {
        return records.map(record => this.transformRecord(record, mappingName));
    }
}

module.exports = DataMapper;