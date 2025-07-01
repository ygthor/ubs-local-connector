// Define how local UBS tables map to remote API endpoints
const tableMappings = {
    // Local table name -> Remote API endpoint & transformation
    'ubs_customers_main': {
        endpoint: '/customers',
        method: 'POST',
        idField: 'customer_id',
        lastModifiedField: 'updated_at',
        transformer: 'customer-mapping'
    },
    'ubs_products_inventory': {
        endpoint: '/products',
        method: 'POST',
        idField: 'product_code',
        lastModifiedField: 'last_updated',
        transformer: 'product-mapping'
    },
    'ubs_transactions_daily': {
        endpoint: '/transactions',
        method: 'POST',
        idField: 'transaction_id',
        lastModifiedField: 'created_at',
        transformer: 'transaction-mapping'
    }
};

// Define field mappings for each transformer
const fieldMappings = {
    'customer-mapping': {
        // local_field: remote_field
        'cust_id': 'customer_id',
        'cust_name': 'full_name',
        'cust_addr1': 'address_line_1',
        'cust_addr2': 'address_line_2',
        'cust_city': 'city',
        'cust_phone': 'phone_number',
        'cust_email': 'email',
        'cust_status': 'status',
        'created_date': 'created_at'
    },
    'product-mapping': {
        'prod_code': 'product_code',
        'prod_name': 'product_name',
        'prod_desc': 'description',
        'prod_price': 'unit_price',
        'prod_qty': 'quantity_in_stock',
        'prod_category': 'category_id',
        'prod_status': 'status'
    },
    'transaction-mapping': {
        'trans_id': 'transaction_id',
        'trans_date': 'transaction_date',
        'cust_id': 'customer_id',
        'total_amount': 'total_amount',
        'trans_type': 'transaction_type',
        'payment_method': 'payment_method',
        'trans_status': 'status'
    }
};

module.exports = {
    tableMappings,
    fieldMappings
};