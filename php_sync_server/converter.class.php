<?php

class Converter
{
    static function ubsTable()
    {
        $dbf_arr = [
            
            'ubs_ubsacc2015_gldata', // statement

            // 'ubs_ubsacc2015_apvend',
            // 'ubs_ubsacc2015_arpay',// => receipt
            // 'ubs_ubsacc2015_arpost',// => order
            // 'ubs_ubsacc2015_artran',
            
            // 'ubs_ubsacc2015_glbatch',
            // 'ubs_ubsacc2015_glpost',
            // 'ubs_ubsacc2015_ictran',


            // 'ubs_ubsstk2015_arcust',
            // 'ubs_ubsstk2015_apvend',
            // 'ubs_ubsstk2015_icarea',
            'ubs_ubsstk2015_icitem', // product/item
            'ubs_ubsstk2015_icgroup', // product group - synced from icgroup.dbf
            // 'ubs_ubsstk2015_ictran',
            'ubs_ubsacc2015_arcust',
            'ubs_ubsstk2015_artran', // invoice
            'ubs_ubsstk2015_ictran', // invoice item

            
        ];
        return $dbf_arr;
    }


    static function primaryKey($entity)
    {
        $maps = [
            'ubs_ubsacc2015_arcust' => 'CUSTNO',
            'ubs_ubsacc2015_arpay' => 'CUSTNO',
            'ubs_ubsacc2015_arpost' => 'ENTRY',

            'ubs_ubsacc2015_gldata' => 'ACCNO',

            'ubs_ubsstk2015_artran' => 'REFNO',
            'ubs_ubsstk2015_ictran' => [
                'REFNO',
                'ITEMCOUNT'
            ],

            'ubs_ubsstk2015_icitem' => 'ITEMNO',
            'ubs_ubsstk2015_icgroup' => 'GROUP',  // DBF field name, not remote field name


            'customers' => 'customer_code',
            'orders' => 'reference_no',
            'order_items' => 'unique_key',
            // 'artrans' => 'REFNO',
            // 'artrans_items' => 'unique_key',
            'gldata' => 'ACCNO',
            'icitem' => 'ITEMNO',
            'icgroup' => 'name',
            
        ];

        return $maps[$entity] ?? null;
    }
    static function columnMap($entity)
    {
        $common = [
            'UPDATED_ON' => 'updated_at',
            'customers' => 'customer_code',
        ];

        return $maps[$entity] ?? null;
    }
    static function table_map()
    {
        $maps = [
            'ubs_ubsacc2015_arcust' => 'customers',

            'ubs_ubsstk2015_artran' => 'orders',
            'ubs_ubsstk2015_ictran' => 'order_items',
            'ubs_ubsacc2015_gldata' => 'gldata',
            'ubs_ubsstk2015_icitem' => 'icitem',
            'ubs_ubsstk2015_icgroup' => 'icgroup',
        ];

        return $maps;
    }
    static function table_convert_remote($entity)
    {
        $maps = self::table_map();
        return $maps[$entity] ?? null;
    }

    static function mapColumns($entity)
    {
        $maps = [
            'customers' => [
                // UBS => REMOTE
                'CUSTNO'     => 'customer_code',
                'NAME'       => 'company_name',
                'NAME2'      => 'company_name2',
                'ADD1'       => 'address1', // overwritten from 'address1'
                'ADD2'       => 'address2',
                'ADD3'       => 'address3',
                'ADD4'       => 'address4', // and extract to postcode , state
                // 'POSTCODE'   => 'postcode',
                // 'STATE'      => 'state',
                'AREA'       => 'territory',
                'PHONE'      => 'telephone1', // overwritten from 'telephone1'
                'PHONEA'     => 'telephone2',
                'FAX'        => 'fax_no',
                'CONTACT'    => 'contact_person',
                'E_MAIL'     => 'email',
                'CT_GROUP'   => 'customer_group',
                'REGION'  => 'customer_type',
                'BUSINESS'   => 'segment',
                'TERM'       => 'payment_type',
                'TERM_IN_M'  => 'payment_term',
                'PROV_DISC'  => 'max_discount',
                'TEMP'       => 'lot_type',
                'AGENT'      => 'agent_no',
                'CREATED_ON' => 'created_at',
                'UPDATED_ON' => 'updated_at',
            ],

            'orders' => [
                'TYPE' => 'type',
                'REFNO'       => 'reference_no', // key was empty
                'CUSTNO'      => 'customer_code', // link using customer id
                'NAME' => 'customer_name',
                'DATE'        => 'order_date',
                'DESP'        => 'description',
                'AGENNO'      => 'agent_no', // Agent number (user's name)
                'GROSS_BIL'   => 'gross_amount',
                'TAX1_BIL'    => 'tax1',
                // 'TAX2_BIL'    => 'tax2',
                // 'TAX3_BIL'    => 'tax3',
                'TAX1'    => 'tax1',
                // 'TAX2'    => 'tax2',
                // 'TAX3'    => 'tax3',
                'TAXP1'       => 'tax1_percentage',
                // 'TAXP2'       => 'tax2_percentage',
                // 'TAXP3'       => 'tax3_percentage',
                'TAX'         => 'tax1',
                'GRAND_BIL'   => 'grand_amount',
                'GRAND'   => 'grand_amount',
                'INVGROSS'    => 'net_amount',
                'DISCOUNT'    => 'discount',
                'NET'         => 'net_amount',
                // Note: DEBITAMT and CREDITAMT are UBS-only fields, not synced to remote orders table
                'CREATED_ON' => 'created_at',
                'PLA_DODATE' => 'order_date',
                'UPDATED_ON' => 'updated_at',

            ],

            'order_items' => [
                'TYPE' => 'orders|type',
                'TRANCODE' => 'item_count',
                'CUSTNO' => 'orders|customer_code',
                'DATE' => 'orders|order_date',
                'AGENNO' => 'orders|agent_no',
                // 'NAME ' => 'orders|customer_name',   

                'REFNO'       => 'reference_no', 
                'ITEMCOUNT' => 'item_count',
                'ITEMNO' => 'product_no',
                'DESP' => 'description',
                'QTY_BIL' => 'quantity',
                'PRICE_BIL' => 'unit_price',
                'UNIT_BIL' => 'unit',
                'AMT1_BIL' => 'amount',
                'AMT_BIL' => 'amount',
                'QTY' => 'quantity',
                'PRICE' => 'unit_price',
                'UNIT' => 'unit',
                'AMT1' => 'amount',
                'AMT' => 'amount',
                'QTY1' => 'quantity',
                     
                'TRDATETIME' => 'created_at',
                'CREATED_ON' => 'created_at',
                'UPDATED_ON' => 'updated_at',

            ],

            'icgroup' => [
                // UBS DBF => REMOTE TABLE
                'GROUP' => 'name',           // Product group name
                'DESP' => 'description',      // Product group description
                'SALEC' => null,              // Skip SALEC (not in remote table)
                'CREATED_ON' => 'CREATED_ON',
                'UPDATED_ON' => 'UPDATED_ON',
            ],

            'icitem' => [
                // Ensure all fields from Icitem model are explicitly mapped
                // This makes sure all fields including UNIT, QTY, TYPE, etc. are synced
                'ITEMNO' => 'ITEMNO',
                'TYPE' => 'TYPE',
                'CATEGORY' => 'CATEGORY',
                'GROUP' => 'GROUP',
                'DESP' => 'DESP',
                'UNIT' => 'UNIT',           // Critical: Unit of measurement
                'UCOST' => 'UCOST',
                'PRICE' => 'PRICE',
                'PRICE_BIL' => 'PRICE_BIL',
                'UNIT2' => 'UNIT2',
                'QTYBF' => 'QTYBF',
                'FACTOR1' => 'FACTOR1',
                'FACTOR2' => 'FACTOR2',
                'PRICEU2' => 'PRICEU2',
                'PRICEU3' => 'PRICEU3',
                'T_UCOST' => 'T_UCOST',
                'QTY' => 'QTY',             // Critical: Stock quantity
                'COST' => 'COST',
                'CREATED_BY' => 'CREATED_BY',
                'CREATED_ON' => 'CREATED_ON',
                'UPDATED_BY' => 'UPDATED_BY',
                'UPDATED_ON' => 'UPDATED_ON',
            ],

            // auto map if identical

        ];

        return $maps[$entity] ?? [];
    }



    static function mapCreatedAtField($remote_table){
        $maps = [
            'orders' => 'created_at',
            'order_items' => 'created_at',
            'gldata' => 'CREATED_ON',
            'icitem' => 'CREATED_ON',
            'icgroup' => 'CREATED_ON',
        ];

        return $maps[$remote_table] ?? null; // default
    }
    static function mapUpdatedAtField($remote_table){
        $maps = [
            'gldata' => 'UPDATED_ON',
            'icitem' => 'UPDATED_ON',
            'icgroup' => 'UPDATED_ON',
        ];

        return $maps[$remote_table] ?? 'updated_at'; // default to 'updated_at'
    }

    /**
     * Transform data from Local (UBS) to Remote format
     * Handles special cases: Extract postcode and state from ADD4
     * POSTCODE and STATE are NOT synced - only ADD4 is synced
     */
    static function transformToRemote($entity, $localData)
    {
        if ($entity !== 'customers') {
            return $localData; // No special handling for other entities
        }

        $transformedData = $localData;

        // Extract postcode and state from ADD4 (ADD4 contains "postcode state")
        if (isset($localData['ADD4']) && !empty($localData['ADD4'])) {
            $parts = self::splitPostcodeState($localData['ADD4']);
            $transformedData['postcode'] = $parts['postcode'];
            $transformedData['state'] = $parts['state'];
        }

        return $transformedData;
    }

    /**
     * Transform data from Remote to Local (UBS) format
     * Handles special cases: Set ADD4 from address4
     * POSTCODE and STATE are NOT synced - only ADD4 is synced
     */
    static function transformToLocal($entity, $remoteData)
    {
        if ($entity !== 'customers') {
            return $remoteData; // No special handling for other entities
        }

        $transformedData = $remoteData;

        // ADD4 is already mapped from address4 in the mapping
        // No additional transformation needed since POSTCODE and STATE are not synced

        return $transformedData;
    }

    /**
     * Split combined POSTCODE field into separate postcode and state
     * Examples: "81100 JHR" => ['postcode' => '81100', 'state' => 'JHR']
     *           "81100" => ['postcode' => '81100', 'state' => '']
     */
    static function splitPostcodeState($combined)
    {
        if (empty($combined)) {
            return ['postcode' => '', 'state' => ''];
        }

        // Trim and split by space
        $combined = trim($combined);
        $parts = preg_split('/\s+/', $combined, 2);

        return [
            'postcode' => $parts[0] ?? '',
            'state' => $parts[1] ?? ''
        ];
    }

    /**
     * Combine separate postcode and state into single POSTCODE field
     * Examples: ('81100', 'JHR') => "81100 JHR"
     *           ('81100', '') => "81100"
     */
    static function combinePostcodeState($postcode, $state)
    {
        $postcode = trim($postcode ?? '');
        $state = trim($state ?? '');

        if (empty($state)) {
            return $postcode;
        }

        return $postcode . ' ' . $state;
    }
}
