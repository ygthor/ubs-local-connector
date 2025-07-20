<?php

class Converter
{
    static function ubsTable()
    {
        $dbf_arr = [
            // 'ubs_ubsacc2015_arcust',
            // 'ubs_ubsacc2015_apvend',
            // 'ubs_ubsacc2015_arpay',// => receipt
            // 'ubs_ubsacc2015_arpost',// => order
            // 'ubs_ubsacc2015_artran',
            // 'ubs_ubsacc2015_gldata',
            // 'ubs_ubsacc2015_glbatch',
            // 'ubs_ubsacc2015_glpost',
            // 'ubs_ubsacc2015_ictran',


            // 'ubs_ubsstk2015_arcust',
            // 'ubs_ubsstk2015_apvend',
            // 'ubs_ubsstk2015_icarea',
            // 'ubs_ubsstk2015_icitem',
            // 'ubs_ubsstk2015_ictran',

            'ubs_ubsstk2015_arpso', // order
            'ubs_ubsstk2015_icpso', // order item
            // 'ubs_ubsstk2015_artran', // invoice
            // 'ubs_ubsstk2015_ictran', // invoice item
        ];
        return $dbf_arr;
    }


    static function primaryKey($entity)
    {
        $maps = [
            'ubs_ubsacc2015_arcust' => 'CUSTNO',
            'ubs_ubsacc2015_arpay' => 'CUSTNO',
            'ubs_ubsacc2015_arpost' => 'ENTRY',


            'ubs_ubsstk2015_arpso' => 'REFNO',
            'ubs_ubsstk2015_icpso' => [
                'REFNO',
                'ITEMCOUNT'
            ],

            


            'customers' => 'customer_code',
            'orders' => 'reference_no',
            'order_items' => 'unique_key',
            
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
    static function table_map($entity)
    {
        $maps = [
            'ubs_ubsacc2015_arcust' => 'customers',
            'ubs_ubsstk2015_arpso' => 'orders',
            'ubs_ubsstk2015_icpso' => 'order_items',
        ];

        return $maps[$entity] ?? null;
    }

    static function mapColumns($entity)
    {
        $maps = [
            'customers' => [
                // UBS => REMOTE
                'CUSTNO'     => 'customer_code',
                'NAME'       => 'name',
                'NAME2'      => 'company_name',
                'ADD1'       => 'address', // overwritten from 'address1'
                'ADD2'       => 'address2',
                'POSTCODE'   => 'postcode',
                'STATE'      => 'state',
                'AREA'       => 'territory',
                'PHONE'      => 'phone', // overwritten from 'telephone1'
                'PHONEA'     => 'telephone2',
                'FAX'        => 'fax_no',
                'CONTACT'    => 'contact_person',
                'E_MAIL'     => 'email',
                'CT_GROUP'   => 'customer_group',
                'CUST_TYPE'  => 'customer_type',
                'BUSINESS'   => 'segment',
                'TERM'       => 'payment_type',
                'TERM_IN_M'  => 'payment_term',
                'PROV_DISC'  => 'max_discount',
                'TEMP'       => 'lot_type',
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
                'CREATED_ON' => 'created_at',
                'PLA_DODATE' => 'order_date',
                'UPDATED_ON' => 'updated_at',

            ],

            'order_items' => [
                // 'TYPE' => 'MASTER',
                // 'TRANCODE' => 'MASTER',
                // 'CUSTNO' => 'MASTER',
                // 'DATE' => 'MASTER',
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
                // 'NAME ' => 'MASTER',        
                // 'NAME ' => 'MASTER', 
                'TRDATETIME' => 'created_at',
                'CREATED_ON' => 'created_at',
                'UPDATED_ON' => 'updated_at',

            ],





        ];

        return $maps[$entity] ?? [];
    }
}
