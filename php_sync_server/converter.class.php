<?php

class Converter
{
    static function ubsTable(){
        $dbf_arr = [
            'ubs_ubsacc2015_arcust',
            // 'ubs_ubsacc2015_apvend',
            // 'ubs_ubsacc2015_arpay',
            // 'ubs_ubsacc2015_arpost',
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
        ];
        return $dbf_arr;
    }


    static function primaryKey($entity){
        $maps = [
            'ubs_ubsacc2015_arcust' => 'CUSTNO',
            'customers' => 'customer_code',
        ];

        return $maps[$entity] ?? null;
    }
    static function columnMap($entity){
        $common = [
            'UPDATED_ON' => 'updated_at',
            'customers' => 'customer_code',
        ];

        return $maps[$entity] ?? null;
    }
    static function table_map($entity){
         $maps = [
            'ubs_ubsacc2015_arcust' => 'customers',
        ];

        return $maps[$entity] ?? null;
    }

    static function mapColumns($entity)
    {
        $maps = [
            'customers' => [
                // remote => ubs
                'customer_code'   => 'CUSTNO',
                'name'            => 'NAME',
                'company_name'    => 'NAME2',
                'address1'        => 'ADD1',
                'address2'        => 'ADD2',
                'postcode'        => 'POSTCODE',
                'state'           => 'STATE',
                'territory'       => 'AREA',
                'telephone1'      => 'PHONE',
                'telephone2'      => 'PHONEA',
                'fax_no'          => 'FAX',
                'address'         => 'ADD1',
                'contact_person'  => 'CONTACT',
                'email'           => 'E_MAIL',
                'phone'           => 'PHONE',
                'customer_group'  => 'CT_GROUP',
                'customer_type'   => 'CUST_TYPE',
                'segment'         => 'BUSINESS',
                'payment_type'    => 'TERM',
                'payment_term'    => 'TERM_IN_M',
                'max_discount'    => 'PROV_DISC',
                'lot_type'        => 'TEMP',
                'avatar_url'      => null,
                'created_at'      => 'CREATED_ON',
                'updated_at'      => 'UPDATED_ON',
            ],



            /*
                $dbf_arr = [
                    'ubs_ubsacc2015_arcust',
                    'apvend',
                    'artran',
                    'icarea',
                    'icitem',
                    'ictran',
                    'arpay',
                    'arpost',
                    'gldata',
                    'glbatch',
                    'glpost',
                ];
            */
            // Add more: 'product' => [...], 'invoice' => [...]
        ];

        return $maps[$entity] ?? [];
    }
}
