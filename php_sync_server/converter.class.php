<?php

class Converter
{
    function map($entity)
    {
        $maps = [
            'customer' => [
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
            // Add more: 'product' => [...], 'invoice' => [...]
        ];

        return $maps[$entity] ?? [];
    }
}
