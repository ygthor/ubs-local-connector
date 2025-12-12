<?php

class Core
{
    private static $instance;

    public $remote_customer_lists = [];
    public $remote_order_lists = [];

    private function __construct()
    {
        // Private constructor to prevent instantiation outside the class
    }

    public static function getInstance()
    {
        if (!isset(self::$instance)) {
            self::$instance = new self;
        }

        return self::$instance;
    }

    public function initRemoteData()
    {
        $db = new mysql;
        $db->connect_remote();

        $sql = "SELECT id, customer_code FROM customers";
        $data = $db->pluck($sql,'id','customer_code');
        $this->remote_customer_lists = $data;

        $sql = "SELECT id, reference_no FROM orders";
        $data = $db->pluck($sql,'id','reference_no');
        $this->remote_order_lists = $data;

        $sql = "SELECT id, receipt_no FROM receipts";
        $data = $db->pluck($sql,'id','receipt_no');
        $this->remote_order_lists = $data;
    }
}