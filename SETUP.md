Explanation
[python_sync_local] 
is for sync UBS dbf to local mysql, for quick processing and comparison purpose


[php_sync_server] is for 
use ubs mysql data from [python_sync_local] compare with web server mysql data
check based on Last  Modified Date and the item id to determine
- insert / update on remote / ubs
-------

<?php
# Remember Create ENV file
abstract class ENV
{
    const DB_HOST = '127.0.0.1';
    const DB_PORT = '3306';
    const DB_USERNAME = 'root';
    const DB_PASSWORD = '';
    const DB_NAME = 'ubs_data';

    # SERVER, need enable remote-sql
    const REMOTE_DB_HOST = '127.0.0.1';
    const REMOTE_DB_PORT = '3306';
    const REMOTE_DB_USERNAME = 'root';
    const REMOTE_DB_PASSWORD = '';
    const REMOTE_DB_NAME = 'kanesan';


    const API_URL = "http://127.0.0.1:8000";
}



Windows Task Scheduler 

[python_sync_local] 
execute python_sync_local\main.py EVERY 1 minutes

[php_sync_server] 
execute php_sync_server\main.php EVERY 5 minutes