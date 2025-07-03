<?php
if (!function_exists('url')) {
    function url($val = '')
    {
        $https = isset($_SERVER['HTTPS']) && $_SERVER['HTTPS'] === 'on' ? "https" : "http";
        $server_url = !isset($_SERVER['HTTP_HOST']) ? ENV::APP_URL : $https . "://" . $_SERVER['HTTP_HOST'];
        if (!empty($val)) {
            $server_url .= $val;
        }
        return $server_url;
    }
}

if (!function_exists('dump')) {

    function dump($v = 'RANDOM_STR')
    {
        echo "<pre style='background:#263238;color:white;padding:10px;margin:20px 0px'>";
        if ($v === null) {
            echo 'null';
        } elseif ($v === 'RANDOM_STR') {
            echo randstr();
        } elseif ($v === true) {
            echo 'true';
        } elseif ($v === false) {
            echo 'false';
        } else {
            if (is_array($v)) {
                $v = (json_encode($v, JSON_PRETTY_PRINT));
                $v = strip_tags($v);
                print_r($v);
            } else {
                print_r($v);
            }
        }
        echo "</pre>";
    }
}

if (!function_exists('dd')) {
    function dd($v = 'RANDOM_STR')
    {
        echo "<pre style='background:#000000;color:white;padding:10px;margin:20px 0px'>";
        if ($v === null) {
            echo 'null';
        } elseif ($v === 'RANDOM_STR') {
            echo randstr();
        } elseif ($v === true) {
            echo 'true';
        } elseif ($v === false) {
            echo 'false';
        } else {
            if (is_array($v)) {
                $v = (json_encode($v, JSON_PRETTY_PRINT));
                $v = strip_tags($v);
                print_r($v);
            } elseif (is_object($v)) {
                // Convert boolean properties to strings in the object
                $v = convertBooleansToStrings(get_object_vars($v));
                
                // JSON encode the object
                $v = json_encode($v, JSON_PRETTY_PRINT);
                $v = strip_tags($v);
                print_r($v);
            }else {
                print_r($v);
            }
        }
        echo "</pre>";
        echo "<hr>";
        echo "EXIT";
        echo "<hr>";
        exit;
    }
}

function timestamp(){
    return date('Y-m-d H:i:s');
}


function insertSyncLog(){
    $db = new mysql();
    $db->insert('sync_logs',[
        'synced_at' => timestamp(),
    ]);
}
function lastSyncAt(){
    $db = new mysql();
    $data = $db->first('SELECT * FROM sync_logs ORDER BY synced_at DESC LIMIT 1');
    return $data ? $data['synced_at'] : null;
}