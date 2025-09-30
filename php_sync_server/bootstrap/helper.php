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

if (!function_exists('isCli')) {
    function isCli()
    {
        return php_sapi_name() === 'cli' || (defined('STDIN') && STDIN !== null);
    }
}

if (!function_exists('dump')) {

    function dump($v = 'RANDOM_STR')
    {
        if (isCli()) {
            // CMD/CLI display pattern - compact format
            
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
            echo "\n";
        } else {
            // Web display pattern
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
}

if (!function_exists('dd')) {
    function dd($v = 'RANDOM_STR')
    {
        if (isCli()) {
            // CMD/CLI display pattern - compact format
            echo "\n[DD] ";
            
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
                    // $v = convertBooleansToStrings(get_object_vars($v));
                    
                    // JSON encode the object
                    $v = json_encode($v, JSON_PRETTY_PRINT);
                    $v = strip_tags($v);
                    print_r($v);
                } else {
                    print_r($v);
                }
            }
            echo "\n[EXIT]\n";
            exit;
        } else {
            // Web display pattern
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
                    // $v = convertBooleansToStrings(get_object_vars($v));
                    
                    // JSON encode the object
                    $v = json_encode($v, JSON_PRETTY_PRINT);
                    $v = strip_tags($v);
                    print_r($v);
                } else {
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
}

function timestamp(){
    return date('Y-m-d H:i:s');
}



/**
 * Validates and fixes UPDATED_ON field values
 * @param array $data Array of records to validate
 * @return array Array with validated UPDATED_ON fields
 */
function validateAndFixUpdatedOn($data) {
    $currentDate = date('Y-m-d H:i:s');
    
    foreach ($data as &$record) {
        if (isset($record['UPDATED_ON'])) {
            $updatedOn = $record['UPDATED_ON'];
            
            // Check if UPDATED_ON is invalid
            if (empty($updatedOn) || 
                $updatedOn === '0000-00-00' || 
                $updatedOn === '0000-00-00 00:00:00' ||
                strtotime($updatedOn) === false ||
                $updatedOn === null) {
                
                ProgressDisplay::info("Invalid UPDATED_ON detected: '$updatedOn' - Converting to current date: $currentDate");
                $record['UPDATED_ON'] = $currentDate;
            }
        }
    }
    
    return $data;
}
