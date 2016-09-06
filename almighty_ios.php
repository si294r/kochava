<?php

function get_http_post($url, $body) {
    $context_options = array(
        'http' => array(
            'method' => 'POST',
            'header' => "Content-type: application/json\r\n"
            . "Content-Length: " . strlen($body) . "\r\n",
            'content' => $body
        )
    );
    $context = stream_context_create($context_options);
    $content = file_get_contents($url, false, $context);
    return $content;
}

include "/var/www/redshift-config2.php";
include "/var/www/kochava_almighty_ios_key.php";

if (isset($argv[1])) {
    $date = $argv[1];
} else {
    $date = date('Y-m-d', strtotime("-1 days"));
}
echo $date;

$time_start = strtotime($date);
$time_end = strtotime("+1 days", strtotime($date));

$url = 'https://reporting.api.kochava.com/v1.1/detail';
$body = '{
    "api_key": "'.$api_key.'",
    "app_guid": "'.$app_guid.'",
    "time_start": "'.$time_start.'",
    "time_end": "'.$time_end.'",
    "traffic": ["event"],
    "traffic_filtering": {
        "event_name": [
            "KOCHAVA INITIALIZATION",
            "BuyIAP_mightbolt5", 
            "BuyIAP_mightbolt4", 
            "BuyIAP_mightbolt3", 
            "BuyIAP_mightbolt2", 
            "BuyIAP_mightbolt1"
        ]
    },
    "currency":    "USD",
    "delivery_format": "csv"
}';
$content = get_http_post($url, $body);
echo $content; // {"status":"queued","report_token":"5210852189867446018"}
$json = json_decode($content);

$token = $json->report_token;

wait_progress:
sleep(60);
$url = 'https://reporting.api.kochava.com/v1.1/progress';
$body = '{
    "api_key": "'.$api_key.'",
    "app_guid": "'.$app_guid.'",
    "token": "'.$token.'"
}';

$content = get_http_post($url, $body);
echo $content;
//'{
//    "status":"completed",
//    "status_date":"2016-09-06 05:48:47",
//    "progress":"100",
//    "report":"http://kochava-reporting.s3.amazonaws.com/KRD5210852189867446018_3798567_kogod-tales57160b3222bfe.Event_NOorganicevent_name.201608240000-201608310000-TZ_UTC-CUR_USD.csv?AWSAccessKeyId=AKIAJ4JWD2OCXDYVOG4A\u0026Expires=1474004927\u0026Signature=Puc165S9KMtFNK3tZ91vc%2F%2FltdY%3D",
//    "report_type":"detail",
//    "report_request": {
//        "api_key":"#########",
//        "app_guid":"#########",
//        "time_start":"1471996800",
//        "time_end":"1472601600",
//        "traffic":["event"],
//        "time_zone":"UTC",
//        "traffic_filtering":{
//            "event_name":[
//                "KOCHAVA INITIALIZATION",
//                "BuyIAP_mightbolt5",
//                "BuyIAP_mightbolt4",
//                "BuyIAP_mightbolt3",
//                "BuyIAP_mightbolt2",
//                "BuyIAP_mightbolt1"
//            ]
//        },
//        "delivery_format":"csv",
//        "delivery_method":["S3link"],
//        "currency":"USD"
//    }
//}';

$json = json_decode($content);
if ($json->status != 'completed') {
    goto wait_progress;
}

$dir = "/var/www/html/kochava";
$filename = "almighty_ios_{$date}.csv";

redownload:
exec("wget -O {$GLOBALS['dir']}/$filename ".$json->report);

if (!is_file("{$GLOBALS['dir']}/$filename")) {
    goto redownload;
}

exec("s3cmd put {$GLOBALS['dir']}/$filename s3://kochava/almighty_ios/$filename");

$table_name = "kochava_event_almighty_ios";

$pcmd = "psql --host=$rhost --port=$rport --username=$ruser --no-password --echo-all $rdatabase  -c \"DELETE FROM {$table_name} WHERE timestamp_utc between '$time_start' and '$time_end';\"";
//echo $pcmd;
$output = array();
exec($pcmd, $output);
echo implode("\n", $output) . "\n\n";

$pcmd = "psql --host=$rhost --port=$rport --username=$ruser --no-password --echo-all $rdatabase  -c \"COPY {$table_name} FROM 's3://kochava/almighty_ios/{$filename}' CREDENTIALS 'aws_access_key_id={$aws_access_key_id};aws_secret_access_key={$aws_secret_access_key}' DELIMITER ',' IGNOREHEADER 1 REMOVEQUOTES;\"";
//echo $pcmd;
$output = array();
exec($pcmd, $output);
echo implode("\n", $output) . "\n\n";

unlink("{$GLOBALS['dir']}/$filename");
