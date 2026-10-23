<?php

require_once '../lib/Qless/Worker.php';
require_once '../lib/Qless/Queue.php';
require_once '../lib/Qless/Client.php';
require_once 'TestWorkerImpl.php';

$client = new Qless\Client();
$queue  = new Qless\Queue('testQueue1', $client);
$queue2 = new Qless\Queue('testQueue2', $client);

$testData1 = ['performMethod' => 'myPerformMethod', 'payload' => 'otherData'];
//$testData1 = ["performMethod"=>'myThrowMethod',"payload"=>"otherData"];
$testData2 = ['performMethod' => 'exitMethod', 'payload' => 'otherData'];
$ret       = $queue->put('TestWorkerImpl', 'jobTestDEF', $testData1);
if ($ret) {
    echo "successfully put on queue.\n";
} else {
    echo "failed put on queue.\n";
}
$ret = $queue->put('TestWorkerImpl', 'jobTestGHI', $testData2);
if ($ret) {
    echo "successfully put on queue.\n";
} else {
    echo "failed put on queue.\n";
}
