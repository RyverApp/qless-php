<?php

require __DIR__ . '/../vendor/autoload.php';
require __DIR__ . '/TestWorkerImpl.php';

$queues     = ['testQueue1', 'testQueue2'];
$REDIS_HOST = getenv('REDIS_HOST') ?: 'localhost';
$REDIS_PORT = getenv('REDIS_PORT') ?: 6379;
$REDIS_DB   = getenv('REDIS_DB') ?: 0;

$client = new Qless\Client($REDIS_HOST, $REDIS_PORT, $REDIS_DB);
$worker = new Qless\Worker('TestWorker_1', $queues, $client, 5);

$worker->run();
