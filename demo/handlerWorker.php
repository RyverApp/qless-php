<?php

require_once '../lib/Qless/Worker.php';
require_once '../lib/Qless/Queue.php';
require_once '../lib/Qless/Client.php';
require_once 'TestWorkerImpl.php';

class JobHandler
{
    public function perform(Qless\Job $job): void
    {
        echo 'Here in JobHandler perform';
        $job->perform();
    }
}

$queues = ['testQueue1', 'testQueue2'];
$client = new Qless\Client();
$worker = new Qless\Worker('WorkerTest_1', $queues, $client, 5);
$worker->registerJobPerformHandler('JobHandler');

$worker->run();
