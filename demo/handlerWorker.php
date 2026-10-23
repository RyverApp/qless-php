<?php

require __DIR__ . '/../vendor/autoload.php';
require __DIR__ . '/TestWorkerImpl.php';

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
