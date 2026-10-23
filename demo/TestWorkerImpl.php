<?php

class TestWorkerImpl
{
    public function myPerformMethod(Qless\Job $job): void
    {
        echo "here in worker performMethod\n\n";
        $job->complete();

        // retry
        // heartbeat

        // fail
        // complete - on success, can throw in some cases, need to handle responses.

        // jobId = instance_id:job_name:user_id:additional if always unique
    }

    public function myThrowMethod($job): void
    {
        echo "in throw job.\n";
        sleep(15);
        throw new \Exception('job exception message.');
    }

    public function exitMethod($job): void
    {
        sleep(5);
        exit(1);
    }
}
