<?php

require_once __DIR__ . '/QlessTest.php';

class JobsTest extends QlessTest
{
    public function testItReturnsNullForInvalidJobID(): void
    {
        $j = $this->client->jobs['xxx'];

        $this->assertNull($j);
    }

    public function testItReturnsExistingJob(): void
    {
        $this->put('j-1');
        $j = $this->client->jobs['j-1'];

        $this->assertNotNull($j);
        $this->assertEquals('j-1', $j->getId());
    }

    public function testItReturnsExistingJobsKeyedByJobIdentifier(): void
    {
        $this->put('j-1');
        $this->put('j-2');

        $j = $this->client->jobs->multiget(['j-1', 'j-2']);
        $this->assertCount(2, $j);
        $this->assertArrayHasKey('j-1', $j);
        $this->assertArrayHasKey('j-2', $j);
    }

    public function testItReturnsNoCompletedJobsWhenNoneExist(): void
    {
        $this->put('j-1');
        $this->put('j-2');

        $j = $this->client->jobs->completed();
        $this->assertEmpty($j);
    }

    public function testItReturnsCompletedJobs(): void
    {
        $this->put('j-1');
        $this->put('j-2');
        $q = $this->client->getQueue('q-1');
        $q->pop('w-1')[0]->complete();
        $q->pop('w-1')[0]->complete();

        $j = $this->client->jobs->completed();
        sort($j);
        $this->assertEquals(['j-1', 'j-2'], $j);
    }

    public function testItReturnsFailedJobs(): void
    {
        $this->put('j-1');
        $this->put('j-2');
        $this->put('j-3');
        $this->put('j-4');
        $q = $this->client->getQueue('q-1');
        $q->pop('w-1')[0]->fail('system', 'msg');
        $q->pop('w-1')[0]->fail('system', 'msg');
        $q->pop('w-1')[0]->fail('system', 'msg');
        $q->pop('w-1')[0]->fail('main', 'msg');

        $j = $this->client->jobs->failed();
        $this->assertEquals(3, $j['system']);
        $this->assertEquals(1, $j['main']);
    }

    /**
     * @depends testItReturnsFailedJobs
     */
    public function testItReturnsFailedBySpecificGroup(): void
    {
        $this->put('j-1');
        $this->put('j-2');
        $this->put('j-3');
        $this->put('j-4');
        $q = $this->client->getQueue('q-1');
        $q->pop('w-1')[0]->fail('system', 'msg');
        $q->pop('w-1')[0]->fail('system', 'msg');
        $q->pop('w-1')[0]->fail('system', 'msg');
        $q->pop('w-1')[0]->fail('main', 'msg');

        $j = $this->client->jobs->failedForGroup('system');

        $this->assertCount(3, $j);
    }

    public function testItReturnsRunningJob(): void
    {
        $this->put('j-1');
        $this->put('j-2');

        $q = $this->client->getQueue('q-1');
        $q->pop('w-1');

        $j = $this->client->jobs['j-1'];
        $this->assertNotNull($j);
    }

    private function put($jid, $opts = []): void
    {
        $opts = array_merge(
            [
                'data'     => [],
                'delay'    => 0,
                'tags'     => [],
                'priority' => 0,
                'retries'  => 0,
                'interval' => 0,
            ],
            $opts
        );

        $this->client->put(
            null,
            'q-1',
            $jid,
            'k',
            json_encode($opts['data'], JSON_UNESCAPED_SLASHES),
            $opts['delay'],
            'tags',
            json_encode($opts['tags'], JSON_UNESCAPED_SLASHES),
            'priority',
            $opts['priority'],
            'retries',
            $opts['retries'],
            'interval',
            $opts['interval']
        );
    }
}
