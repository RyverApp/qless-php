<?php

namespace Qless;

/**
 * @implements \IteratorAggregate<string, Job>
 */
class JobIterator implements \IteratorAggregate, \Countable
{
    /**
     * @var callable
     */
    private $pager;

    /**
     * @var array<string, mixed>
     */
    private $page;

    public function __construct(callable $pager, private readonly Client $client)
    {
        $this->pager = $pager;
    }

    public function getIterator(): \Generator
    {
        $pager = $this->pager;

        while (true) {
            $this->page = $res = json_decode($pager(), true);
            if ($res['jobs']) {
                yield from $this->client->getJobs()->multiget($res['jobs']);
            } else {
                break;
            }
        }
    }

    public function count(): int
    {
        if (!$this->page) {
            $this->getIterator()->next();
        }
        return $this->page['total'];
    }
}
