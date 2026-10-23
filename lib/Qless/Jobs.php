<?php

namespace Qless;

class Jobs implements \ArrayAccess
{
    /**
     * @var Client
     */
    private $client;

    public function __construct(Client $client)
    {
        $this->client = $client;
    }

    /**
     * Return a paginated list of JIDs which are in a completed state
     *
     * @param int $offset
     * @param int $count
     *
     * @return string[]
     */
    public function completed($offset = 0, $count = 25)
    {
        return $this->client->jobs('complete', $offset, $count);
    }

    /**
     * Return a {@see Job} instance for the specified job identifier or null if the job does not exist
     *
     * @param string $jid the job identifier to fetch
     *
     * @return Job|null
     */
    public function get($jid)
    {
        return $this->offsetGet($jid);
    }

    /**
     * Returns a list of jobs for the specified job identifiers, keyed by job identifier
     *
     * @param string[] $jids
     *
     * @return Job[]
     */
    public function multiget($jids)
    {
        if (empty($jids)) {
            return [];
        }

        $jobs = json_decode($this->client->multiget(...$jids), true);
        $ret  = [];
        foreach ($jobs as $job_data) {
            $job                = new Job($this->client, $job_data);
            $ret[$job->getId()] = $job;
        }

        return $ret;
    }

    /**
     * Fetches failed jobs for the specified group, keyed by job identifier
     *
     * @param string $group
     * @param int    $start
     * @param int    $chunkSize
     *
     * @return JobIterator
     */
    public function failedForGroup($group, $start = 0, $chunkSize = 25)
    {
        $pager = function () use ($group, &$start, $chunkSize) {
            $res = $this->client->failed($group, $start, $chunkSize);
            $start += $chunkSize;
            return $res;
        };
        return new JobIterator($pager, $this->client);
    }

    /**
     * Fetches the number of failed jobs for each group
     *
     * @return int[]
     */
    public function failed()
    {
        return json_decode($this->client->failed(), true);
    }

    /**
     * Fetches jobs with the specified tag, keyed by job identifier
     *
     * @param string $tag
     * @param int    $start
     * @param int    $chunkSize
     *
     * @return JobIterator
     */
    public function tagged($tag, $start = 0, $chunkSize = 25)
    {
        $pager = function () use ($tag, &$start, $chunkSize) {
            $res = $this->client->tag('get', $tag, $start, $chunkSize);
            $start += $chunkSize;
            return $res;
        };
        return new JobIterator($pager, $this->client);
    }

    public function offsetExists($jid): bool
    {
        return $this->client->get($jid) !== false;
    }

    public function offsetGet($jid): ?Job
    {
        $job_data = $this->client->get($jid);
        if ($job_data === false) {
            $job_data = $this->client->call('recur.get', $jid);
            if ($job_data === false) {
                return null;
            }
        }

        return new Job($this->client, json_decode($job_data, true));
    }

    public function offsetSet($offset, $value): void
    {
        throw new \LogicException('set not supported');
    }

    public function offsetUnset($offset): void
    {
        throw new \LogicException('unset not supported');
    }
}
