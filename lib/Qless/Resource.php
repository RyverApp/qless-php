<?php

namespace Qless;

class Resource
{
    /**
     * @var int|null
     */
    private $max;

    public function __construct(private readonly Client $client, private readonly string $name)
    {
    }

    /**
     * Returns true if this resource exists
     */
    public function exists(): bool
    {
        return $this->client->lua->run('resource.exists', [$this->name]) === 1;
    }

    /**
     * Gets the current lock count for this resource
     *
     * @return int
     */
    public function getLockCount()
    {
        return $this->client->lua->run('resource.lock_count', [$this->name]);
    }

    /**
     * Get a list of job identifiers that have active locks for this resource
     *
     * @return string[]
     */
    public function getLocks(): mixed
    {
        $data = $this->client->lua->run('resource.locks', [$this->name]);

        return json_decode($data);
    }

    /**
     * Gets the current lock count for this resource
     *
     * @return int
     */
    public function getPendingCount()
    {
        return $this->client->lua->run('resource.pending_count', [$this->name]);
    }

    /**
     * Gets a list of job identifiers that are waiting on this resource
     *
     * @return string[]
     */
    public function getPending(): mixed
    {
        $data = $this->client->lua->run('resource.pending', [$this->name]);

        return json_decode($data);
    }

    /**
     * Deletes this resource
     *
     * @return bool true if resource was deleted
     */
    public function delete(): bool
    {
        return $this->client->lua->run('resource.unset', [$this->name]) === 1;
    }

    /**
     * @param int $max Update the maximum number of units available for this resource
     */
    public function setMax($max): void
    {
        $this->client->lua->run('resource.set', [$this->name, $max]);
        unset($this->max);
    }

    /**
     * Return the maximum number of units available
     *
     * @return int
     */
    public function getMax()
    {
        $this->max ??= $this->client->lua->run('resource.get', [$this->name]);
        return $this->max;
    }
}
