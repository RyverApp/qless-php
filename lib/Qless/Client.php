<?php

namespace Qless;

/**
 * Call commands in the qless-core lua script
 *
 * @method string|int   put(?string $worker, string $queue, string $jid, string $klass, string $data, int $delay_in_seconds, mixed ...$args)
 * @method string       recur(string $queue, string $jid, string $klass, string $data, string $spec, mixed ...$args)
 * @method string       pop(string $queue, string $worker, int $count)
 * @method int          length(string $queue)
 * @method string       stats(string $queue, int $date)
 * @method int|false    paused(string $queue)
 * @method void         pause(string ...$queues)
 * @method void         unpause(string ...$queues)
 * @method string       requeue(string $worker, string $queue, string $jid, string $klass, string $data, int $delay_in_seconds, mixed ...$args)
 * @method int          unrecur(string $jid)
 * @method int          retry(string $jid, string $queue, string $worker, int $delay, string $group, string $message)
 * @method string[]     cancel(string ...$jids)
 * @method string|false get(string $jid)
 * @method string       multiget(string ...$jids)
 * @method string[]     jobs(string $state, mixed ...$args)
 * @method int          heartbeat(string $jid, string $worker, string $data = null)
 * @method string       complete(string $jid, string $worker, string $queue, string $data = null, mixed ...$args)
 * @method string       fail(string $jid, string $worker, string $group, string $message, string $data = null)
 * @method void         timeout(string ...$jids)
 * @method string       tag(string $command, mixed ...$args)
 * @method string       failed(string $group = null, int $start = 0, int $limit = 25)
 * @method string       queues(string $queue = null)
 *
 * @property-read Jobs $jobs
 */
class Client
{
    /**
     * Used for testing and internal use
     *
     * @var Lua
     * @internal
     */
    public $lua;

    /**
     * @var Config
     */
    public $config;

    /**
     * @var array{host: string, port: int, db: int}
     */
    private array $redis;

    private readonly Jobs $_jobs;

    public function __construct(string $host = 'localhost', int $port = 6379, int $db = 0)
    {
        $this->redis['host'] = $host;
        $this->redis['port'] = $port;
        $this->redis['db']   = $db;

        $this->lua    = new Lua($this->redis);
        $this->config = new Config($this);
        $this->_jobs  = new Jobs($this);
    }

    /**
     * Return the host for the Redis server
     *
     * @return string
     */
    public function getRedisHost()
    {
        return $this->redis['host'];
    }

    /**
     * Return the port for the Redis server
     *
     * @return int
     */
    public function getRedisPort()
    {
        return $this->redis['port'];
    }

    /**
     * Return the DB for the Redis server
     *
     * @return int
     */
    public function getRedisDb()
    {
        return $this->redis['db'];
    }

    /**
     * Create a new listener
     *
     * @param string[] $channels
     */
    public function createListener($channels): Listener
    {
        return new Listener($this->redis, $channels);
    }

    /**
     * @param array<mixed> $arguments
     *
     * @return mixed
     */
    public function __call(string $command, array $arguments)
    {
        return $this->lua->run($command, $arguments);
    }

    public function __get(string $name): mixed
    {
        if ($name === 'jobs') {
            return $this->_jobs;
        }

        return null;
    }

    public function __set(string $name, mixed $value): void
    {
    }

    public function __isset(string $name): bool
    {
        return $name === 'jobs';
    }

    /**
     * Call a specific qless command
     *
     * @param string       $command
     * @param array<mixed> $arguments ...
     *
     * @return mixed
     */
    public function call($command, ...$arguments)
    {
        return $this->lua->run($command, $arguments);
    }

    /**
     * Returns
     *
     * @param string $name name of queue
     */
    public function getQueue($name): Queue
    {
        return new Queue($name, $this);
    }

    /**
     * APIs for manipulating a resource
     *
     * @param string $name
     */
    public function getResource($name): Resource
    {
        return new Resource($this, $name);
    }

    /**
     * Returns APIs for querying information about jobs
     */
    public function getJobs(): Jobs
    {
        return $this->_jobs;
    }

    /**
     * Call to reconnect to Redis server
     */
    public function reconnect(): void
    {
        $this->lua->reconnect();
    }
}
