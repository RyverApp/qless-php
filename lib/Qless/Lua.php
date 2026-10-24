<?php

namespace Qless;

/**
 * Wrapper to load and execute lua script for qless-core.
 */
class Lua
{
    /**
     * @var \Redis
     */
    protected $redisCli;

    /**
     * @var string
     */
    private $redisHost;

    /**
     * @var int
     */
    private $redisPort;

    /**
     * @var int
     */
    private $redisDb;

    /**
     * @var string
     */
    protected $sha;

    /**
     * @param array{host: string, port: int, db: int} $redis
     */
    public function __construct($redis)
    {
        $this->redisHost = $redis['host'];
        $this->redisPort = $redis['port'];
        $this->redisDb   = $redis['db'];

        $this->redisCli = new \Redis();
        $this->redisCli->connect($this->redisHost, $this->redisPort);
        $this->redisCli->select($this->redisDb);
    }

    /**
     * @param string       $command
     * @param array<mixed> $args
     *
     * @return mixed
     */
    public function run($command, $args)
    {
        if (empty($this->sha)) {
            $this->reload();
        }
        $luaArgs  = [$command, microtime(true)];
        $argArray = array_merge($luaArgs, $args);
        $result   = $this->redisCli->evalSha($this->sha, $argArray);
        $error    = $this->redisCli->getLastError();
        if ($error && str_starts_with($error, 'NOSCRIPT')) {
            $this->redisCli->clearLastError();
            $script    = file_get_contents(__DIR__ . '/qless-core/qless.lua', true);
            $this->sha = $this->redisCli->script('load', $script);
            $error     = $this->redisCli->getLastError();
        }
        if ($error) {
            $this->handleError($error);
            return null;
        }

        return $result;
    }

    /**
     * @param string $error
     *
     * @throws QlessException
     */
    protected function handleError($error): void
    {
        $this->redisCli->clearLastError();
        throw QlessException::createExceptionFromError($error);
    }

    protected function reload(): void
    {
        $script    = file_get_contents(__DIR__ . '/qless-core/qless.lua', true);
        $this->sha = sha1($script);
        $res       = $this->redisCli->script('exists', $this->sha);
        if ($res[0] !== 1) {
            $this->sha = $this->redisCli->script('load', $script);
            if (!$this->sha) {
                $error = $this->redisCli->getLastError();
                $this->handleError($error);
            }
        }
    }

    /**
     * Removes all the entries from the default Redis database
     *
     * @internal
     */
    public function flush(): void
    {
        $this->redisCli->flushDB();
    }

    /**
     * Reconnect to the Redis server
     */
    public function reconnect(): void
    {
        $this->redisCli->close();
        $this->redisCli->connect($this->redisHost, $this->redisPort);
        $this->redisCli->select($this->redisDb);
    }
}
