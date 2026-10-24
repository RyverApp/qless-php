<?php

namespace Qless;

class Listener
{
    /**
     * @var \Redis
     */
    private $redis;

    /**
     * @var string[]
     */
    private $channels;

    /**
     * @param array<string, mixed> $redisConfig
     * @param string[]             $channels
     */
    public function __construct(array $redisConfig, array $channels)
    {
        $this->redis = new \Redis();
        $this->redis->connect($redisConfig['host'], $redisConfig['port']);

        $this->channels = $channels;
    }

    /**
     * Wait for messages
     *
     * @param callable $callback
     */
    public function messages(callable $callback): void
    {
        try {
            $this->redis->subscribe($this->channels, function (\Redis $redis, $channel, $data) use ($callback): void {
                $callback($channel, json_decode($data));
            });
        } catch (\RedisException $e) {
            if ($e->getMessage() !== 'Connection closed') {
                throw $e;
            }
        }
    }

    public function stop(): void
    {
        $this->redis->unsubscribe($this->channels);
        // workaround: give the above unsubscribe a chance to fully process
        sleep(1);
        $this->redis->close();
    }
}
