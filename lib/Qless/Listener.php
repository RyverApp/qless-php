<?php

namespace Qless;

class Listener
{
    private readonly \Redis $redis;

    /**
     * @param array<string, mixed> $redisConfig
     * @param string[]             $channels
     */
    public function __construct(array $redisConfig, private readonly array $channels)
    {
        $this->redis = new \Redis();
        $this->redis->connect($redisConfig['host'], $redisConfig['port']);
    }

    /**
     * Wait for messages
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
