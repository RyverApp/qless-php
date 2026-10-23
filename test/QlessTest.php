<?php

require_once __DIR__ . '/LuaTester.php';

/**
 * Base class for qless-php testing
 */
abstract class QlessTest extends \PHPUnit\Framework\TestCase
{
    protected static $REDIS_HOST;
    protected static $REDIS_PORT;
    protected static $REDIS_DB;

    public static function setUpBeforeClass(): void
    {
        self::$REDIS_HOST = getenv('REDIS_HOST') ?: 'localhost';
        self::$REDIS_PORT = (int)(getenv('REDIS_PORT') ?: 6379);
        self::$REDIS_DB   = (int)(getenv('REDIS_DB') ?: 0);
    }

    /**
     * @var Qless\Client
     */
    protected $client;

    public function setUp(): void
    {
        $this->client = new Qless\Client(self::$REDIS_HOST, self::$REDIS_PORT, self::$REDIS_DB);
    }

    public function tearDown(): void
    {
        $this->client->lua->flush();
    }
}
