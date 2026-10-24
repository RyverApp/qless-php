<?php

require __DIR__ . '/../vendor/autoload.php';

use Testcontainers\Modules\RedisContainer;

if (getenv('REDIS_HOST') === false) {
    $container = (new RedisContainer(getenv('REDIS_VERSION') ?: '7-alpine'))->start();

    putenv('REDIS_HOST=' . $container->getHost());
    putenv('REDIS_PORT=' . $container->getFirstMappedPort());

    register_shutdown_function(function () use ($container): void {
        $container->stop();
    });
}
