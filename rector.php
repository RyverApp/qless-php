<?php

use Rector\CodingStyle\Rector\ArrowFunction\ArrowFunctionDelegatingCallToFirstClassCallableRector;
use Rector\Config\RectorConfig;
use Rector\DeadCode\Rector\ClassMethod\RemoveEmptyClassMethodRector;
use Rector\ValueObject\PhpVersion;

return RectorConfig::configure()
    ->withPaths([
        __DIR__ . '/lib',
        __DIR__ . '/test',
        __DIR__ . '/demo',
    ])
    ->withPhpSets()
    ->withPhpVersion(PhpVersion::PHP_82)
    ->withPreparedSets(
        deadCode: true,
        typeDeclarations: true,
    )
    ->withSkip([
        // Unsafe when the arrow function body calls a method with side effects, e.g. `fn () => $queue->pop()[0]->getId()`;
        // the first-class callable form would evaluate the receiver once instead of on every invocation.
        ArrowFunctionDelegatingCallToFirstClassCallableRector::class,
        // `Client::__set()` is an intentional no-op magic method; removing it would re-enable dynamic property writes.
        RemoveEmptyClassMethodRector::class,
    ])
    // Keep the existing fully-qualified class name style; php-cs-fixer owns import formatting.
    ->withImportNames(importNames: false, importDocBlockNames: false, importShortClasses: false);
