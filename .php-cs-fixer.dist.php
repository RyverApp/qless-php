<?php

$finder = PhpCsFixer\Finder::create()
    ->in(__DIR__)
    ->exclude('vendor');

return (new PhpCsFixer\Config())
    ->setRules([
        '@PSR12' => true,
        '@PHP82Migration' => true,
        '@PHP82Migration:risky' => true,
        'binary_operator_spaces' => ['operators' => ['=' => 'align_single_space_minimal', '=>' => 'align_single_space_minimal']],
        'declare_strict_types' => false,
        'phpdoc_align' => ['align' => 'vertical'],
        'phpdoc_readonly_class_comment_to_keyword' => false,
    ])
    ->setRiskyAllowed(true)
    ->setFinder($finder);
