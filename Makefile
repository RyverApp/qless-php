.PHONY: deps
deps:
	composer install

.PHONY: format
format:
	vendor/bin/php-cs-fixer fix

.PHONY: check
check:
	composer validate --strict
	vendor/bin/php-cs-fixer fix --dry-run --diff
	vendor/bin/phpstan analyse

.PHONY: test
test:
	php vendor/bin/phpunit
