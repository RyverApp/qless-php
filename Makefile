.PHONY: deps
deps:
	composer install

.PHONY: outdated
outdated:
	composer outdated -D

.PHONY: upgrade
upgrade:
	composer update -o --no-blocking

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
