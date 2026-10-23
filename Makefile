.PHONY: deps
deps:
	composer install

.PHONY: format
format:
	vendor/bin/php-cs-fixer fix

.PHONY: test
test:
	php vendor/bin/phpunit
