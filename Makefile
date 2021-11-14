help:
	@echo targets are: setup test clean help

setup:
	mkdir -p test_data
	mkdir -p test_cache

test: setup
	cd tests; pytest

clean:
	rm -rf test_data test_cache tests/test_temp

.PHONY: setup test clean help
