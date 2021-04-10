help:
	@echo targets are: setup clean help

setup:
	mkdir -p test_data
	mkdir -p test_cache


clean:
	rm -rf test_data test_cache

.PHONY: setup clean help
