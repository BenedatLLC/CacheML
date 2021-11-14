help:
	@echo targets are: setup test pkg-tag pkg-build pkg-upload clean help

setup:
	mkdir -p test_data
	mkdir -p test_cache

test: setup
	cd tests; pytest

TAG=$(shell python3 -c "import cacheml; print(cacheml.__version__)")


# don't run tag directly, use this target
pkg-upload: pkg-build pkg-tag
	cd $(DWS_DIR); python3 -m twine upload --repository-url https://upload.pypi.org/legacy/ dist/*

pkg-tag:
	@echo "Tagging with $(TAG)"
	git tag -a $(TAG); git push origin main --tags

pkg-build: clean
	python3 -m pip install --upgrade build; python3 -m build

clean:
	rm -rf test_data test_cache tests/test_temp
	rm -rf CacheML.egg-info/ dist/

.PHONY: setup test pkg-upload pkg-tag pkg-build clean help
