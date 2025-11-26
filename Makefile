.PHONY: install test test-examples verify-examples lint format type-check quality clean build check-dist publish-testpypi publish-pypi

install:
	uv pip install -e ".[dev]"

test:
	pytest tests/unit -v --cov=src --cov-report=term-missing

test-integration:
	pytest tests/integration -v

test-all:
	pytest tests -v --cov=src --cov-report=html

test-examples:
	python scripts/test_examples.py

verify-examples:
	python scripts/verify_examples.py

lint:
	ruff check src tests

format:
	ruff format src tests

type-check:
	mypy src

quality: lint type-check test verify-examples

clean:
	rm -rf build dist *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

build: clean
	python -m build

check-dist:
	python -m twine check dist/*

publish-testpypi: build check-dist
	@echo "Publishing to TestPyPI..."
	python -m twine upload --repository testpypi dist/*

publish-pypi: build check-dist
	@echo "Publishing to PyPI..."
	@echo "⚠️  Make sure you've tested on TestPyPI first!"
	python -m twine upload dist/*
