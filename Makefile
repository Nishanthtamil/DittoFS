# DittoFS Makefile for development and testing

.PHONY: help install test test-unit test-integration test-property test-performance test-security test-network test-slow test-all test-quick test-parallel lint format coverage clean

# Default target
help:
	@echo "DittoFS Development Commands:"
	@echo ""
	@echo "Setup:"
	@echo "  install          Install development dependencies"
	@echo "  install-dev      Install with all optional dependencies"
	@echo ""
	@echo "Testing:"
	@echo "  test             Run quick tests (unit + basic integration)"
	@echo "  test-unit        Run unit tests only"
	@echo "  test-integration Run integration tests only"
	@echo "  test-property    Run property-based tests with Hypothesis"
	@echo "  test-performance Run performance/benchmark tests"
	@echo "  test-security    Run security-related tests"
	@echo "  test-network     Run network-related tests"
	@echo "  test-slow        Run slow tests (normally skipped)"
	@echo "  test-all         Run all tests except slow ones"
	@echo "  test-parallel    Run tests in parallel"
	@echo ""
	@echo "Code Quality:"
	@echo "  lint             Run code linting (black, flake8, mypy)"
	@echo "  format           Format code with black"
	@echo "  coverage         Generate coverage report"
	@echo ""
	@echo "Maintenance:"
	@echo "  clean            Clean test artifacts and cache files"
	@echo "  clean-all        Clean everything including build artifacts"

# Installation targets
install:
	pip install -e .

install-dev:
	pip install -e .[dev,all]

# Testing targets
test:
	python tests/test_runner.py quick --verbose

test-unit:
	python tests/test_runner.py unit --verbose

test-integration:
	python tests/test_runner.py integration --verbose

test-property:
	python tests/test_runner.py property --verbose

test-performance:
	python tests/test_runner.py performance --verbose

test-security:
	python tests/test_runner.py security --verbose

test-network:
	python tests/test_runner.py network --verbose

test-slow:
	python tests/test_runner.py slow --verbose

test-all:
	python tests/test_runner.py all --verbose

test-parallel:
	python tests/test_runner.py parallel --verbose --workers 4

# Code quality targets
lint:
	python tests/test_runner.py lint

format:
	python tests/test_runner.py format

coverage:
	python tests/test_runner.py coverage

# Maintenance targets
clean:
	python tests/test_runner.py clean

clean-all: clean
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	find . -name "*.pyc" -delete
	find . -name "*.pyo" -delete
	find . -name "__pycache__" -type d -exec rm -rf {} +

# Development workflow targets
dev-setup: install-dev
	@echo "Development environment set up successfully!"
	@echo "Run 'make test' to verify everything is working."

dev-test: format lint test-unit
	@echo "Development tests completed successfully!"

ci-test: lint test-all coverage
	@echo "CI tests completed successfully!"

# Specific test file execution
test-file:
	@if [ -z "$(FILE)" ]; then \
		echo "Usage: make test-file FILE=path/to/test_file.py"; \
		exit 1; \
	fi
	python tests/test_runner.py specific --test-path $(FILE) --verbose

# Run tests with specific markers
test-marker:
	@if [ -z "$(MARKER)" ]; then \
		echo "Usage: make test-marker MARKER=unit"; \
		exit 1; \
	fi
	python -m pytest -m $(MARKER) -v tests/

# Debug test execution
test-debug:
	PYTEST_DEBUG=1 python -m pytest -v -s tests/

# Performance profiling
profile-tests:
	python -m pytest --profile-svg tests/

# Generate test documentation
test-docs:
	python -m pytest --collect-only --quiet tests/ > test_inventory.txt
	@echo "Test inventory generated in test_inventory.txt"

# Continuous testing (requires pytest-watch)
test-watch:
	ptw tests/ -- --verbose

# Test with different Python versions (requires tox)
test-tox:
	tox

# Security scanning
security-scan:
	@echo "🔒 Running comprehensive security scans..."
	bandit -r src/ -f json -o bandit-report.json
	bandit -r src/ -f txt -o bandit-report.txt
	safety check --json --output safety-report.json
	safety check --output safety-report.txt
	pip-audit --format=json --output=pip-audit-report.json
	pip-audit --output=pip-audit-report.txt
	@echo "✅ Security scans completed. Check *-report.* files for details."

# Dependency checking
check-deps:
	@echo "📦 Checking dependencies for vulnerabilities..."
	pip-audit --desc
	safety check
	@echo "✅ Dependency check completed."

# Pre-commit setup and execution
setup-pre-commit:
	@echo "🔧 Setting up pre-commit hooks..."
	pip install pre-commit
	pre-commit install
	pre-commit install --hook-type commit-msg
	@echo "✅ Pre-commit hooks installed."

run-pre-commit:
	@echo "🔍 Running pre-commit checks..."
	pre-commit run --all-files

# CI/CD simulation
ci-local:
	@echo "🚀 Running local CI simulation..."
	$(MAKE) format
	$(MAKE) lint
	$(MAKE) security-scan
	$(MAKE) test-unit
	$(MAKE) test-integration
	@echo "✅ Local CI simulation completed."

# Code quality comprehensive check
quality-check:
	@echo "📊 Running comprehensive code quality checks..."
	black --check --diff src/ tests/
	isort --check-only --diff src/ tests/
	flake8 src/ tests/ --statistics
	mypy src/
	pydocstyle src/ --convention=google
	radon cc src/ --min B --show-complexity
	xenon --max-absolute B --max-modules B --max-average A src/
	@echo "✅ Code quality checks completed."

# Vulnerability scanning with detailed reports
vuln-scan:
	@echo "🛡️ Running detailed vulnerability scanning..."
	bandit -r src/ -f sarif -o bandit-results.sarif
	safety audit --json --output safety-audit.json
	pip-audit --format=cyclonedx-json --output=pip-audit-sbom.json
	@echo "✅ Vulnerability scanning completed with SARIF/SBOM outputs."

# Documentation generation
docs:
	@echo "Documentation generation not yet implemented"

# Docker testing environment
docker-test:
	docker build -t dittofs-test -f Dockerfile.test .
	docker run --rm dittofs-test

# Benchmark comparison
benchmark-compare:
	python -m pytest --benchmark-compare --benchmark-compare-fail=mean:10% tests/

# Memory profiling
memory-profile:
	python -m pytest --memray tests/

# Test data generation
generate-test-data:
	python -c "from tests.test_utils import *; print('Test data generation utilities available')"

# Hypothesis settings for property-based testing
test-property-extensive:
	HYPOTHESIS_MAX_EXAMPLES=1000 python tests/test_runner.py property --verbose --examples 1000

# Integration with external services (when available)
test-external:
	@echo "External service tests not yet implemented"

# Load testing
test-load:
	@echo "Load testing not yet implemented"

# Stress testing
test-stress:
	@echo "Stress testing not yet implemented"

# Mutation testing (requires mutmut)
test-mutation:
	mutmut run --paths-to-mutate src/

# Test report generation
test-report:
	python -m pytest --html=test_report.html --self-contained-html tests/
	@echo "Test report generated: test_report.html"

# Verify test framework setup
verify-framework:
	@echo "Verifying test framework setup..."
	python -c "import tests.conftest; print('✅ conftest.py imported successfully')"
	python -c "import tests.mocks; print('✅ mocks module imported successfully')"
	python -c "import tests.test_utils; print('✅ test_utils module imported successfully')"
	python -c "import tests.base_test_classes; print('✅ base_test_classes module imported successfully')"
	python -c "import tests.fixtures; print('✅ fixtures module imported successfully')"
	@echo "✅ Test framework verification completed!"

# Show test statistics
test-stats:
	python -m pytest --collect-only --quiet tests/ | grep -E "test session starts|collected" | tail -2