# Contributing to DittoFS

Thank you for your interest in contributing to DittoFS! This document provides guidelines and information for contributors.

## Table of Contents

- [Getting Started](#getting-started)
- [Development Environment Setup](#development-environment-setup)
- [Development Workflow](#development-workflow)
- [Code Standards](#code-standards)
- [Testing](#testing)
- [Submitting Changes](#submitting-changes)
- [Code Review Process](#code-review-process)
- [Community Guidelines](#community-guidelines)

## Getting Started

DittoFS is a distributed offline file system that enables peer-to-peer file sharing without requiring a central server. Before contributing, please:

1. Read the [README.md](README.md) to understand the project
2. Check the [issues](https://github.com/your-org/dittofs/issues) for open tasks
3. Join our community discussions
4. Set up your development environment

### Prerequisites

- Python 3.9 or later
- Git
- Basic understanding of distributed systems and file synchronization
- Familiarity with async/await programming in Python

## Development Environment Setup

We provide multiple ways to set up your development environment:

### Option 1: Automated Setup (Recommended)

**Linux/macOS:**
```bash
./scripts/dev-setup.sh
```

**Windows:**
```powershell
PowerShell -ExecutionPolicy Bypass -File scripts/dev-setup.ps1
```

**Cross-platform Python script:**
```bash
python scripts/dev-setup.py
```

### Option 2: Docker Development Environment

```bash
# Build and start development environment
./scripts/docker-dev.sh build
./scripts/docker-dev.sh start

# Open shell in development container
./scripts/docker-dev.sh shell
```

### Option 3: Manual Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/your-org/dittofs.git
   cd dittofs
   ```

2. **Create virtual environment:**
   ```bash
   python -m venv .ditto
   source .ditto/bin/activate  # Linux/macOS
   # or
   .ditto\Scripts\activate     # Windows
   ```

3. **Install dependencies:**
   ```bash
   pip install -e .[dev,all]
   ```

4. **Set up pre-commit hooks:**
   ```bash
   pre-commit install
   ```

5. **Verify installation:**
   ```bash
   make test-unit
   ```

## Development Workflow

### 1. Issue Assignment

- Check existing issues or create a new one
- Comment on the issue to get it assigned to you
- For major changes, discuss the approach first

### 2. Branch Creation

Create a feature branch from `main`:

```bash
git checkout main
git pull origin main
git checkout -b feature/your-feature-name
```

Branch naming conventions:
- `feature/description` - New features
- `fix/description` - Bug fixes
- `docs/description` - Documentation updates
- `refactor/description` - Code refactoring
- `test/description` - Test improvements

### 3. Development Process

1. **Write tests first** (Test-Driven Development)
2. **Implement the feature/fix**
3. **Run tests frequently:**
   ```bash
   make test-unit          # Quick unit tests
   make test-integration   # Integration tests
   make test-all          # All tests
   ```

4. **Check code quality:**
   ```bash
   make lint              # Run linting
   make format            # Format code
   ```

### 4. Commit Guidelines

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

**Examples:**
```bash
git commit -m "feat(sync): add delta synchronization for large files"
git commit -m "fix(chunker): handle edge case with empty files"
git commit -m "docs: update API documentation for CRDT store"
```

### 5. Pre-commit Hooks

Pre-commit hooks automatically run before each commit:

- **Black**: Code formatting
- **isort**: Import sorting
- **flake8**: Linting
- **mypy**: Type checking
- **bandit**: Security scanning

If hooks fail, fix the issues and commit again.

## Code Standards

### Python Code Style

- **PEP 8** compliance (enforced by flake8)
- **Black** formatting (line length: 88 characters)
- **Type hints** for all functions and methods
- **Docstrings** for all public functions (Google style)

### Code Organization

```python
# Standard library imports
import asyncio
import logging
from pathlib import Path
from typing import Dict, List, Optional

# Third-party imports
import trio
import blake3
from pycrdt import Doc

# Local imports
from .chunker import Chunker
from .crdt_store import CRDTStore
```

### Async/Await Guidelines

- Use `async`/`await` for I/O operations
- Prefer `trio` over `asyncio` for new code
- Handle cancellation properly
- Use structured concurrency patterns

### Error Handling

```python
import logging

logger = logging.getLogger(__name__)

async def example_function():
    try:
        # Operation that might fail
        result = await risky_operation()
        return result
    except SpecificException as e:
        logger.error(f"Specific error occurred: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise
```

### Configuration

- Use dataclasses for configuration objects
- Provide sensible defaults
- Support environment variable overrides

## Testing

### Test Structure

```
tests/
├── unit/                 # Unit tests
├── integration/          # Integration tests
├── property/            # Property-based tests
├── performance/         # Performance tests
├── security/           # Security tests
├── conftest.py         # Pytest configuration
├── fixtures.py         # Test fixtures
├── mocks.py           # Mock objects
└── test_utils.py      # Test utilities
```

### Writing Tests

1. **Unit Tests**: Test individual functions/classes
   ```python
   import pytest
   from dittofs.chunker import Chunker

   @pytest.mark.asyncio
   async def test_chunker_splits_file():
       chunker = Chunker()
       data = b"test data" * 1000
       chunks = await chunker.chunk_data(data)
       assert len(chunks) > 0
   ```

2. **Integration Tests**: Test component interactions
   ```python
   @pytest.mark.integration
   @pytest.mark.asyncio
   async def test_sync_between_peers():
       # Test synchronization between multiple peers
       pass
   ```

3. **Property-based Tests**: Use Hypothesis
   ```python
   from hypothesis import given, strategies as st

   @given(st.binary())
   def test_chunker_roundtrip(data):
       # Test that chunking and reassembly preserves data
       pass
   ```

### Running Tests

```bash
# Quick tests (unit tests only)
make test

# Specific test categories
make test-unit
make test-integration
make test-property
make test-security

# All tests
make test-all

# Parallel execution
make test-parallel

# With coverage
make coverage
```

### Test Markers

Use pytest markers to categorize tests:

```python
@pytest.mark.unit
@pytest.mark.asyncio
async def test_unit_function():
    pass

@pytest.mark.integration
@pytest.mark.network
async def test_network_sync():
    pass

@pytest.mark.slow
def test_large_file_processing():
    pass
```

## Submitting Changes

### 1. Pre-submission Checklist

- [ ] All tests pass
- [ ] Code is properly formatted
- [ ] Type checking passes
- [ ] Documentation is updated
- [ ] Commit messages follow conventions
- [ ] No security vulnerabilities introduced

### 2. Create Pull Request

1. **Push your branch:**
   ```bash
   git push origin feature/your-feature-name
   ```

2. **Create PR** with:
   - Clear title and description
   - Reference related issues
   - Include testing instructions
   - Add screenshots/demos if applicable

### 3. PR Template

```markdown
## Description
Brief description of changes

## Related Issues
Fixes #123

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] Manual testing performed

## Checklist
- [ ] Code follows style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] Tests pass locally
```

## Code Review Process

### For Contributors

- **Respond promptly** to review feedback
- **Ask questions** if feedback is unclear
- **Make requested changes** in separate commits
- **Squash commits** before final merge (if requested)

### Review Criteria

Reviewers will check:

1. **Functionality**: Does the code work as intended?
2. **Code Quality**: Is the code clean and maintainable?
3. **Testing**: Are there adequate tests?
4. **Documentation**: Is documentation updated?
5. **Security**: Are there security implications?
6. **Performance**: Does it impact performance?

### Approval Process

- At least one maintainer approval required
- All CI checks must pass
- No unresolved conversations
- Branch must be up-to-date with main

## Community Guidelines

### Code of Conduct

We follow the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). Please read and follow it.

### Communication

- **Be respectful** and constructive
- **Ask questions** when unsure
- **Help others** when you can
- **Share knowledge** and learn from others

### Getting Help

- **GitHub Issues**: Bug reports and feature requests
- **Discussions**: General questions and ideas
- **Discord/Slack**: Real-time chat (if available)
- **Email**: Maintainer contact for sensitive issues

## Development Tips

### Debugging

1. **Use logging extensively:**
   ```python
   import logging
   logger = logging.getLogger(__name__)
   logger.debug("Debug information")
   ```

2. **Enable debug mode:**
   ```bash
   export DITTOFS_LOG_LEVEL=DEBUG
   python -m dittofs.cli daemon
   ```

3. **Use pytest debugging:**
   ```bash
   pytest -v -s tests/test_specific.py::test_function
   ```

### Performance Profiling

```bash
# Profile tests
python -m pytest --profile-svg tests/

# Memory profiling
python -m pytest --memray tests/
```

### Security Testing

```bash
# Run security scans
make security-scan

# Check dependencies
make check-deps
```

## Release Process

### Version Numbering

We use [Semantic Versioning](https://semver.org/):
- `MAJOR.MINOR.PATCH`
- `MAJOR`: Breaking changes
- `MINOR`: New features (backward compatible)
- `PATCH`: Bug fixes (backward compatible)

### Release Checklist

1. Update version in `pyproject.toml`
2. Update `CHANGELOG.md`
3. Run full test suite
4. Create release PR
5. Tag release after merge
6. Build and publish packages

## Troubleshooting

### Common Issues

1. **Import errors**: Check virtual environment activation
2. **Test failures**: Ensure all dependencies installed
3. **Pre-commit failures**: Run `pre-commit run --all-files`
4. **Docker issues**: Check Docker daemon is running

### Getting Unstuck

1. Check existing issues and discussions
2. Ask in community channels
3. Create a detailed issue with:
   - Environment information
   - Steps to reproduce
   - Expected vs actual behavior
   - Relevant logs

## Thank You!

Your contributions make DittoFS better for everyone. We appreciate your time and effort in improving this project!

---

For questions about contributing, please open an issue or reach out to the maintainers.