# DittoFS Development Scripts

This directory contains scripts to help with DittoFS development environment setup and maintenance.

## Setup Scripts

### `dev-setup.sh` (Linux/macOS)
Automated development environment setup for Unix-like systems.

```bash
# Full setup
./scripts/dev-setup.sh

# Skip system dependencies
./scripts/dev-setup.sh --skip-system

# Skip verification
./scripts/dev-setup.sh --skip-verify
```

**What it does:**
- Checks Python 3.9+ installation
- Installs system dependencies (FUSE, Bluetooth, build tools)
- Creates Python virtual environment
- Installs Python dependencies
- Sets up pre-commit hooks
- Creates development configuration
- Verifies installation

### `dev-setup.ps1` (Windows)
PowerShell script for Windows development environment setup.

```powershell
# Run with execution policy bypass
PowerShell -ExecutionPolicy Bypass -File scripts/dev-setup.ps1

# With options
PowerShell -ExecutionPolicy Bypass -File scripts/dev-setup.ps1 -SkipSystem
```

### `dev-setup.py` (Cross-platform)
Python-based setup script that works on all platforms.

```bash
# Basic setup
python scripts/dev-setup.py

# With options
python scripts/dev-setup.py --skip-system --skip-verify
```

## Docker Scripts

### `docker-dev.sh`
Docker development environment management.

```bash
# Build development image
./scripts/docker-dev.sh build

# Start development environment
./scripts/docker-dev.sh start

# Open shell in container
./scripts/docker-dev.sh shell

# Run tests in container
./scripts/docker-dev.sh test unit

# Start multi-peer testing
./scripts/docker-dev.sh testing

# Clean up
./scripts/docker-dev.sh clean
```

## Pre-commit Scripts

### `setup-pre-commit.sh`
Pre-commit hooks management.

```bash
# Install hooks
./scripts/setup-pre-commit.sh install

# Run all hooks
./scripts/setup-pre-commit.sh run-all

# Run specific hook
./scripts/setup-pre-commit.sh run-hook black

# Update hooks
./scripts/setup-pre-commit.sh update

# Show status
./scripts/setup-pre-commit.sh status
```

## Verification Scripts

### `verify-dev-setup.py`
Comprehensive development environment verification.

```bash
# Verify complete setup
python scripts/verify-dev-setup.py
```

**Checks performed:**
- Python version (>= 3.9)
- Virtual environment status
- Required dependencies
- Pre-commit hooks installation
- Code formatting (black)
- Import sorting (isort)
- Linting (flake8)
- Type checking (mypy)
- Security scanning (bandit)
- Test framework
- Project structure
- Git configuration

## Installation Scripts

### `install.sh`
Production installation script for end users.

```bash
# Install latest release
curl -sSL https://raw.githubusercontent.com/your-org/dittofs/main/scripts/install.sh | bash
```

## Usage Examples

### Quick Development Setup

```bash
# Clone repository
git clone https://github.com/your-org/dittofs.git
cd dittofs

# Run setup script
./scripts/dev-setup.sh

# Verify setup
python scripts/verify-dev-setup.py

# Start developing
make test
```

### Docker Development

```bash
# Start Docker environment
./scripts/docker-dev.sh build
./scripts/docker-dev.sh start

# Work in container
./scripts/docker-dev.sh shell

# Inside container
make test
make lint
```

### Pre-commit Workflow

```bash
# Install hooks (done automatically by dev-setup)
./scripts/setup-pre-commit.sh install

# Before committing
./scripts/setup-pre-commit.sh run-all

# Or let hooks run automatically on commit
git commit -m "your changes"
```

## Troubleshooting

### Common Issues

1. **Permission denied on scripts**
   ```bash
   chmod +x scripts/*.sh scripts/*.py
   ```

2. **Python version issues**
   ```bash
   # Check Python version
   python --version
   python3 --version
   
   # Use specific Python version
   python3.11 scripts/dev-setup.py
   ```

3. **Virtual environment issues**
   ```bash
   # Remove and recreate
   rm -rf .ditto
   ./scripts/dev-setup.sh
   ```

4. **Docker issues**
   ```bash
   # Check Docker status
   docker info
   
   # Clean up Docker resources
   ./scripts/docker-dev.sh clean
   ```

5. **Pre-commit issues**
   ```bash
   # Reinstall hooks
   pre-commit uninstall
   pre-commit install
   
   # Clear cache
   pre-commit clean
   ```

### Platform-Specific Notes

#### Linux
- Requires `sudo` for system package installation
- Supports apt, yum, and pacman package managers
- FUSE3 support requires kernel modules

#### macOS
- Requires Homebrew for system dependencies
- macFUSE installation may require system restart
- Xcode Command Line Tools needed for compilation

#### Windows
- Requires PowerShell 5.0 or later
- Visual Studio Build Tools recommended
- WSL recommended for better development experience

### Getting Help

1. **Check script help:**
   ```bash
   ./scripts/dev-setup.sh --help
   ./scripts/docker-dev.sh help
   ./scripts/setup-pre-commit.sh help
   ```

2. **Run verification:**
   ```bash
   python scripts/verify-dev-setup.py
   ```

3. **Check logs:**
   ```bash
   # Enable debug logging
   export DITTOFS_LOG_LEVEL=DEBUG
   ./scripts/dev-setup.sh
   ```

4. **Create issue:**
   - Include output from verification script
   - Specify operating system and version
   - Include error messages and logs

## Contributing to Scripts

When modifying or adding scripts:

1. **Follow naming conventions:**
   - Use kebab-case for script names
   - Add appropriate file extensions
   - Make scripts executable

2. **Include help text:**
   - Add `--help` option
   - Document all parameters
   - Provide usage examples

3. **Error handling:**
   - Check prerequisites
   - Provide clear error messages
   - Suggest solutions

4. **Cross-platform support:**
   - Test on multiple platforms
   - Handle platform differences
   - Provide fallbacks

5. **Documentation:**
   - Update this README
   - Add inline comments
   - Document any new dependencies

## Script Dependencies

### System Dependencies
- **Linux:** apt/yum/pacman, sudo, curl, git
- **macOS:** Homebrew, Xcode Command Line Tools
- **Windows:** PowerShell, Git, Visual Studio Build Tools

### Python Dependencies
- Python 3.9+
- pip
- venv module

### Optional Dependencies
- Docker (for container development)
- WSL (Windows, recommended)
- Pre-commit (installed automatically)

---

For questions about development scripts, please check the main [CONTRIBUTING.md](../CONTRIBUTING.md) or create an issue.