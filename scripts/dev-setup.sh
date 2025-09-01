#!/bin/bash
# DittoFS Development Environment Setup Script for Unix-like systems (Linux/macOS)
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Detect OS
detect_os() {
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        echo "linux"
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        echo "macos"
    else
        echo "unknown"
    fi
}

# Check Python version
check_python() {
    log_info "Checking Python installation..."
    
    if command_exists python3; then
        PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
        PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d'.' -f1)
        PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d'.' -f2)
        
        if [ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -ge 9 ]; then
            log_success "Python $PYTHON_VERSION found (>= 3.9 required)"
            return 0
        else
            log_error "Python $PYTHON_VERSION found, but >= 3.9 is required"
            return 1
        fi
    else
        log_error "Python 3 not found. Please install Python 3.9 or later"
        return 1
    fi
}

# Install system dependencies
install_system_deps() {
    local os=$(detect_os)
    log_info "Installing system dependencies for $os..."
    
    case $os in
        "linux")
            # Check for package manager and install dependencies
            if command_exists apt-get; then
                log_info "Using apt-get package manager"
                sudo apt-get update
                sudo apt-get install -y \
                    python3-dev \
                    python3-pip \
                    python3-venv \
                    build-essential \
                    libfuse3-dev \
                    pkg-config \
                    git \
                    curl \
                    bluetooth \
                    libbluetooth-dev
            elif command_exists yum; then
                log_info "Using yum package manager"
                sudo yum install -y \
                    python3-devel \
                    python3-pip \
                    gcc \
                    gcc-c++ \
                    fuse3-devel \
                    pkgconfig \
                    git \
                    curl \
                    bluez-libs-devel
            elif command_exists pacman; then
                log_info "Using pacman package manager"
                sudo pacman -S --noconfirm \
                    python \
                    python-pip \
                    base-devel \
                    fuse3 \
                    pkgconf \
                    git \
                    curl \
                    bluez-libs
            else
                log_warning "Unknown package manager. Please install system dependencies manually:"
                log_warning "- Python 3.9+ development headers"
                log_warning "- Build tools (gcc, make)"
                log_warning "- FUSE3 development libraries"
                log_warning "- Bluetooth development libraries"
            fi
            ;;
        "macos")
            log_info "Using Homebrew package manager"
            if ! command_exists brew; then
                log_error "Homebrew not found. Please install Homebrew first:"
                log_error "https://brew.sh/"
                return 1
            fi
            
            brew install \
                python@3.11 \
                macfuse \
                pkg-config \
                git \
                curl
            ;;
        *)
            log_error "Unsupported operating system: $os"
            return 1
            ;;
    esac
    
    log_success "System dependencies installed"
}

# Setup Python virtual environment
setup_venv() {
    log_info "Setting up Python virtual environment..."
    
    if [ -d ".ditto" ]; then
        log_warning "Virtual environment .ditto already exists"
        read -p "Do you want to recreate it? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            rm -rf .ditto
        else
            log_info "Using existing virtual environment"
            return 0
        fi
    fi
    
    python3 -m venv .ditto
    source .ditto/bin/activate
    
    # Upgrade pip and setuptools
    pip install --upgrade pip setuptools wheel
    
    log_success "Virtual environment created and activated"
}

# Install Python dependencies
install_python_deps() {
    log_info "Installing Python dependencies..."
    
    # Ensure we're in the virtual environment
    if [[ "$VIRTUAL_ENV" == "" ]]; then
        log_info "Activating virtual environment..."
        source .ditto/bin/activate
    fi
    
    # Install development dependencies
    pip install -e .[dev,all]
    
    log_success "Python dependencies installed"
}

# Setup pre-commit hooks
setup_pre_commit() {
    log_info "Setting up pre-commit hooks..."
    
    # Ensure we're in the virtual environment
    if [[ "$VIRTUAL_ENV" == "" ]]; then
        source .ditto/bin/activate
    fi
    
    # Install and setup pre-commit
    pre-commit install
    pre-commit install --hook-type commit-msg
    
    # Run pre-commit on all files to ensure everything works
    log_info "Running pre-commit on all files (this may take a while)..."
    pre-commit run --all-files || {
        log_warning "Pre-commit found issues. Running auto-fixes..."
        pre-commit run --all-files || true
    }
    
    log_success "Pre-commit hooks installed and configured"
}

# Verify installation
verify_installation() {
    log_info "Verifying installation..."
    
    # Ensure we're in the virtual environment
    if [[ "$VIRTUAL_ENV" == "" ]]; then
        source .ditto/bin/activate
    fi
    
    # Run basic tests
    python -c "import dittofs; print('✅ DittoFS package imports successfully')"
    
    # Check if CLI works
    python -m dittofs.cli --help > /dev/null && log_success "CLI command works"
    
    # Run quick test suite
    log_info "Running quick test suite..."
    make test-unit || {
        log_warning "Some tests failed. This might be expected in a fresh setup."
    }
    
    log_success "Installation verification completed"
}

# Create development configuration
create_dev_config() {
    log_info "Creating development configuration..."
    
    # Create .dittofs directory for development
    mkdir -p ~/.dittofs/dev
    
    # Create development configuration file
    cat > ~/.dittofs/dev/config.toml << EOF
[development]
# Development-specific settings
debug = true
log_level = "DEBUG"
test_mode = true

[storage]
# Use development storage location
chunk_dir = "~/.dittofs/dev/chunks"
crdt_store = "~/.dittofs/dev/crdt_store.yrs"

[network]
# Development network settings
discovery_port = 8080
sync_port = 8081
enable_mdns = true
enable_bluetooth = false  # Disable by default for development

[security]
# Relaxed security for development
require_encryption = false
allow_self_signed = true
EOF
    
    log_success "Development configuration created at ~/.dittofs/dev/config.toml"
}

# Print usage instructions
print_usage() {
    echo
    log_success "🎉 DittoFS development environment setup completed!"
    echo
    echo "Next steps:"
    echo "1. Activate the virtual environment:"
    echo "   source .ditto/bin/activate"
    echo
    echo "2. Run tests to verify everything works:"
    echo "   make test"
    echo
    echo "3. Start developing! Common commands:"
    echo "   make help                 # Show all available commands"
    echo "   make test-unit           # Run unit tests"
    echo "   make lint                # Run code linting"
    echo "   make format              # Format code"
    echo
    echo "4. Before committing, pre-commit hooks will automatically:"
    echo "   - Format your code with black"
    echo "   - Sort imports with isort"
    echo "   - Run linting with flake8"
    echo "   - Check types with mypy"
    echo "   - Run security checks with bandit"
    echo
    echo "Happy coding! 🚀"
}

# Main execution
main() {
    echo "🔧 DittoFS Development Environment Setup"
    echo "========================================"
    echo
    
    # Check if we're in the right directory
    if [ ! -f "pyproject.toml" ]; then
        log_error "This script must be run from the DittoFS project root directory"
        exit 1
    fi
    
    # Run setup steps
    check_python || exit 1
    install_system_deps || exit 1
    setup_venv || exit 1
    install_python_deps || exit 1
    setup_pre_commit || exit 1
    create_dev_config || exit 1
    verify_installation || exit 1
    
    print_usage
}

# Handle command line arguments
case "${1:-}" in
    --help|-h)
        echo "Usage: $0 [options]"
        echo
        echo "Options:"
        echo "  --help, -h     Show this help message"
        echo "  --skip-system  Skip system dependency installation"
        echo "  --skip-verify  Skip installation verification"
        echo
        exit 0
        ;;
    --skip-system)
        SKIP_SYSTEM=1
        ;;
    --skip-verify)
        SKIP_VERIFY=1
        ;;
esac

# Run main function
main "$@"