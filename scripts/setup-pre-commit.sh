#!/bin/bash
# Pre-commit hooks setup and management script for DittoFS

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

# Check if pre-commit is installed
check_precommit() {
    if ! command -v pre-commit &> /dev/null; then
        log_error "pre-commit is not installed"
        log_info "Installing pre-commit..."
        pip install pre-commit
    fi
    log_success "pre-commit is available"
}

# Install pre-commit hooks
install_hooks() {
    log_info "Installing pre-commit hooks..."
    
    # Install commit hooks
    pre-commit install
    
    # Install commit-msg hooks
    pre-commit install --hook-type commit-msg
    
    # Install pre-push hooks
    pre-commit install --hook-type pre-push
    
    log_success "Pre-commit hooks installed"
}

# Update pre-commit hooks
update_hooks() {
    log_info "Updating pre-commit hooks..."
    pre-commit autoupdate
    log_success "Pre-commit hooks updated"
}

# Run pre-commit on all files
run_all() {
    log_info "Running pre-commit on all files..."
    pre-commit run --all-files
    log_success "Pre-commit checks completed"
}

# Run pre-commit on specific files
run_files() {
    local files="$*"
    log_info "Running pre-commit on files: $files"
    pre-commit run --files $files
}

# Clean pre-commit cache
clean_cache() {
    log_info "Cleaning pre-commit cache..."
    pre-commit clean
    log_success "Pre-commit cache cleaned"
}

# Show pre-commit configuration
show_config() {
    log_info "Pre-commit configuration:"
    pre-commit sample-config
}

# Validate pre-commit configuration
validate_config() {
    log_info "Validating pre-commit configuration..."
    pre-commit validate-config
    log_success "Pre-commit configuration is valid"
}

# Run specific hook
run_hook() {
    local hook_id="$1"
    if [ -z "$hook_id" ]; then
        log_error "Hook ID is required"
        log_info "Available hooks:"
        pre-commit run --all-files --verbose | grep -E "^\[INFO\]" | head -20
        return 1
    fi
    
    log_info "Running hook: $hook_id"
    pre-commit run "$hook_id" --all-files
}

# Show hook status
show_status() {
    log_info "Pre-commit hook status:"
    
    # Check if hooks are installed
    if [ -f ".git/hooks/pre-commit" ]; then
        log_success "Pre-commit hooks are installed"
    else
        log_warning "Pre-commit hooks are not installed"
    fi
    
    # Show installed hooks
    log_info "Configured hooks:"
    grep -E "^\s*-\s*id:" .pre-commit-config.yaml | sed 's/.*id: /  - /'
}

# Bypass pre-commit for emergency commits
bypass_info() {
    log_info "To bypass pre-commit hooks for emergency commits:"
    echo "  git commit --no-verify -m 'emergency commit message'"
    echo ""
    log_warning "Use bypass sparingly and fix issues in follow-up commits"
}

# Show help
show_help() {
    echo "DittoFS Pre-commit Hook Management"
    echo
    echo "Usage: $0 <command> [options]"
    echo
    echo "Commands:"
    echo "  install         Install pre-commit hooks"
    echo "  update          Update pre-commit hooks to latest versions"
    echo "  run-all         Run all hooks on all files"
    echo "  run-files FILES Run hooks on specific files"
    echo "  run-hook HOOK   Run specific hook on all files"
    echo "  clean           Clean pre-commit cache"
    echo "  config          Show pre-commit configuration"
    echo "  validate        Validate pre-commit configuration"
    echo "  status          Show hook installation status"
    echo "  bypass          Show how to bypass hooks for emergency commits"
    echo "  help            Show this help message"
    echo
    echo "Examples:"
    echo "  $0 install                    # Install hooks"
    echo "  $0 run-all                    # Run all hooks"
    echo "  $0 run-files src/file.py      # Run hooks on specific file"
    echo "  $0 run-hook black             # Run only black formatter"
    echo "  $0 update                     # Update hook versions"
    echo
    echo "Common hooks:"
    echo "  black           Code formatting"
    echo "  isort           Import sorting"
    echo "  flake8          Code linting"
    echo "  mypy            Type checking"
    echo "  bandit          Security scanning"
}

# Main execution
main() {
    # Check if we're in a git repository
    if [ ! -d ".git" ]; then
        log_error "This script must be run from a git repository root"
        exit 1
    fi
    
    # Check if pre-commit config exists
    if [ ! -f ".pre-commit-config.yaml" ]; then
        log_error "No .pre-commit-config.yaml found"
        exit 1
    fi
    
    # Handle commands
    case "${1:-help}" in
        install)
            check_precommit
            install_hooks
            ;;
        update)
            check_precommit
            update_hooks
            ;;
        run-all)
            check_precommit
            run_all
            ;;
        run-files)
            shift
            check_precommit
            run_files "$@"
            ;;
        run-hook)
            check_precommit
            run_hook "$2"
            ;;
        clean)
            check_precommit
            clean_cache
            ;;
        config)
            show_config
            ;;
        validate)
            check_precommit
            validate_config
            ;;
        status)
            show_status
            ;;
        bypass)
            bypass_info
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            log_error "Unknown command: $1"
            show_help
            exit 1
            ;;
    esac
}

# Run main function
main "$@"