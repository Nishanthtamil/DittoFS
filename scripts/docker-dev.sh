#!/bin/bash
# Docker development environment management script for DittoFS

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

# Check if Docker is available
check_docker() {
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed or not in PATH"
        log_error "Please install Docker: https://docs.docker.com/get-docker/"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose is not installed or not in PATH"
        log_error "Please install Docker Compose: https://docs.docker.com/compose/install/"
        exit 1
    fi
    
    # Check if Docker daemon is running
    if ! docker info &> /dev/null; then
        log_error "Docker daemon is not running"
        log_error "Please start Docker daemon"
        exit 1
    fi
    
    log_success "Docker and Docker Compose are available"
}

# Build development image
build_dev() {
    log_info "Building DittoFS development image..."
    docker-compose -f docker-compose.dev.yml build dittofs-dev
    log_success "Development image built successfully"
}

# Start development environment
start_dev() {
    log_info "Starting DittoFS development environment..."
    docker-compose -f docker-compose.dev.yml up -d dittofs-dev test-db test-redis test-mqtt
    log_success "Development environment started"
    
    log_info "Waiting for services to be ready..."
    sleep 5
    
    log_info "Development environment is ready!"
    log_info "Access the development container with: $0 shell"
    log_info "View logs with: $0 logs"
}

# Stop development environment
stop_dev() {
    log_info "Stopping DittoFS development environment..."
    docker-compose -f docker-compose.dev.yml down
    log_success "Development environment stopped"
}

# Restart development environment
restart_dev() {
    log_info "Restarting DittoFS development environment..."
    stop_dev
    start_dev
}

# Open shell in development container
shell_dev() {
    log_info "Opening shell in development container..."
    docker-compose -f docker-compose.dev.yml exec dittofs-dev /bin/bash
}

# Show logs
logs_dev() {
    if [ -n "$1" ]; then
        docker-compose -f docker-compose.dev.yml logs -f "$1"
    else
        docker-compose -f docker-compose.dev.yml logs -f
    fi
}

# Run tests in container
test_dev() {
    log_info "Running tests in development container..."
    docker-compose -f docker-compose.dev.yml exec dittofs-dev python tests/test_runner.py "${@:-unit}"
}

# Run linting in container
lint_dev() {
    log_info "Running linting in development container..."
    docker-compose -f docker-compose.dev.yml exec dittofs-dev python tests/test_runner.py lint
}

# Format code in container
format_dev() {
    log_info "Formatting code in development container..."
    docker-compose -f docker-compose.dev.yml exec dittofs-dev python tests/test_runner.py format
}

# Start multi-peer testing environment
start_testing() {
    log_info "Starting multi-peer testing environment..."
    docker-compose -f docker-compose.dev.yml --profile testing up -d
    log_success "Multi-peer testing environment started"
    
    log_info "Available peers:"
    log_info "- dittofs-peer1 (ports 8080-8081)"
    log_info "- dittofs-peer2 (ports 8083-8084)"
    log_info "- dittofs-peer3 (ports 8085-8086)"
}

# Stop multi-peer testing environment
stop_testing() {
    log_info "Stopping multi-peer testing environment..."
    docker-compose -f docker-compose.dev.yml --profile testing down
    log_success "Multi-peer testing environment stopped"
}

# Clean up Docker resources
clean_dev() {
    log_info "Cleaning up Docker resources..."
    
    # Stop and remove containers
    docker-compose -f docker-compose.dev.yml down --volumes --remove-orphans
    
    # Remove development image
    docker rmi dittofs-dev 2>/dev/null || true
    
    # Clean up unused Docker resources
    docker system prune -f
    
    log_success "Docker resources cleaned up"
}

# Show status of development environment
status_dev() {
    log_info "Development environment status:"
    docker-compose -f docker-compose.dev.yml ps
}

# Show help
show_help() {
    echo "DittoFS Docker Development Environment Manager"
    echo
    echo "Usage: $0 <command> [options]"
    echo
    echo "Commands:"
    echo "  build       Build the development Docker image"
    echo "  start       Start the development environment"
    echo "  stop        Stop the development environment"
    echo "  restart     Restart the development environment"
    echo "  shell       Open a shell in the development container"
    echo "  logs [svc]  Show logs (optionally for specific service)"
    echo "  test [args] Run tests in the container"
    echo "  lint        Run linting in the container"
    echo "  format      Format code in the container"
    echo "  testing     Start multi-peer testing environment"
    echo "  stop-test   Stop multi-peer testing environment"
    echo "  status      Show status of development environment"
    echo "  clean       Clean up Docker resources"
    echo "  help        Show this help message"
    echo
    echo "Examples:"
    echo "  $0 start                    # Start development environment"
    echo "  $0 shell                    # Open shell in dev container"
    echo "  $0 test unit               # Run unit tests"
    echo "  $0 test integration        # Run integration tests"
    echo "  $0 logs dittofs-dev        # Show logs for main container"
    echo "  $0 testing                 # Start multi-peer testing"
}

# Main execution
main() {
    # Check if we're in the right directory
    if [ ! -f "docker-compose.dev.yml" ]; then
        log_error "This script must be run from the DittoFS project root directory"
        exit 1
    fi
    
    # Check Docker availability
    check_docker
    
    # Handle commands
    case "${1:-help}" in
        build)
            build_dev
            ;;
        start)
            start_dev
            ;;
        stop)
            stop_dev
            ;;
        restart)
            restart_dev
            ;;
        shell)
            shell_dev
            ;;
        logs)
            logs_dev "$2"
            ;;
        test)
            shift
            test_dev "$@"
            ;;
        lint)
            lint_dev
            ;;
        format)
            format_dev
            ;;
        testing)
            start_testing
            ;;
        stop-test)
            stop_testing
            ;;
        status)
            status_dev
            ;;
        clean)
            clean_dev
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