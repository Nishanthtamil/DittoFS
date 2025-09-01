# DittoFS Development Environment Setup Script for Windows PowerShell
# Run with: PowerShell -ExecutionPolicy Bypass -File scripts/dev-setup.ps1

param(
    [switch]$SkipSystem,
    [switch]$SkipVerify,
    [switch]$Help
)

# Colors for output
$Red = "Red"
$Green = "Green"
$Yellow = "Yellow"
$Blue = "Blue"

function Write-Info {
    param([string]$Message)
    Write-Host "[INFO] $Message" -ForegroundColor $Blue
}

function Write-Success {
    param([string]$Message)
    Write-Host "[SUCCESS] $Message" -ForegroundColor $Green
}

function Write-Warning {
    param([string]$Message)
    Write-Host "[WARNING] $Message" -ForegroundColor $Yellow
}

function Write-Error {
    param([string]$Message)
    Write-Host "[ERROR] $Message" -ForegroundColor $Red
}

function Test-Command {
    param([string]$Command)
    try {
        Get-Command $Command -ErrorAction Stop | Out-Null
        return $true
    }
    catch {
        return $false
    }
}

function Test-Python {
    Write-Info "Checking Python installation..."
    
    if (Test-Command "python") {
        $pythonVersion = python --version 2>&1
        if ($pythonVersion -match "Python (\d+)\.(\d+)\.(\d+)") {
            $major = [int]$matches[1]
            $minor = [int]$matches[2]
            
            if ($major -eq 3 -and $minor -ge 9) {
                Write-Success "Python $($matches[0]) found (>= 3.9 required)"
                return $true
            }
            else {
                Write-Error "Python $($matches[0]) found, but >= 3.9 is required"
                return $false
            }
        }
    }
    
    Write-Error "Python 3 not found. Please install Python 3.9 or later from https://python.org"
    return $false
}

function Install-SystemDeps {
    Write-Info "Checking system dependencies for Windows..."
    
    # Check for Git
    if (-not (Test-Command "git")) {
        Write-Warning "Git not found. Please install Git from https://git-scm.com/"
        Write-Warning "Git is required for version control and some dependencies"
    }
    else {
        Write-Success "Git found"
    }
    
    # Check for Visual Studio Build Tools (required for some Python packages)
    $vsWhere = "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe"
    if (Test-Path $vsWhere) {
        $buildTools = & $vsWhere -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -format json | ConvertFrom-Json
        if ($buildTools) {
            Write-Success "Visual Studio Build Tools found"
        }
        else {
            Write-Warning "Visual Studio Build Tools not found"
            Write-Warning "Some Python packages may fail to compile"
            Write-Warning "Install from: https://visualstudio.microsoft.com/visual-cpp-build-tools/"
        }
    }
    else {
        Write-Warning "Visual Studio Installer not found"
        Write-Warning "Install Visual Studio Build Tools for C++ compilation support"
    }
    
    # Check for Windows Subsystem for Linux (optional but recommended)
    $wslFeature = Get-WindowsOptionalFeature -Online -FeatureName Microsoft-Windows-Subsystem-Linux -ErrorAction SilentlyContinue
    if ($wslFeature -and $wslFeature.State -eq "Enabled") {
        Write-Success "WSL is available (recommended for development)"
    }
    else {
        Write-Info "WSL not enabled. Consider enabling it for better development experience"
    }
    
    Write-Success "System dependency check completed"
}

function Setup-VirtualEnv {
    Write-Info "Setting up Python virtual environment..."
    
    if (Test-Path ".ditto") {
        Write-Warning "Virtual environment .ditto already exists"
        $response = Read-Host "Do you want to recreate it? (y/N)"
        if ($response -eq "y" -or $response -eq "Y") {
            Remove-Item -Recurse -Force .ditto
        }
        else {
            Write-Info "Using existing virtual environment"
            return
        }
    }
    
    python -m venv .ditto
    
    # Activate virtual environment
    & .\.ditto\Scripts\Activate.ps1
    
    # Upgrade pip and setuptools
    python -m pip install --upgrade pip setuptools wheel
    
    Write-Success "Virtual environment created and activated"
}

function Install-PythonDeps {
    Write-Info "Installing Python dependencies..."
    
    # Ensure we're in the virtual environment
    if (-not $env:VIRTUAL_ENV) {
        Write-Info "Activating virtual environment..."
        & .\.ditto\Scripts\Activate.ps1
    }
    
    # Install development dependencies
    pip install -e .[dev,all]
    
    Write-Success "Python dependencies installed"
}

function Setup-PreCommit {
    Write-Info "Setting up pre-commit hooks..."
    
    # Ensure we're in the virtual environment
    if (-not $env:VIRTUAL_ENV) {
        & .\.ditto\Scripts\Activate.ps1
    }
    
    # Install and setup pre-commit
    pre-commit install
    pre-commit install --hook-type commit-msg
    
    # Run pre-commit on all files to ensure everything works
    Write-Info "Running pre-commit on all files (this may take a while)..."
    try {
        pre-commit run --all-files
    }
    catch {
        Write-Warning "Pre-commit found issues. Running auto-fixes..."
        try {
            pre-commit run --all-files
        }
        catch {
            Write-Warning "Some pre-commit issues remain. This is normal for initial setup."
        }
    }
    
    Write-Success "Pre-commit hooks installed and configured"
}

function Test-Installation {
    Write-Info "Verifying installation..."
    
    # Ensure we're in the virtual environment
    if (-not $env:VIRTUAL_ENV) {
        & .\.ditto\Scripts\Activate.ps1
    }
    
    # Test basic import
    try {
        python -c "import dittofs; print('✅ DittoFS package imports successfully')"
    }
    catch {
        Write-Error "Failed to import DittoFS package"
        return $false
    }
    
    # Check if CLI works
    try {
        python -m dittofs.cli --help | Out-Null
        Write-Success "CLI command works"
    }
    catch {
        Write-Warning "CLI command test failed"
    }
    
    # Run quick test suite
    Write-Info "Running quick test suite..."
    try {
        python tests/test_runner.py unit --verbose
    }
    catch {
        Write-Warning "Some tests failed. This might be expected in a fresh setup."
    }
    
    Write-Success "Installation verification completed"
}

function New-DevConfig {
    Write-Info "Creating development configuration..."
    
    # Create .dittofs directory for development
    $devDir = "$env:USERPROFILE\.dittofs\dev"
    New-Item -ItemType Directory -Force -Path $devDir | Out-Null
    
    # Create development configuration file
    $configContent = @"
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
"@
    
    $configPath = "$devDir\config.toml"
    $configContent | Out-File -FilePath $configPath -Encoding UTF8
    
    Write-Success "Development configuration created at $configPath"
}

function Show-Usage {
    Write-Host ""
    Write-Success "🎉 DittoFS development environment setup completed!"
    Write-Host ""
    Write-Host "Next steps:"
    Write-Host "1. Activate the virtual environment:"
    Write-Host "   .\.ditto\Scripts\Activate.ps1"
    Write-Host ""
    Write-Host "2. Run tests to verify everything works:"
    Write-Host "   python tests/test_runner.py unit"
    Write-Host ""
    Write-Host "3. Start developing! Common commands:"
    Write-Host "   python tests/test_runner.py --help    # Show test runner options"
    Write-Host "   python tests/test_runner.py unit      # Run unit tests"
    Write-Host "   python tests/test_runner.py lint      # Run code linting"
    Write-Host "   python tests/test_runner.py format    # Format code"
    Write-Host ""
    Write-Host "4. Before committing, pre-commit hooks will automatically:"
    Write-Host "   - Format your code with black"
    Write-Host "   - Sort imports with isort"
    Write-Host "   - Run linting with flake8"
    Write-Host "   - Check types with mypy"
    Write-Host "   - Run security checks with bandit"
    Write-Host ""
    Write-Host "Happy coding! 🚀"
}

function Show-Help {
    Write-Host "DittoFS Development Environment Setup for Windows"
    Write-Host "Usage: PowerShell -ExecutionPolicy Bypass -File scripts/dev-setup.ps1 [options]"
    Write-Host ""
    Write-Host "Options:"
    Write-Host "  -Help          Show this help message"
    Write-Host "  -SkipSystem    Skip system dependency checks"
    Write-Host "  -SkipVerify    Skip installation verification"
    Write-Host ""
    Write-Host "Prerequisites:"
    Write-Host "  - Python 3.9 or later"
    Write-Host "  - Git (recommended)"
    Write-Host "  - Visual Studio Build Tools (for C++ compilation)"
    Write-Host ""
}

# Main execution
function Main {
    if ($Help) {
        Show-Help
        return
    }
    
    Write-Host "🔧 DittoFS Development Environment Setup for Windows" -ForegroundColor $Blue
    Write-Host "====================================================" -ForegroundColor $Blue
    Write-Host ""
    
    # Check if we're in the right directory
    if (-not (Test-Path "pyproject.toml")) {
        Write-Error "This script must be run from the DittoFS project root directory"
        exit 1
    }
    
    # Run setup steps
    if (-not (Test-Python)) { exit 1 }
    
    if (-not $SkipSystem) {
        Install-SystemDeps
    }
    
    Setup-VirtualEnv
    Install-PythonDeps
    Setup-PreCommit
    New-DevConfig
    
    if (-not $SkipVerify) {
        Test-Installation
    }
    
    Show-Usage
}

# Run main function
Main