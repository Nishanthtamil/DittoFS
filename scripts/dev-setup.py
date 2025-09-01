#!/usr/bin/env python3
"""
Cross-platform development environment setup script for DittoFS.
This script provides a Python-based alternative to the shell scripts.
"""

import os
import sys
import subprocess
import platform
import shutil
import json
from pathlib import Path
from typing import List, Optional, Tuple


class Colors:
    """ANSI color codes for terminal output."""
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[0;34m'
    NC = '\033[0m'  # No Color


class Logger:
    """Simple logger with colored output."""
    
    @staticmethod
    def info(message: str) -> None:
        print(f"{Colors.BLUE}[INFO]{Colors.NC} {message}")
    
    @staticmethod
    def success(message: str) -> None:
        print(f"{Colors.GREEN}[SUCCESS]{Colors.NC} {message}")
    
    @staticmethod
    def warning(message: str) -> None:
        print(f"{Colors.YELLOW}[WARNING]{Colors.NC} {message}")
    
    @staticmethod
    def error(message: str) -> None:
        print(f"{Colors.RED}[ERROR]{Colors.NC} {message}")


class DevSetup:
    """Development environment setup manager."""
    
    def __init__(self, skip_system: bool = False, skip_verify: bool = False):
        self.skip_system = skip_system
        self.skip_verify = skip_verify
        self.platform = platform.system().lower()
        self.project_root = Path.cwd()
        self.venv_path = self.project_root / ".ditto"
        
    def run_command(self, cmd: List[str], check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
        """Run a command with proper error handling."""
        try:
            if capture:
                return subprocess.run(cmd, check=check, capture_output=True, text=True)
            else:
                return subprocess.run(cmd, check=check)
        except subprocess.CalledProcessError as e:
            Logger.error(f"Command failed: {' '.join(cmd)}")
            if capture and e.stdout:
                Logger.error(f"stdout: {e.stdout}")
            if capture and e.stderr:
                Logger.error(f"stderr: {e.stderr}")
            raise
    
    def command_exists(self, command: str) -> bool:
        """Check if a command exists in PATH."""
        return shutil.which(command) is not None
    
    def check_python(self) -> bool:
        """Check if Python 3.9+ is available."""
        Logger.info("Checking Python installation...")
        
        python_cmd = "python3" if self.command_exists("python3") else "python"
        
        if not self.command_exists(python_cmd):
            Logger.error("Python not found. Please install Python 3.9 or later")
            return False
        
        try:
            result = self.run_command([python_cmd, "--version"], capture=True)
            version_str = result.stdout.strip()
            
            # Parse version
            version_parts = version_str.split()[1].split('.')
            major, minor = int(version_parts[0]), int(version_parts[1])
            
            if major == 3 and minor >= 9:
                Logger.success(f"{version_str} found (>= 3.9 required)")
                return True
            else:
                Logger.error(f"{version_str} found, but >= 3.9 is required")
                return False
                
        except Exception as e:
            Logger.error(f"Failed to check Python version: {e}")
            return False
    
    def install_system_deps(self) -> bool:
        """Install system dependencies based on platform."""
        if self.skip_system:
            Logger.info("Skipping system dependency installation")
            return True
            
        Logger.info(f"Installing system dependencies for {self.platform}...")
        
        try:
            if self.platform == "linux":
                return self._install_linux_deps()
            elif self.platform == "darwin":
                return self._install_macos_deps()
            elif self.platform == "windows":
                return self._install_windows_deps()
            else:
                Logger.error(f"Unsupported platform: {self.platform}")
                return False
        except Exception as e:
            Logger.error(f"Failed to install system dependencies: {e}")
            return False
    
    def _install_linux_deps(self) -> bool:
        """Install Linux system dependencies."""
        deps_apt = [
            "python3-dev", "python3-pip", "python3-venv", "build-essential",
            "libfuse3-dev", "pkg-config", "git", "curl", "bluetooth", "libbluetooth-dev"
        ]
        
        deps_yum = [
            "python3-devel", "python3-pip", "gcc", "gcc-c++", "fuse3-devel",
            "pkgconfig", "git", "curl", "bluez-libs-devel"
        ]
        
        deps_pacman = [
            "python", "python-pip", "base-devel", "fuse3", "pkgconf",
            "git", "curl", "bluez-libs"
        ]
        
        if self.command_exists("apt-get"):
            Logger.info("Using apt-get package manager")
            self.run_command(["sudo", "apt-get", "update"])
            self.run_command(["sudo", "apt-get", "install", "-y"] + deps_apt)
        elif self.command_exists("yum"):
            Logger.info("Using yum package manager")
            self.run_command(["sudo", "yum", "install", "-y"] + deps_yum)
        elif self.command_exists("pacman"):
            Logger.info("Using pacman package manager")
            self.run_command(["sudo", "pacman", "-S", "--noconfirm"] + deps_pacman)
        else:
            Logger.warning("Unknown package manager. Please install dependencies manually:")
            Logger.warning("- Python 3.9+ development headers")
            Logger.warning("- Build tools (gcc, make)")
            Logger.warning("- FUSE3 development libraries")
            Logger.warning("- Bluetooth development libraries")
            return True  # Don't fail, just warn
        
        Logger.success("System dependencies installed")
        return True
    
    def _install_macos_deps(self) -> bool:
        """Install macOS system dependencies."""
        if not self.command_exists("brew"):
            Logger.error("Homebrew not found. Please install Homebrew first:")
            Logger.error("https://brew.sh/")
            return False
        
        Logger.info("Using Homebrew package manager")
        deps = ["python@3.11", "macfuse", "pkg-config", "git", "curl"]
        self.run_command(["brew", "install"] + deps)
        
        Logger.success("System dependencies installed")
        return True
    
    def _install_windows_deps(self) -> bool:
        """Check Windows system dependencies."""
        Logger.info("Checking system dependencies for Windows...")
        
        # Check for Git
        if not self.command_exists("git"):
            Logger.warning("Git not found. Please install Git from https://git-scm.com/")
        else:
            Logger.success("Git found")
        
        # Note: We can't easily install Visual Studio Build Tools automatically
        Logger.warning("Ensure Visual Studio Build Tools are installed for C++ compilation")
        Logger.warning("Download from: https://visualstudio.microsoft.com/visual-cpp-build-tools/")
        
        return True
    
    def setup_venv(self) -> bool:
        """Setup Python virtual environment."""
        Logger.info("Setting up Python virtual environment...")
        
        if self.venv_path.exists():
            Logger.warning(f"Virtual environment {self.venv_path} already exists")
            response = input("Do you want to recreate it? (y/N): ").strip().lower()
            if response == 'y':
                shutil.rmtree(self.venv_path)
            else:
                Logger.info("Using existing virtual environment")
                return True
        
        # Create virtual environment
        python_cmd = "python3" if self.command_exists("python3") else "python"
        self.run_command([python_cmd, "-m", "venv", str(self.venv_path)])
        
        # Get activation script path
        if self.platform == "windows":
            activate_script = self.venv_path / "Scripts" / "activate"
            pip_cmd = str(self.venv_path / "Scripts" / "pip")
        else:
            activate_script = self.venv_path / "bin" / "activate"
            pip_cmd = str(self.venv_path / "bin" / "pip")
        
        # Upgrade pip and setuptools
        self.run_command([pip_cmd, "install", "--upgrade", "pip", "setuptools", "wheel"])
        
        Logger.success("Virtual environment created")
        return True
    
    def install_python_deps(self) -> bool:
        """Install Python dependencies."""
        Logger.info("Installing Python dependencies...")
        
        # Get pip path
        if self.platform == "windows":
            pip_cmd = str(self.venv_path / "Scripts" / "pip")
        else:
            pip_cmd = str(self.venv_path / "bin" / "pip")
        
        # Install development dependencies
        self.run_command([pip_cmd, "install", "-e", ".[dev,all]"])
        
        Logger.success("Python dependencies installed")
        return True
    
    def setup_pre_commit(self) -> bool:
        """Setup pre-commit hooks."""
        Logger.info("Setting up pre-commit hooks...")
        
        # Get pre-commit path
        if self.platform == "windows":
            precommit_cmd = str(self.venv_path / "Scripts" / "pre-commit")
        else:
            precommit_cmd = str(self.venv_path / "bin" / "pre-commit")
        
        # Install pre-commit hooks
        self.run_command([precommit_cmd, "install"])
        self.run_command([precommit_cmd, "install", "--hook-type", "commit-msg"])
        
        # Run pre-commit on all files
        Logger.info("Running pre-commit on all files (this may take a while)...")
        try:
            self.run_command([precommit_cmd, "run", "--all-files"])
        except subprocess.CalledProcessError:
            Logger.warning("Pre-commit found issues. Running auto-fixes...")
            try:
                self.run_command([precommit_cmd, "run", "--all-files"], check=False)
            except:
                pass  # Ignore failures on second run
        
        Logger.success("Pre-commit hooks installed and configured")
        return True
    
    def create_dev_config(self) -> bool:
        """Create development configuration."""
        Logger.info("Creating development configuration...")
        
        # Create .dittofs directory for development
        if self.platform == "windows":
            dev_dir = Path.home() / ".dittofs" / "dev"
        else:
            dev_dir = Path.home() / ".dittofs" / "dev"
        
        dev_dir.mkdir(parents=True, exist_ok=True)
        
        # Create development configuration file
        config_content = """[development]
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
"""
        
        config_path = dev_dir / "config.toml"
        config_path.write_text(config_content)
        
        Logger.success(f"Development configuration created at {config_path}")
        return True
    
    def verify_installation(self) -> bool:
        """Verify the installation."""
        if self.skip_verify:
            Logger.info("Skipping installation verification")
            return True
            
        Logger.info("Verifying installation...")
        
        # Get python path
        if self.platform == "windows":
            python_cmd = str(self.venv_path / "Scripts" / "python")
        else:
            python_cmd = str(self.venv_path / "bin" / "python")
        
        # Test basic import
        try:
            self.run_command([python_cmd, "-c", "import dittofs; print('✅ DittoFS package imports successfully')"])
        except subprocess.CalledProcessError:
            Logger.error("Failed to import DittoFS package")
            return False
        
        # Check if CLI works
        try:
            self.run_command([python_cmd, "-m", "dittofs.cli", "--help"], capture=True)
            Logger.success("CLI command works")
        except subprocess.CalledProcessError:
            Logger.warning("CLI command test failed")
        
        # Run quick test suite
        Logger.info("Running quick test suite...")
        try:
            self.run_command([python_cmd, "tests/test_runner.py", "unit", "--verbose"])
        except subprocess.CalledProcessError:
            Logger.warning("Some tests failed. This might be expected in a fresh setup.")
        
        Logger.success("Installation verification completed")
        return True
    
    def print_usage(self) -> None:
        """Print usage instructions."""
        print()
        Logger.success("🎉 DittoFS development environment setup completed!")
        print()
        print("Next steps:")
        
        if self.platform == "windows":
            print("1. Activate the virtual environment:")
            print("   .ditto\\Scripts\\activate")
        else:
            print("1. Activate the virtual environment:")
            print("   source .ditto/bin/activate")
        
        print()
        print("2. Run tests to verify everything works:")
        print("   python tests/test_runner.py unit")
        print()
        print("3. Start developing! Common commands:")
        print("   python tests/test_runner.py --help    # Show test runner options")
        print("   python tests/test_runner.py unit      # Run unit tests")
        print("   python tests/test_runner.py lint      # Run code linting")
        print("   python tests/test_runner.py format    # Format code")
        print()
        print("4. Before committing, pre-commit hooks will automatically:")
        print("   - Format your code with black")
        print("   - Sort imports with isort")
        print("   - Run linting with flake8")
        print("   - Check types with mypy")
        print("   - Run security checks with bandit")
        print()
        print("Happy coding! 🚀")
    
    def run(self) -> bool:
        """Run the complete setup process."""
        print("🔧 DittoFS Development Environment Setup")
        print("========================================")
        print()
        
        # Check if we're in the right directory
        if not (self.project_root / "pyproject.toml").exists():
            Logger.error("This script must be run from the DittoFS project root directory")
            return False
        
        # Run setup steps
        steps = [
            ("Checking Python", self.check_python),
            ("Installing system dependencies", self.install_system_deps),
            ("Setting up virtual environment", self.setup_venv),
            ("Installing Python dependencies", self.install_python_deps),
            ("Setting up pre-commit hooks", self.setup_pre_commit),
            ("Creating development configuration", self.create_dev_config),
            ("Verifying installation", self.verify_installation),
        ]
        
        for step_name, step_func in steps:
            try:
                if not step_func():
                    Logger.error(f"Failed: {step_name}")
                    return False
            except KeyboardInterrupt:
                Logger.error("Setup interrupted by user")
                return False
            except Exception as e:
                Logger.error(f"Unexpected error in {step_name}: {e}")
                return False
        
        self.print_usage()
        return True


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="DittoFS Development Environment Setup")
    parser.add_argument("--skip-system", action="store_true", 
                       help="Skip system dependency installation")
    parser.add_argument("--skip-verify", action="store_true",
                       help="Skip installation verification")
    
    args = parser.parse_args()
    
    setup = DevSetup(skip_system=args.skip_system, skip_verify=args.skip_verify)
    
    try:
        success = setup.run()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        Logger.error("Setup interrupted by user")
        sys.exit(1)
    except Exception as e:
        Logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()