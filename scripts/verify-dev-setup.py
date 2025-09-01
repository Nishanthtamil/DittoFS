#!/usr/bin/env python3
"""
Development environment verification script for DittoFS.
This script checks that all development tools and configurations are properly set up.
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path
from typing import List, Tuple, Optional
import importlib.util


class Colors:
    """ANSI color codes for terminal output."""
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[0;34m'
    NC = '\033[0m'  # No Color


class VerificationResult:
    """Result of a verification check."""
    
    def __init__(self, name: str, passed: bool, message: str, suggestion: Optional[str] = None):
        self.name = name
        self.passed = passed
        self.message = message
        self.suggestion = suggestion


class DevSetupVerifier:
    """Verifies development environment setup."""
    
    def __init__(self):
        self.results: List[VerificationResult] = []
        self.project_root = Path.cwd()
    
    def log_info(self, message: str) -> None:
        print(f"{Colors.BLUE}[INFO]{Colors.NC} {message}")
    
    def log_success(self, message: str) -> None:
        print(f"{Colors.GREEN}[SUCCESS]{Colors.NC} {message}")
    
    def log_warning(self, message: str) -> None:
        print(f"{Colors.YELLOW}[WARNING]{Colors.NC} {message}")
    
    def log_error(self, message: str) -> None:
        print(f"{Colors.RED}[ERROR]{Colors.NC} {message}")
    
    def run_command(self, cmd: List[str], capture: bool = True) -> Tuple[bool, str]:
        """Run a command and return success status and output."""
        try:
            if capture:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                return result.returncode == 0, result.stdout + result.stderr
            else:
                result = subprocess.run(cmd, timeout=30)
                return result.returncode == 0, ""
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError, FileNotFoundError):
            return False, "Command failed or timed out"
    
    def check_python_version(self) -> VerificationResult:
        """Check Python version."""
        try:
            version = sys.version_info
            if version.major == 3 and version.minor >= 9:
                return VerificationResult(
                    "Python Version",
                    True,
                    f"Python {version.major}.{version.minor}.{version.micro} (>= 3.9 required)"
                )
            else:
                return VerificationResult(
                    "Python Version",
                    False,
                    f"Python {version.major}.{version.minor}.{version.micro} found, but >= 3.9 required",
                    "Install Python 3.9 or later"
                )
        except Exception as e:
            return VerificationResult(
                "Python Version",
                False,
                f"Failed to check Python version: {e}",
                "Ensure Python is properly installed"
            )
    
    def check_virtual_environment(self) -> VerificationResult:
        """Check if virtual environment is active and properly configured."""
        venv_path = self.project_root / ".ditto"
        
        if not venv_path.exists():
            return VerificationResult(
                "Virtual Environment",
                False,
                "Virtual environment .ditto not found",
                "Run ./scripts/dev-setup.sh to create virtual environment"
            )
        
        # Check if we're in the virtual environment
        if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
            return VerificationResult(
                "Virtual Environment",
                True,
                f"Virtual environment active: {sys.prefix}"
            )
        else:
            return VerificationResult(
                "Virtual Environment",
                False,
                "Virtual environment exists but not activated",
                "Activate with: source .ditto/bin/activate"
            )
    
    def check_dependencies(self) -> VerificationResult:
        """Check if required dependencies are installed."""
        required_packages = [
            'pytest', 'black', 'flake8', 'mypy', 'pre-commit',
            'trio', 'blake3', 'pycrdt', 'bandit', 'safety'
        ]
        
        missing_packages = []
        for package in required_packages:
            spec = importlib.util.find_spec(package.replace('-', '_'))
            if spec is None:
                missing_packages.append(package)
        
        if not missing_packages:
            return VerificationResult(
                "Dependencies",
                True,
                f"All {len(required_packages)} required packages are installed"
            )
        else:
            return VerificationResult(
                "Dependencies",
                False,
                f"Missing packages: {', '.join(missing_packages)}",
                "Run: pip install -e .[dev,all]"
            )
    
    def check_pre_commit_hooks(self) -> VerificationResult:
        """Check if pre-commit hooks are installed."""
        git_hooks_dir = self.project_root / ".git" / "hooks"
        pre_commit_hook = git_hooks_dir / "pre-commit"
        
        if not pre_commit_hook.exists():
            return VerificationResult(
                "Pre-commit Hooks",
                False,
                "Pre-commit hooks not installed",
                "Run: pre-commit install"
            )
        
        # Check if pre-commit config is valid
        success, output = self.run_command(["pre-commit", "validate-config"])
        if success:
            return VerificationResult(
                "Pre-commit Hooks",
                True,
                "Pre-commit hooks installed and configuration is valid"
            )
        else:
            return VerificationResult(
                "Pre-commit Hooks",
                False,
                f"Pre-commit configuration invalid: {output}",
                "Check .pre-commit-config.yaml for errors"
            )
    
    def check_code_formatting(self) -> VerificationResult:
        """Check if code is properly formatted."""
        # Check black formatting
        success, output = self.run_command(["black", "--check", "--diff", "src/", "tests/"])
        if success:
            return VerificationResult(
                "Code Formatting",
                True,
                "Code is properly formatted with black"
            )
        else:
            return VerificationResult(
                "Code Formatting",
                False,
                "Code formatting issues found",
                "Run: black src/ tests/"
            )
    
    def check_import_sorting(self) -> VerificationResult:
        """Check if imports are properly sorted."""
        success, output = self.run_command(["isort", "--check-only", "--diff", "src/", "tests/"])
        if success:
            return VerificationResult(
                "Import Sorting",
                True,
                "Imports are properly sorted with isort"
            )
        else:
            return VerificationResult(
                "Import Sorting",
                False,
                "Import sorting issues found",
                "Run: isort src/ tests/"
            )
    
    def check_linting(self) -> VerificationResult:
        """Check code linting."""
        success, output = self.run_command(["flake8", "src/", "tests/"])
        if success:
            return VerificationResult(
                "Code Linting",
                True,
                "No linting issues found"
            )
        else:
            return VerificationResult(
                "Code Linting",
                False,
                "Linting issues found",
                "Run: flake8 src/ tests/ to see details"
            )
    
    def check_type_checking(self) -> VerificationResult:
        """Check type checking."""
        success, output = self.run_command(["mypy", "src/"])
        if success:
            return VerificationResult(
                "Type Checking",
                True,
                "No type checking issues found"
            )
        else:
            return VerificationResult(
                "Type Checking",
                False,
                "Type checking issues found",
                "Run: mypy src/ to see details"
            )
    
    def check_security_scanning(self) -> VerificationResult:
        """Check security scanning."""
        success, output = self.run_command(["bandit", "-r", "src/", "-f", "txt"])
        if success:
            return VerificationResult(
                "Security Scanning",
                True,
                "No security issues found"
            )
        else:
            return VerificationResult(
                "Security Scanning",
                False,
                "Security issues found",
                "Run: bandit -r src/ for details"
            )
    
    def check_test_framework(self) -> VerificationResult:
        """Check if test framework is working."""
        success, output = self.run_command(["python", "-m", "pytest", "--collect-only", "-q"])
        if success:
            return VerificationResult(
                "Test Framework",
                True,
                "Test framework is working and can collect tests"
            )
        else:
            return VerificationResult(
                "Test Framework",
                False,
                "Test framework issues found",
                "Check test configuration and dependencies"
            )
    
    def check_project_structure(self) -> VerificationResult:
        """Check if project structure is correct."""
        required_files = [
            "pyproject.toml",
            "src/dittofs/__init__.py",
            "tests/conftest.py",
            ".pre-commit-config.yaml",
            "Makefile"
        ]
        
        missing_files = []
        for file_path in required_files:
            if not (self.project_root / file_path).exists():
                missing_files.append(file_path)
        
        if not missing_files:
            return VerificationResult(
                "Project Structure",
                True,
                "All required project files are present"
            )
        else:
            return VerificationResult(
                "Project Structure",
                False,
                f"Missing files: {', '.join(missing_files)}",
                "Ensure you're in the project root directory"
            )
    
    def check_git_configuration(self) -> VerificationResult:
        """Check Git configuration."""
        if not (self.project_root / ".git").exists():
            return VerificationResult(
                "Git Configuration",
                False,
                "Not a Git repository",
                "Initialize with: git init"
            )
        
        # Check if user name and email are configured
        success1, name = self.run_command(["git", "config", "user.name"])
        success2, email = self.run_command(["git", "config", "user.email"])
        
        if success1 and success2 and name.strip() and email.strip():
            return VerificationResult(
                "Git Configuration",
                True,
                f"Git configured for {name.strip()} <{email.strip()}>"
            )
        else:
            return VerificationResult(
                "Git Configuration",
                False,
                "Git user name or email not configured",
                "Configure with: git config user.name 'Your Name' && git config user.email 'your@email.com'"
            )
    
    def run_all_checks(self) -> None:
        """Run all verification checks."""
        checks = [
            self.check_python_version,
            self.check_virtual_environment,
            self.check_dependencies,
            self.check_project_structure,
            self.check_git_configuration,
            self.check_pre_commit_hooks,
            self.check_code_formatting,
            self.check_import_sorting,
            self.check_linting,
            self.check_type_checking,
            self.check_security_scanning,
            self.check_test_framework,
        ]
        
        self.log_info("Running development environment verification...")
        print()
        
        for check in checks:
            result = check()
            self.results.append(result)
            
            if result.passed:
                self.log_success(f"✓ {result.name}: {result.message}")
            else:
                self.log_error(f"✗ {result.name}: {result.message}")
                if result.suggestion:
                    print(f"  💡 Suggestion: {result.suggestion}")
            print()
    
    def print_summary(self) -> None:
        """Print verification summary."""
        passed = sum(1 for r in self.results if r.passed)
        total = len(self.results)
        
        print("=" * 60)
        print(f"VERIFICATION SUMMARY: {passed}/{total} checks passed")
        print("=" * 60)
        
        if passed == total:
            self.log_success("🎉 All checks passed! Your development environment is ready.")
            print()
            print("Next steps:")
            print("1. Run tests: make test")
            print("2. Start developing: make help")
            print("3. Before committing: pre-commit run --all-files")
        else:
            failed = total - passed
            self.log_error(f"❌ {failed} check(s) failed. Please fix the issues above.")
            print()
            print("Quick fixes:")
            for result in self.results:
                if not result.passed and result.suggestion:
                    print(f"• {result.suggestion}")
        
        print()
    
    def run(self) -> bool:
        """Run verification and return success status."""
        # Check if we're in the right directory
        if not (self.project_root / "pyproject.toml").exists():
            self.log_error("This script must be run from the DittoFS project root directory")
            return False
        
        self.run_all_checks()
        self.print_summary()
        
        return all(r.passed for r in self.results)


def main():
    """Main entry point."""
    verifier = DevSetupVerifier()
    success = verifier.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()