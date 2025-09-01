#!/usr/bin/env python3
"""
Comprehensive test runner for DittoFS with various test categories and reporting.

This script provides a convenient way to run different categories of tests
with appropriate configurations and reporting.
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Dict, Any, Optional


class TestRunner:
    """Test runner with support for different test categories and configurations."""
    
    def __init__(self):
        self.project_root = Path(__file__).parent.parent
        self.test_dir = self.project_root / "tests"
        self.coverage_dir = self.project_root / "htmlcov"
        
    def run_command(self, cmd: List[str], **kwargs) -> subprocess.CompletedProcess:
        """Run a command and return the result."""
        print(f"Running: {' '.join(cmd)}")
        return subprocess.run(cmd, **kwargs)
    
    def run_unit_tests(self, verbose: bool = False, coverage: bool = True) -> bool:
        """Run unit tests."""
        cmd = ["python", "-m", "pytest"]
        
        if verbose:
            cmd.append("-v")
        
        if coverage:
            cmd.extend([
                "--cov=src/dittofs",
                "--cov-report=term-missing",
                "--cov-report=html"
            ])
        
        cmd.extend([
            "-m", "unit",
            "tests/"
        ])
        
        result = self.run_command(cmd, cwd=self.project_root)
        return result.returncode == 0
    
    def run_integration_tests(self, verbose: bool = False) -> bool:
        """Run integration tests."""
        cmd = ["python", "-m", "pytest"]
        
        if verbose:
            cmd.append("-v")
        
        cmd.extend([
            "-m", "integration",
            "--timeout=300",  # 5 minute timeout for integration tests
            "tests/"
        ])
        
        result = self.run_command(cmd, cwd=self.project_root)
        return result.returncode == 0
    
    def run_property_tests(self, verbose: bool = False, examples: int = 100) -> bool:
        """Run property-based tests with Hypothesis."""
        cmd = ["python", "-m", "pytest"]
        
        if verbose:
            cmd.append("-v")
        
        cmd.extend([
            "-m", "property",
            f"--hypothesis-show-statistics",
            "tests/"
        ])
        
        # Set Hypothesis configuration via environment
        env = os.environ.copy()
        env["HYPOTHESIS_MAX_EXAMPLES"] = str(examples)
        
        result = self.run_command(cmd, cwd=self.project_root, env=env)
        return result.returncode == 0
    
    def run_performance_tests(self, verbose: bool = False) -> bool:
        """Run performance and benchmark tests."""
        cmd = ["python", "-m", "pytest"]
        
        if verbose:
            cmd.append("-v")
        
        cmd.extend([
            "-m", "benchmark",
            "--benchmark-only",
            "--benchmark-sort=mean",
            "tests/"
        ])
        
        result = self.run_command(cmd, cwd=self.project_root)
        return result.returncode == 0
    
    def run_security_tests(self, verbose: bool = False) -> bool:
        """Run security-related tests."""
        cmd = ["python", "-m", "pytest"]
        
        if verbose:
            cmd.append("-v")
        
        cmd.extend([
            "-m", "security",
            "tests/"
        ])
        
        result = self.run_command(cmd, cwd=self.project_root)
        return result.returncode == 0
    
    def run_network_tests(self, verbose: bool = False) -> bool:
        """Run network-related tests."""
        cmd = ["python", "-m", "pytest"]
        
        if verbose:
            cmd.append("-v")
        
        cmd.extend([
            "-m", "network",
            "tests/"
        ])
        
        result = self.run_command(cmd, cwd=self.project_root)
        return result.returncode == 0
    
    def run_slow_tests(self, verbose: bool = False) -> bool:
        """Run slow tests that are normally skipped."""
        cmd = ["python", "-m", "pytest"]
        
        if verbose:
            cmd.append("-v")
        
        cmd.extend([
            "-m", "slow",
            "--timeout=600",  # 10 minute timeout for slow tests
            "tests/"
        ])
        
        result = self.run_command(cmd, cwd=self.project_root)
        return result.returncode == 0
    
    def run_all_tests(self, verbose: bool = False, coverage: bool = True) -> bool:
        """Run all tests except slow ones."""
        cmd = ["python", "-m", "pytest"]
        
        if verbose:
            cmd.append("-v")
        
        if coverage:
            cmd.extend([
                "--cov=src/dittofs",
                "--cov-report=term-missing",
                "--cov-report=html",
                "--cov-report=xml"
            ])
        
        cmd.extend([
            "-m", "not slow",
            "tests/"
        ])
        
        result = self.run_command(cmd, cwd=self.project_root)
        return result.returncode == 0
    
    def run_quick_tests(self, verbose: bool = False) -> bool:
        """Run quick tests for development feedback."""
        cmd = ["python", "-m", "pytest"]
        
        if verbose:
            cmd.append("-v")
        
        cmd.extend([
            "-m", "not slow and not integration",
            "--maxfail=5",  # Stop after 5 failures
            "tests/"
        ])
        
        result = self.run_command(cmd, cwd=self.project_root)
        return result.returncode == 0
    
    def run_parallel_tests(self, workers: int = 4, verbose: bool = False) -> bool:
        """Run tests in parallel using pytest-xdist."""
        cmd = ["python", "-m", "pytest"]
        
        if verbose:
            cmd.append("-v")
        
        cmd.extend([
            f"-n{workers}",
            "-m", "not slow",
            "tests/"
        ])
        
        result = self.run_command(cmd, cwd=self.project_root)
        return result.returncode == 0
    
    def run_specific_test(self, test_path: str, verbose: bool = False) -> bool:
        """Run a specific test file or test function."""
        cmd = ["python", "-m", "pytest"]
        
        if verbose:
            cmd.append("-v")
        
        cmd.append(test_path)
        
        result = self.run_command(cmd, cwd=self.project_root)
        return result.returncode == 0
    
    def lint_code(self) -> bool:
        """Run code linting."""
        print("Running code linting...")
        
        # Run black
        black_result = self.run_command([
            "python", "-m", "black", "--check", "src/", "tests/"
        ], cwd=self.project_root)
        
        # Run flake8
        flake8_result = self.run_command([
            "python", "-m", "flake8", "src/", "tests/"
        ], cwd=self.project_root)
        
        # Run mypy
        mypy_result = self.run_command([
            "python", "-m", "mypy", "src/"
        ], cwd=self.project_root)
        
        return all(r.returncode == 0 for r in [black_result, flake8_result, mypy_result])
    
    def format_code(self) -> bool:
        """Format code using black."""
        print("Formatting code...")
        
        result = self.run_command([
            "python", "-m", "black", "src/", "tests/"
        ], cwd=self.project_root)
        
        return result.returncode == 0
    
    def generate_coverage_report(self) -> bool:
        """Generate detailed coverage report."""
        print("Generating coverage report...")
        
        # Run tests with coverage
        test_result = self.run_command([
            "python", "-m", "pytest",
            "--cov=src/dittofs",
            "--cov-report=html",
            "--cov-report=xml",
            "--cov-report=term",
            "tests/"
        ], cwd=self.project_root)
        
        if test_result.returncode == 0:
            print(f"Coverage report generated in {self.coverage_dir}")
            return True
        
        return False
    
    def clean_test_artifacts(self):
        """Clean up test artifacts and cache files."""
        print("Cleaning test artifacts...")
        
        # Remove pytest cache
        pytest_cache = self.project_root / ".pytest_cache"
        if pytest_cache.exists():
            subprocess.run(["rm", "-rf", str(pytest_cache)])
        
        # Remove coverage files
        coverage_files = [
            self.project_root / ".coverage",
            self.coverage_dir,
            self.project_root / "coverage.xml"
        ]
        
        for file_path in coverage_files:
            if file_path.exists():
                if file_path.is_file():
                    file_path.unlink()
                else:
                    subprocess.run(["rm", "-rf", str(file_path)])
        
        # Remove __pycache__ directories
        for pycache in self.project_root.rglob("__pycache__"):
            subprocess.run(["rm", "-rf", str(pycache)])
        
        print("Test artifacts cleaned.")
    
    def install_test_dependencies(self) -> bool:
        """Install test dependencies."""
        print("Installing test dependencies...")
        
        result = self.run_command([
            "pip", "install", "-e", ".[dev]"
        ], cwd=self.project_root)
        
        return result.returncode == 0
    
    def run_test_suite(self, suite_name: str, **kwargs) -> bool:
        """Run a predefined test suite."""
        suites = {
            "unit": self.run_unit_tests,
            "integration": self.run_integration_tests,
            "property": self.run_property_tests,
            "performance": self.run_performance_tests,
            "security": self.run_security_tests,
            "network": self.run_network_tests,
            "slow": self.run_slow_tests,
            "all": self.run_all_tests,
            "quick": self.run_quick_tests,
            "parallel": self.run_parallel_tests,
        }
        
        if suite_name not in suites:
            print(f"Unknown test suite: {suite_name}")
            print(f"Available suites: {', '.join(suites.keys())}")
            return False
        
        return suites[suite_name](**kwargs)


def main():
    """Main entry point for the test runner."""
    parser = argparse.ArgumentParser(description="DittoFS Test Runner")
    
    parser.add_argument(
        "command",
        choices=[
            "unit", "integration", "property", "performance", "security",
            "network", "slow", "all", "quick", "parallel", "specific",
            "lint", "format", "coverage", "clean", "install"
        ],
        help="Test command to run"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output"
    )
    
    parser.add_argument(
        "--no-coverage",
        action="store_true",
        help="Disable coverage reporting"
    )
    
    parser.add_argument(
        "--workers", "-n",
        type=int,
        default=4,
        help="Number of parallel workers (for parallel command)"
    )
    
    parser.add_argument(
        "--examples",
        type=int,
        default=100,
        help="Number of examples for property-based tests"
    )
    
    parser.add_argument(
        "--test-path",
        help="Specific test path (for specific command)"
    )
    
    args = parser.parse_args()
    
    runner = TestRunner()
    
    start_time = time.time()
    success = False
    
    try:
        if args.command == "unit":
            success = runner.run_unit_tests(
                verbose=args.verbose,
                coverage=not args.no_coverage
            )
        elif args.command == "integration":
            success = runner.run_integration_tests(verbose=args.verbose)
        elif args.command == "property":
            success = runner.run_property_tests(
                verbose=args.verbose,
                examples=args.examples
            )
        elif args.command == "performance":
            success = runner.run_performance_tests(verbose=args.verbose)
        elif args.command == "security":
            success = runner.run_security_tests(verbose=args.verbose)
        elif args.command == "network":
            success = runner.run_network_tests(verbose=args.verbose)
        elif args.command == "slow":
            success = runner.run_slow_tests(verbose=args.verbose)
        elif args.command == "all":
            success = runner.run_all_tests(
                verbose=args.verbose,
                coverage=not args.no_coverage
            )
        elif args.command == "quick":
            success = runner.run_quick_tests(verbose=args.verbose)
        elif args.command == "parallel":
            success = runner.run_parallel_tests(
                workers=args.workers,
                verbose=args.verbose
            )
        elif args.command == "specific":
            if not args.test_path:
                print("--test-path is required for specific command")
                sys.exit(1)
            success = runner.run_specific_test(args.test_path, verbose=args.verbose)
        elif args.command == "lint":
            success = runner.lint_code()
        elif args.command == "format":
            success = runner.format_code()
        elif args.command == "coverage":
            success = runner.generate_coverage_report()
        elif args.command == "clean":
            runner.clean_test_artifacts()
            success = True
        elif args.command == "install":
            success = runner.install_test_dependencies()
        
    except KeyboardInterrupt:
        print("\nTest run interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error running tests: {e}")
        sys.exit(1)
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"\nTest run completed in {duration:.2f} seconds")
    
    if success:
        print("✅ All tests passed!")
        sys.exit(0)
    else:
        print("❌ Some tests failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()