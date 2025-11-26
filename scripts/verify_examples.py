#!/usr/bin/env python3
"""Simple verification script to check examples have correct syntax and structure."""

import ast
import sys
from pathlib import Path

# Get project root (parent of scripts directory)
PROJECT_ROOT = Path(__file__).parent.parent


def check_syntax(file_path) -> None:
    """Check if a Python file has valid syntax."""
    try:
        with open(file_path, encoding='utf-8') as f:
            source = f.read()
        ast.parse(source, filename=str(file_path))
        return True, None
    except SyntaxError as e:
        return False, f"Syntax error: {e}"
    except Exception as e:
        return False, f"Error: {e}"

def verify_examples() -> None:
    """Verify all example files."""
    examples_dir = PROJECT_ROOT / "examples"

    if not examples_dir.exists():
        print("❌ Examples directory not found")
        return False

    print("=" * 60)
    print("Verifying example files")
    print("=" * 60 + "\n")

    all_passed = True
    example_files = list(examples_dir.rglob("*.py"))

    for example_file in sorted(example_files):
        # Skip __pycache__ and test files
        if "__pycache__" in str(example_file) or "test" in example_file.name.lower():
            continue

        print(f"Checking {example_file.relative_to(PROJECT_ROOT)}...", end=" ")
        is_valid, error = check_syntax(example_file)

        if is_valid:
            print("✅")
        else:
            print(f"❌ {error}")
            all_passed = False

    print("\n" + "=" * 60)
    if all_passed:
        print("✅ All example files have valid syntax!")
    else:
        print("❌ Some example files have syntax errors")
    print("=" * 60)

    return all_passed

def verify_imports() -> None:
    """Verify that imports in examples reference correct modules."""
    examples_dir = PROJECT_ROOT / "examples"

    print("\n" + "=" * 60)
    print("Verifying import paths")
    print("=" * 60 + "\n")

    # Check for old events_adapters imports (should be subscribers now)
    old_imports_found = []
    for example_file in examples_dir.rglob("*.py"):
        if "__pycache__" in str(example_file):
            continue

        try:
            with open(example_file, encoding='utf-8') as f:
                content = f.read()
                if "events_adapters" in content:
                    old_imports_found.append(str(example_file.relative_to(PROJECT_ROOT)))
        except Exception:
            pass

    if old_imports_found:
        print("❌ Found old import paths (events_adapters):")
        for path in old_imports_found:
            print(f"   - {path}")
        print("\n   Update to use: from autotel.subscribers import ...")
        return False
    else:
        print("✅ All import paths are correct (using subscribers)")
        return True

if __name__ == "__main__":
    syntax_ok = verify_examples()
    imports_ok = verify_imports()

    if syntax_ok and imports_ok:
        print("\n✅ All verifications passed!")
        sys.exit(0)
    else:
        print("\n❌ Some verifications failed")
        sys.exit(1)


