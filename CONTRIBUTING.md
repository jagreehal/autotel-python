# Contributing to autotel-python

Thank you for your interest in contributing to autotel-python! This document provides guidelines and instructions for contributing.

## Development Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/jagreehal/autotel-python.git
   cd autotel-python
   ```

2. **Install dependencies**
   ```bash
   # Using uv (recommended)
   uv pip install -e ".[dev]"
   
   # Or using pip
   pip install -e ".[dev]"
   ```

3. **Run tests**
   ```bash
   pytest tests/ -v
   ```

## Development Workflow

1. **Create a branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**
   - Follow the existing code style
   - Add tests for new features
   - Update documentation as needed

3. **Run quality checks**
   ```bash
   make quality  # Runs lint, type-check, and tests
   ```

4. **Commit your changes**
   ```bash
   git commit -m "feat: add your feature"
   ```

5. **Push and create a PR**
   ```bash
   git push origin feature/your-feature-name
   ```

## Code Style

- **Formatting**: Use `ruff format` (configured in `pyproject.toml`)
- **Linting**: Use `ruff check` (configured in `pyproject.toml`)
- **Type Hints**: Use full type hints (checked with `mypy`)
- **Line Length**: 100 characters
- **Docstrings**: Use Google-style docstrings

## Testing

- Write tests for all new features
- Aim for >90% code coverage
- Run tests with: `pytest tests/ -v --cov=src`

## Documentation

- Update README.md for user-facing changes
- Update CHANGELOG.md for notable changes
- Add docstrings to all public functions/classes

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation changes
- `test:` Test additions/changes
- `refactor:` Code refactoring
- `chore:` Maintenance tasks

## Pull Request Process

1. Ensure all tests pass
2. Update documentation
3. Add entry to CHANGELOG.md
4. Request review from maintainers

## Questions?

Open an issue or reach out to the maintainers!
