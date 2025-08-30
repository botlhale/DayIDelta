# Contributing to DayIDelta

Thank you for your interest in improving DayIDelta! This guide explains how to set up a development environment, coding standards, testing strategy, and how to submit a great pull request.

## Table of Contents
1. Code of Conduct
2. Architecture Overview
3. Development Environment
4. Installation (Editable Mode)
5. Running Tests
6. Linting & Formatting
7. Commit & Branch Conventions
8. Adding Features / Changing Public API
9. Documentation Guidelines
10. Performance Considerations
11. Release Preparation (Future)
12. Getting Help

## 1. Code of Conduct
Be respectful. Harassment of any kind is not tolerated. (If CODE_OF_CONDUCT.md exists, it governs.)

## 2. Architecture Overview

**DayIDelta uses a modern modular architecture** for maintainability and extensibility:

```
dayidelta/                    # Main package
├── core/                    # Platform-agnostic SCD2 engine
│   ├── scd2_engine.py      # Core SCD2 logic and DayIDelta function
│   ├── models.py           # Data models and configuration classes
│   └── interfaces.py       # Platform adapter interfaces
├── platforms/              # Platform-specific implementations  
│   ├── unity_catalog.py    # Azure Databricks Unity Catalog
│   ├── fabric.py           # Microsoft Fabric
│   └── base.py             # Base platform adapter
├── agents/                 # AI agents and interfaces
│   ├── chatbot.py          # SCD2 AI chatbot for query generation
│   └── cli.py              # Command-line interface
├── query/                  # Query generation and parsing
│   ├── generators.py       # SQL and Python code generators
│   ├── parsers.py          # Natural language query parsing
│   └── patterns.py         # SCD2 query patterns and templates
├── utils/                  # Utilities and validation
│   └── validation.py       # Input validation and SQL sanitization
└── tests/                  # Comprehensive test suite
    ├── test_core/          # Core engine tests
    ├── test_platforms/     # Platform adapter tests
    ├── test_agents/        # Chatbot and CLI tests
    └── test_query/         # Query generation tests
```

**Legacy Structure**: Files like `DayIDelta.py`, `scd2_chatbot.py` in the root directory have been superseded by the modular structure. See [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) for migration instructions.

## 3. Development Environment
Requirements:
- Python 3.9–3.11
- Spark 3.x with Delta Lake (cluster or local)
- Make (optional)
- Virtual environment tool (venv/conda/uv)

## 4. Installation (Editable)
```bash
git clone https://github.com/botlhale/DayIDelta.git
cd DayIDelta
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

**Working with the modular structure:**
```python
# Import from the modular structure
from dayidelta.core.scd2_engine import SCD2Engine, DayIDelta
from dayidelta.agents.chatbot import SCD2Chatbot
from dayidelta.platforms.unity_catalog import UnityCatalogAdapter

# Or use convenient top-level imports
from dayidelta import DayIDelta, SCD2Chatbot, setup_unity_catalog_environment
```

## 5. Running Tests
```bash
# Run all tests (requires PySpark for core/platform tests)
pytest -q

# Run tests that don't require PySpark
pytest dayidelta/tests/test_query/ dayidelta/tests/test_agents/ -v

# Run with coverage
pytest --cov=dayidelta --cov-report=term-missing
```

**Test Organization:**
- `dayidelta/tests/test_core/` - Core SCD2 engine tests
- `dayidelta/tests/test_platforms/` - Platform adapter tests  
- `dayidelta/tests/test_agents/` - Chatbot and CLI tests
- `dayidelta/tests/test_query/` - Query generation tests

## 6. Linting & Formatting
Tools:
- Ruff (lint + some fixes)
- Black (format)
- Mypy (optional future addition)

Commands:
```bash
ruff check .
black .
```

## 7. Commit & Branch Conventions
Branch names:
- feature/<short-description>
- fix/<issue-id-or-short-description>
- docs/<topic>
- chore/<task>

Commit message format (recommended, not enforced):
```
feat(core): add selective expiration filter
fix(chatbot): correct quarter parsing logic
docs: restructure Unity Catalog section
```

## 8. Adding Features / API Changes
1. Open an issue describing proposed change.
2. Discuss design (avoid large unreviewed PRs).
3. Add/update tests.
4. Update relevant docs (README, architecture, chatbot, performance_design).
5. Ensure backward compatibility or note breaking change.

## 9. Documentation Guidelines
- Keep README high-level; deep detail in docs/.
- Provide minimal runnable examples.
- For new chatbot patterns: add to `docs/chatbot.md`.
- For performance-impacting features: update `docs/performance_design.md`.

## 10. Performance Considerations
Benchmark large table operations if logic changes iteration complexity.
Avoid wide shuffles; prefer column pruning; test multi-source scenarios.

## 11. Release Preparation (Future)
When publishing is enabled:
- Update version in pyproject.toml
- Update CHANGELOG.md
- Build & test source/wheel: `python -m build`
- Smoke test install in clean venv

## 12. Getting Help
Open a GitHub discussion or issue with label `question`.
Tag logs or minimal reproducible example when reporting issues.

Thank you for contributing!