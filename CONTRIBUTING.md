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
Core SCD2 logic lives in `src/dayidelta/` (core, dimension management, environment helpers).
Chatbot logic in `src/dayidelta/chatbot/` (schema abstraction, parsing, pattern generation).
Docs in `docs/` and examples in `examples/`.

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

## 5. Running Tests
```bash
pytest -q
```
For coverage:
```bash
pytest --cov=dayidelta --cov-report=term-missing
```

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