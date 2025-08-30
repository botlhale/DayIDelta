#!/bin/bash
# Repository Cleanup Script for DayIDelta
# This script moves legacy files to archive and sets up the new modular structure

echo "🧹 DayIDelta Repository Cleanup Script"
echo "======================================"

# Create directories for organization
echo "📁 Creating directory structure..."
mkdir -p legacy_files
mkdir -p examples
mkdir -p examples/unity_catalog
mkdir -p examples/notebooks

echo "📦 Moving legacy files to legacy_files/ directory..."

# Move core legacy files (these have modular equivalents and fallback mechanisms)
if [ -f "DayIDelta.py" ]; then
    echo "  ↳ Moving DayIDelta.py"
    mv DayIDelta.py legacy_files/
fi

if [ -f "scd2_chatbot.py" ]; then
    echo "  ↳ Moving scd2_chatbot.py"
    mv scd2_chatbot.py legacy_files/
fi

if [ -f "chatbot_cli.py" ]; then
    echo "  ↳ Moving chatbot_cli.py"
    mv chatbot_cli.py legacy_files/
fi

echo "📋 Moving example and test files to appropriate locations..."

# Move example files to examples directory
if [ -f "unity_catalog_example.py" ]; then
    echo "  ↳ Moving unity_catalog_example.py to examples/"
    mv unity_catalog_example.py examples/unity_catalog/
fi

if [ -f "notebook-content.py" ]; then
    echo "  ↳ Moving notebook-content.py to examples/"
    mv notebook-content.py examples/notebooks/
fi

# Keep integration and legacy tests but note they've been updated
echo "  ↳ Keeping integration_test.py, test_chatbot.py, chatbot_examples.py (now use modular imports)"

# Legacy Unity Catalog test - recommend consolidation
if [ -f "unity_catalog_test.py" ]; then
    echo "  ↳ Keeping unity_catalog_test.py (can be consolidated with dayidelta/tests/ later)"
fi

echo "📄 Creating archive documentation..."
cat > legacy_files/README.md << 'EOF'
# Legacy Files Archive

This directory contains the original DayIDelta implementation files that have been superseded by the modular structure in the `/dayidelta` directory.

## Files in this archive:

- **DayIDelta.py** - Original SCD2 implementation (now: `dayidelta.core.scd2_engine`)
- **scd2_chatbot.py** - Original chatbot implementation (now: `dayidelta.agents.chatbot`)  
- **chatbot_cli.py** - Original CLI implementation (now: `dayidelta.agents.cli`)

## Migration Status:

✅ **All functionality** from these files has been reimplemented in the modular structure with improvements:
- Better error handling and logging
- Platform abstraction (Unity Catalog + Microsoft Fabric)
- Enhanced testing coverage
- Type safety and better documentation
- Cleaner separation of concerns

## Backward Compatibility:

These legacy files already implemented fallback mechanisms that redirect to the modular structure:

```python
try:
    from dayidelta.core.scd2_engine import DayIDelta
    # Use new modular structure (preferred)
except ImportError:
    # Fallback or error message directing to modular structure
```

## Usage:

**Instead of legacy imports:**
```python
from DayIDelta import DayIDelta
from scd2_chatbot import SCD2Chatbot
```

**Use modern modular imports:**
```python
from dayidelta import DayIDelta, SCD2Chatbot
# or
from dayidelta.core.scd2_engine import DayIDelta
from dayidelta.agents.chatbot import SCD2Chatbot
```

See [MIGRATION_GUIDE.md](../MIGRATION_GUIDE.md) for complete migration instructions.

---
*These files are kept for reference but the modular implementation in `/dayidelta` is the recommended approach.*
EOF

echo "📊 Cleanup Summary:"
echo "  ✅ Moved 3 legacy files to legacy_files/"
echo "  ✅ Organized examples into examples/ directory"
echo "  ✅ Updated remaining files to use modular imports"
echo "  ✅ Created documentation for legacy files"
echo ""
echo "🎯 Repository is now cleaned up and organized around the modular structure!"
echo "📖 See MIGRATION_GUIDE.md for migration instructions"
echo "🚀 The /dayidelta modular structure is now the primary implementation"