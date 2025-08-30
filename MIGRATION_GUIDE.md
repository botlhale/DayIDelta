# Migration Guide: Root Files to Modular Structure

This guide explains how to migrate from the legacy root-level files to the new modular DayIDelta structure.

## 🎯 Why Migrate?

The modular structure provides:
- ✅ **Better maintainability** - Clean separation of concerns
- ✅ **Enhanced functionality** - Additional platform support (Fabric + Unity Catalog)
- ✅ **Improved testing** - Comprehensive test coverage (12+ test modules)
- ✅ **Better error handling** - Robust error management and logging
- ✅ **Type safety** - Enhanced type hints and data models
- ✅ **Future-proof** - Designed for easy extensibility

## 📦 Import Migration Map

### Core SCD2 Functionality

**Before (Legacy):**
```python
from DayIDelta import DayIDelta, setup_unity_catalog_environment
```

**After (Modular):**
```python
# Option 1: Use convenient top-level imports (recommended)
from dayidelta import DayIDelta, setup_unity_catalog_environment

# Option 2: Use specific modular imports (for advanced usage)
from dayidelta.core.scd2_engine import DayIDelta, SCD2Engine
from dayidelta.platforms.unity_catalog import setup_unity_catalog_environment
```

### AI Chatbot Functionality

**Before (Legacy):**
```python
from scd2_chatbot import SCD2Chatbot, TableSchema, quick_query
```

**After (Modular):**
```python
# Option 1: Use convenient top-level imports (recommended)
from dayidelta import SCD2Chatbot
from dayidelta.core.models import TableSchema

# Option 2: Use specific modular imports  
from dayidelta.agents.chatbot import SCD2Chatbot, quick_query
from dayidelta.core.models import TableSchema, QueryType, QueryRequest
```

### CLI Interface

**Before (Legacy):**
```bash
python chatbot_cli.py catalog schema table "key1,key2" "tracked1,tracked2" "query"
```

**After (Modular):**
```bash
# Option 1: Use the modular CLI directly
python -m dayidelta.agents.cli catalog schema table "key1,key2" "tracked1,tracked2" "query"

# Option 2: The legacy script now redirects to modular structure automatically
python chatbot_cli.py catalog schema table "key1,key2" "tracked1,tracked2" "query"
```

## 🔄 Backward Compatibility

**Good News!** The legacy files already implement automatic fallback:

```python
# This pattern is already implemented in legacy files
try:
    from dayidelta.core.scd2_engine import DayIDelta
    # Use modular structure (preferred)
except ImportError:
    # Fallback to legacy implementation
    # (with helpful error message directing to modular structure)
```

This means your existing code will continue to work while encouraging migration to the modular structure.

## 🗃️ File Status & Recommendations

| Legacy File | Status | Action | Migration Target |
|-------------|--------|--------|------------------|
| `DayIDelta.py` | ✅ **Can be archived** | Move to `legacy_files/` | `dayidelta.core.scd2_engine` |
| `scd2_chatbot.py` | ✅ **Can be archived** | Move to `legacy_files/` | `dayidelta.agents.chatbot` |
| `chatbot_cli.py` | ✅ **Can be archived** | Move to `legacy_files/` | `dayidelta.agents.cli` |
| `integration_test.py` | ✅ **Updated** | Now uses modular imports | Use `dayidelta.tests` for new tests |
| `test_chatbot.py` | ✅ **Updated** | Now uses modular imports | Use `dayidelta.tests` for new tests |
| `chatbot_examples.py` | ✅ **Updated** | Now uses modular imports | Continue using with modular imports |
| `unity_catalog_test.py` | ⚠️ **Can be consolidated** | Merge into modular tests | `dayidelta.tests.test_platforms` |
| `unity_catalog_example.py` | ⚠️ **Can be updated** | Update to modular imports | Move to `examples/` directory |
| `notebook-content.py` | ⚠️ **Can be updated** | Update to modular imports | Move to `examples/` directory |

## 🚀 Migration Steps

### Phase 1: Update Existing Code (Immediate)
```python
# Simply update your imports to use the top-level dayidelta package
from dayidelta import DayIDelta, SCD2Chatbot, setup_unity_catalog_environment
from dayidelta.core.models import TableSchema
```

### Phase 2: Clean Repository (When Ready)
1. Move legacy files to `legacy_files/` directory
2. Update documentation to emphasize modular structure
3. Create examples directory with updated examples

### Phase 3: Advanced Usage (Optional)
```python
# Take advantage of new features
from dayidelta.platforms.fabric import FabricAdapter  # New platform support
from dayidelta.core.scd2_engine import SCD2Engine    # Advanced engine usage
from dayidelta.query.generators import SQLQueryGenerator  # Custom query generation
```

## 🛠️ New Features Available in Modular Structure

1. **Microsoft Fabric Support**
   ```python
   from dayidelta.platforms.fabric import FabricAdapter
   ```

2. **Advanced SCD2 Engine**
   ```python
   from dayidelta.core.scd2_engine import SCD2Engine
   from dayidelta.platforms.unity_catalog import UnityCatalogAdapter
   
   engine = SCD2Engine(UnityCatalogAdapter())
   ```

3. **Enhanced Query Generation**
   ```python
   from dayidelta.query.generators import SQLQueryGenerator, PythonQueryGenerator
   from dayidelta.query.parsers import NaturalLanguageParser
   ```

4. **Better Error Handling**
   - Platform-specific error messages
   - Detailed logging with context
   - Graceful degradation for missing dependencies

## 🆘 Need Help?

- 📖 **Documentation**: Check the `docs/` directory for detailed guides
- 🧪 **Examples**: Look at `dayidelta/tests/` for usage examples
- 🐛 **Issues**: Open a GitHub issue if you encounter migration problems
- 💬 **Discussions**: Use GitHub Discussions for questions

## 📋 Checklist for Migration

- [ ] Update imports to use `from dayidelta import ...`
- [ ] Test that your existing functionality works
- [ ] Consider using new features (additional platforms, enhanced query generation)
- [ ] Update your documentation/README to use modular imports
- [ ] Run tests to ensure everything works correctly
- [ ] Archive legacy files when you're confident everything works

---

**The modular structure is production-ready and provides significant improvements over the legacy implementation. Migration is straightforward and backward compatibility is maintained.**