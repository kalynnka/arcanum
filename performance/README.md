# Arcanum Performance Benchmarks

This directory contains performance benchmarks comparing arcanum with pure SQLAlchemy and pure Pydantic approaches.

## Structure

The benchmarks are organized into **two modules**:

### 1. `test_noop_materia.py` - Pure Pydantic vs NoOpMateria

Compares arcanum's NoOpMateria transmuters against pure Pydantic models to measure the transmuter abstraction overhead **WITHOUT database integration**.

| Test Group | Description | Items |
|------------|-------------|-------|
| `simple-validate` | Flat objects, scalar fields | 100 |
| `simple-construct` | model_construct (no validation) | 100 |
| `nested-validate` | One level nesting (Book → Author, Publisher) | 50 |
| `deep-nested-validate` | Multi-level nesting (Author → Books → Details) | 10 |
| `circular-validate` | Circular references (Company ↔ Employee ↔ Dept) | 5 |
| `dump-dict` | model_dump() to dict | 100 |
| `dump-json` | model_dump_json() to JSON | 100 |
| `from-attributes-simple` | ORM-style attribute access | 100 |
| `from-attributes-nested` | Nested attribute access | 100 |

### 2. `test_sqlalchemy_crud.py` - SQLAlchemy Integration

Compares arcanum's SQLAlchemy materia against:
- **Pure SQLAlchemy** (baseline)
- **Common Pattern** (Pydantic validate → model_dump → ORM)

| Test Group | Description | Items |
|------------|-------------|-------|
| `create-single` | Create one object | 1 |
| `create-bulk` | Create batch of objects | 100 |
| `create-nested` | Create with relationships | 20 |
| `read-single` | Load by ID | 1 |
| `read-bulk` | Query multiple objects | 50 |
| `read-nested` | Eager load with relationships | 50 |
| `read-deep` | Deep nested loading (Author → Books) | 10 |
| `update-single` | Update one object | 1 |
| `update-bulk` | Update batch | 50 |
| `delete-single` | Delete one object | 1 |
| `delete-bulk` | Delete batch | 50 |
| `roundtrip` | Load → Modify → Save | 10 |

## Prerequisites

1. **Database**: Ensure PostgreSQL is running:
   ```bash
   docker-compose up -d
   ```

2. **Dependencies**: Install with dev dependencies:
   ```bash
   uv sync --dev
   ```

## Running Benchmarks

### Run All Benchmarks
```bash
pytest performance/ -v --benchmark-only --benchmark-disable-gc
```

### Run by Module

```bash
# NoOpMateria vs Pure Pydantic (no database)
pytest performance/test_noop_materia.py -v --benchmark-only

# SQLAlchemy CRUD operations
pytest performance/test_sqlalchemy_crud.py -v --benchmark-only
```

### Run by Benchmark Group

```bash
# Simple validation comparison
pytest performance/ -k "simple-validate" --benchmark-only

# All create operations
pytest performance/ -k "create-" --benchmark-only

# All read operations
pytest performance/ -k "read-" --benchmark-only

# All update operations
pytest performance/ -k "update-" --benchmark-only

# Delete operations
pytest performance/ -k "delete-" --benchmark-only

# Nested relationship tests
pytest performance/ -k "nested" --benchmark-only
```

### Save and Compare Results

```bash
# Save baseline
pytest performance/ --benchmark-save=baseline --benchmark-only

# Compare against baseline
pytest performance/ --benchmark-compare=baseline --benchmark-only
```

### JSON Output
```bash
pytest performance/ --benchmark-json=results.json --benchmark-only
```

## Test Fairness

All benchmarks follow strict fairness principles:

1. **Same Data**: All tests within a benchmark group use identical data (same seed, same count)
2. **Reproducible Randomness**: SEED=42 ensures reproducible but varied data
3. **Clear Documentation**: Docstrings explain when patterns differ architecturally
4. **Isolated Tests**: Each test uses unique test_id for database isolation

### Architectural Differences (Not Cheating)

Some comparisons show different patterns by design:

- **NoOpMateria validates only scalars**: Relationships are lazy associations
- **Pure Pydantic validates everything inline**: All nested objects validated upfront

These are intentional architectural differences, not unfair comparisons. The benchmarks document these differences clearly.

## Benchmark Approaches

### NoOpMateria Tests (test_noop_materia.py)

| Approach | Description |
|----------|-------------|
| Pure Pydantic | Standard `BaseModel` with `model_validate` |
| NoOpMateria | `BaseTransmuter` without database backing |

### SQLAlchemy Tests (test_sqlalchemy_crud.py)

| Approach | Description |
|----------|-------------|
| Pure SQLAlchemy | Direct ORM operations (baseline) |
| Common Pattern | Pydantic validate → model_dump → ORM create |
| Arcanum (validation) | Transmuter with `model_validate` |
| Arcanum (no validation) | Transmuter with `model_construct` |

## Example Output

```
-------------- benchmark 'create-bulk': 4 tests ---------------
Name                                      Mean
test_pure_sqlalchemy_bulk_create         422μs
test_common_pattern_bulk_create          531μs  (+26%)
test_arcanum_bulk_create_with_validation 1.6ms  (+278%)
test_arcanum_bulk_create_without_validation 2.6ms (+525%)

-------------- benchmark 'read-nested': 4 tests ---------------
Name                                      Mean
test_pure_sqlalchemy_nested_read          5.9ms
test_common_pattern_nested_read           5.5ms  (-7%)
test_arcanum_nested_read_with_validation  5.5ms  (-7%)
test_arcanum_nested_read_without_validation 5.8ms (-2%)
```

## Key Findings

1. **Single object overhead**: Arcanum adds ~400-700μs per object for transmuter creation
2. **Bulk operations amortize**: Per-object overhead decreases significantly in batches
3. **Nested relationships**: Gap narrows dramatically; database I/O dominates
4. **Common pattern competitive**: Pydantic + model_dump is often fastest for simple cases
5. **Arcanum advantages**: 
   - Single source of truth for validation + ORM
   - Bi-directional sync between transmuter and ORM
   - Cleaner code for complex relationships

## Files

```
performance/
├── conftest.py           # Shared fixtures (database, seeded data)
├── test_noop_materia.py  # Pure Pydantic vs NoOpMateria (18 tests)
├── test_sqlalchemy_crud.py # SQLAlchemy CRUD operations (39 tests)
└── README.md             # This file
```

Total: **57 tests** across **21 benchmark groups**
