# Arcanum TODO

## Performance Optimizations

Based on profiling results comparing NoOpMateria transmuters vs pure Pydantic models, the following optimizations could reduce the ~10x overhead for simple validation operations.

### Profiling Summary

| Metric | Pure Pydantic | NoOp Transmuter | Ratio |
|--------|---------------|-----------------|-------|
| Function calls (1000 objects) | 4,001 | 58,001 | 14.5x |
| Total time | 0.001s | 0.010s | 10x |

### High Priority

- [ ] **Reduce `__transmuter_provider__` call frequency**
  - Location: `BaseTransmuter.__init__`, `__setattr__`, `model_formulate`
  - Currently: `__transmuter_provider__` called 3,000+ times for 1,000 validations
  - Note: The lookup itself is already O(1) (ContextVar + dict lookup), the issue is call frequency
  - Solution: Cache the provider result as a local variable within each method instead of calling the property multiple times
  
  ```python
  # Current (multiple property calls):
  def __init__(self, **data):
      super().__init__(**data)
      if (provider := type(self).__transmuter_provider__) is not None:
          ...
      for name in type(self).model_associations:
          ...  # may call __transmuter_provider__ again via __getattribute__
  
  # Optimized (single lookup, reuse):
  def __init__(self, **data):
      super().__init__(**data)
      cls = type(self)
      provider = cls.__transmuter_provider__
      if provider is not None:
          ...
      # Use cached provider/cls throughout method
  ```
  - Impact: Reduces property lookups from 3+ per object to 1 per object in hot paths

- [ ] **Optimize `__getattribute__` for non-association fields**
  - Location: `BaseTransmuter.__getattribute__`
  - Currently: Every attribute access checks `name in type(self).model_associations`
  - Solution: Cache association names as a `frozenset` on the class for O(1) membership test, or skip check entirely for known non-association fields using `__slots__`
  - Impact: Eliminates 9,000+ dict lookups per 1,000 validations

- [ ] **Short-circuit wrap validator for NoOp case**
  - Location: `BaseTransmuter.model_formulate`
  - Currently: Wrap validator runs for every validation, even when no provider exists
  - Solution: Check for NoOpMateria early and skip the wrap validator machinery, or use conditional validator registration
  - Impact: Eliminates 2,000 unnecessary validator calls per 1,000 validations

### Medium Priority

- [ ] **Lazy association iteration in `__init__`**
  - Location: `BaseTransmuter.__init__`
  - Currently: Always iterates over `model_associations` even if empty
  - Solution: Skip iteration if `model_associations` is empty (common for simple models)
  - Impact: Minor improvement for simple models

- [ ] **Reduce `model_associations` property calls**
  - Location: Multiple places calling `type(self).model_associations`
  - Currently: Property called 10,000+ times per 1,000 validations
  - Solution: Cache on instance or use direct class attribute access
  - Impact: Eliminates redundant property overhead

### Low Priority / Future Considerations

- [ ] **Consider `__slots__` for BaseTransmuter**
  - Would reduce memory footprint and slightly improve attribute access
  - Trade-off: Less flexible for dynamic attributes

- [ ] **Benchmark with Cython compilation**
  - Hot paths like `__getattribute__` could benefit from Cython
  - Trade-off: Build complexity

### Notes

The overhead is inherent to the transmuter architecture - it's designed to intercept attribute access and sync with ORM objects. For NoOpMateria (no ORM), most of this machinery does nothing but still executes. This is the "tax" for having a unified API across different backends.

**Key insight**: The arcanum architecture excels at complex CRUD operations with relationships (where it's often faster than manual patterns), but has overhead for simple flat object validation where pure Pydantic is more direct.
