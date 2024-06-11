---
title: The Registry
---

The Registry is Chroma's global object registry. It is a singleton object that is used to store a dictionary of Types by name. The Registry is primarily used to persist objects alongside Collections, so that they can be reconstructed when the collection is loaded.

Any object can be registered using the @_register decorator. Objects with the same name cannot be registered more than once.

```python
from chromadb.utils.the_registry import _register, _get

@_register
class RegisteredObject:
    ...

concrete_object = RegisteredObject()

# Returns its type
_get(concrete_object.__name__)
```

The Registry is defined in `chromadb/utils/the_registry.py`
Tests for the Registry can be found in `chromadb/test/object_persistence/test_object_registry.py`.
