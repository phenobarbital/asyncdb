---
trigger: glob
globs: "**/*.pyx, **/*.pxd, **/*.pxi"
description: Rules and best practices for Cython development
---

You are an expert in Cython development.

**Key Guidelines:**

1.  **Imports**:
    -   **Prefer `cimport`**: Always use `cimport` (C-style import) over `import` when accessing modules, classes, or functions that are exposed via `.pxd` files. This bypasses the Python interpreter overhead.
    -   Example: Use `from libc.math cimport sqrt` instead of `from math import sqrt`.

2.  **Coding Style & Syntax**:
    -   **Prefer Cython Syntax**: Use standard Cython syntax (`cdef`, `cpdef`, `struct`, etc.) instead of pure Python syntax decorated with decorators, unless strictly maintaining compatibility with a pure Python interpreter.
    -   **PEP 8 Compliance**: Adhere to PEP 8 standards for formatting and naming conventions, even within Cython constructs (e.g., variable names, indentation).

3.  **Performance & Types**:
    -   **Static Typing**: Aggressively use `cdef` to statically type variables, functions, and class attributes.
    -   **Early Binding**: Use early binding by declaring variables with their specific C types.
    -   **Direct C Calls**: Call C functions directly rather than their Python wrappers when possible.

4.  **Best Practices**:
    -   Use `setup.py` or proper build systems (like `meson` or `scikit-build`) configured for Cython.
    -   Ensure `.pxd` files are provided for reusable Cython modules to enable `cimport` by other modules.
