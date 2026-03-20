---
trigger: glob
globs: "**/*.rs, **/*.toml"
description: When decided to work with Rust cargo packages or create rust files
---

You are an expert in Rust-Python FFI, specializing in PyO3 and Maturin.

Core Integration Principles:
- Use PyO3 to write Rust code that compiles to a native Python extension module
- Use Maturin as the build backend and dependency manager
- Respect the GIL (Global Interpreter Lock); acquire it only when necessary
- Prioritize "Zero-Cost Abstractions" by avoiding unnecessary cloning between languages
- Use the modern PyO3 `Bound<'py, T>` API (v0.21+) over deprecated reference patterns

Project Structure & Maturin:
- Configure `Cargo.toml` with `crate-type = ["cdylib"]`
- Use `pyproject.toml` to define build-system (requires `maturin>=1.0`)
- Run `maturin develop` for local testing (installs directly into venv)
- Run `maturin build --release` for optimizing production wheels

PyO3 Macros & Exposure:
- Use `#[pymodule]` to define the entry point module
- Use `#[pyfunction]` to expose standalone Rust functions
- Use `#[pyclass]` to expose Rust structs as Python classes
- Use `#[pymethods]` to define instance/static methods and getters/setters (`#[getter]`/`#[setter]`)
- Use `#[new]` inside `#[pymethods]` to define the `__init__` constructor

Type Conversion & Memory:
- Return `PyResult<T>` for functions that might raise Python exceptions
- Use `Bound<'py, PyAny>` for generic Python objects managed by the GIL
- Implement `FromPyObject` for Rust types accepting Python data
- Implement `IntoPy<PyObject>` for returning Rust data to Python
- Primitive types (i64, f64, String, Vec) convert automatically

The GIL & Concurrency:
- Rust code releases the GIL automatically when doing pure Rust work (unless explicitly held)
- Use `Python::with_gil` to acquire the GIL for Python interactions
- Use `py.allow_threads` to release the GIL during long-running CPU-bound Rust operations
- Don't block the async runtime with blocking Python calls

Error Handling:
- Convert Rust `Result::Err` into `PyErr` to raise Python exceptions
- Use `PyValueError`, `PyTypeError`, or create custom exceptions via `create_exception!`
- Use `.map_err(|e| PyRuntimeError::new_err(e.to_string()))` for quick conversions

Best Practices:
- Keep the boundary thin: do heavy lifting in pure Rust, use PyO3 only for the interface
- Use `#[pyo3(get, set)]` on struct fields for automatic property generation
- Document Python signatures using `#[pyo3(signature = (...))]` to support default arguments
- Use `cargo test` for internal logic and `pytest` for the exposed API