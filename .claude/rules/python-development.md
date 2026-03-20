---
trigger: always_on
---

You are an expert in Python, AI, and Machine Learning development.

**CRITICAL ENVIRONMENT RULES:**
1. **Package Manager**: You MUST use **`uv`** for all package management (e.g., `uv pip install`, `uv pip list`, `uv add`).
2. **Virtual Environment**: You MUST always act within the virtual environment.
   - **CRITICAL**: NEVER run `uv`, `python`, or `pip` commands without first activating the environment.
   - **ALWAYS** run `source .venv/bin/activate` before any python-related command.
3. **Dependencies**: All dependencies must be managed via `pyproject.toml`.

Key Principles:
- Write clean, efficient, and well-documented code
- Follow PEP 8 style guidelines
- Use type hints for better code clarity
- Implement proper error handling
- Write modular and reusable code

Python Best Practices:
- Follow naming conventions (snake_case for functions/variables)
- Use list comprehensions and generator expressions
- Use context managers (with statement)
- Implement proper logging

Machine Learning:
- Use scikit-learn for traditional ML
- Use PyTorch or TensorFlow for deep learning
- Implement proper data preprocessing
- Use cross-validation for model evaluation
- Track experiments with MLflow or Weights & Biases
- Version control datasets and models

Data Processing:
- Use pandas for data manipulation
- Use numpy for numerical computations
- Use matplotlib/seaborn for visualization
- Implement data validation
- Handle missing data appropriately
- Use efficient data structures

Deep Learning:
- Use PyTorch or TensorFlow/Keras
- Implement proper model architecture
- Use data augmentation
- Implement early stopping and checkpointing
- Use GPU acceleration when available
- Monitor training with TensorBoard

Model Deployment:
- Use aiohttp for serving models
- Implement model versioning
- Use Docker for containerization
- Implement proper API documentation
- Add input validation and error handling
- Monitor model performance in production

Testing:
- Write unit tests with pytest and pytest-asyncio
- Test data pipelines
- Test model predictions
- Use fixtures for test data
- Implement integration tests

Performance:
- Use vectorization with numpy
- Use multiprocessing for CPU-bound tasks
- Use async/await for I/O-bound tasks
- Profile code to identify bottlenecks
- Use Cython or numba for optimization