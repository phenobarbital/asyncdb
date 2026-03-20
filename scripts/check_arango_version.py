import importlib.metadata
try:
    print(f"python-arango version: {importlib.metadata.version('python-arango')}")
except importlib.metadata.PackageNotFoundError:
    print("python-arango not installed")

try:
    import aioarangodb
    print(f"aioarangodb version: {aioarangodb.__version__}")
except ImportError:
    print("aioarangodb not installed")

try:
    from asyncdb.drivers.arangodb import arangodb
    print("asyncdb.drivers.arangodb imported successfully")
except ImportError as e:
    print(f"Failed to import asyncdb driver: {e}")
