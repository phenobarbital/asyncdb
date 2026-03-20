
import inspect
import sys
import asyncio

try:
    import arango
    print(f"Imported arango from: {arango.__file__}")
    from arango import ArangoClient
    print("ArangoClient imported successfully")
    
    client = ArangoClient(hosts="http://localhost:8529")
    print(f"Client: {client}")
    
    # Check if db method is async
    if inspect.iscoroutinefunction(client.db):
        print("client.db is async")
    else:
        print("client.db is sync")

    # Mock db object to check aql.execute
    # We might need a real connection or just inspect the class if possible
    # Let's inspect the class methods
    print("Inspecting ArangoClient methods:")
    for name, method in inspect.getmembers(ArangoClient):
        if not name.startswith("_"):
            print(f"  {name}: {'async' if inspect.iscoroutinefunction(method) else 'sync'}")

except ImportError:
    print("Failed to import arango. Maybe python-arango-async uses a different namespace?")
    # Try getting the package name from installed packages
    import importlib.metadata
    try:
        dist = importlib.metadata.distribution('python-arango-async')
        print(f"python-arango-async installed. Files: {list(dist.files)[:5]}")
    except Exception as e:
        print(f"python-arango-async not found: {e}")
