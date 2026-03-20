
import inspect
from arango.database import StandardDatabase
from arango.collection import StandardCollection

print("Inspecting StandardDatabase methods:")
for name, method in inspect.getmembers(StandardDatabase):
    if not name.startswith("_"):
        print(f"  {name}: {'async' if inspect.iscoroutinefunction(method) else 'sync'}")

print("\nInspecting StandardCollection methods:")
for name, method in inspect.getmembers(StandardCollection):
    if not name.startswith("_"):
        print(f"  {name}: {'async' if inspect.iscoroutinefunction(method) else 'sync'}")
