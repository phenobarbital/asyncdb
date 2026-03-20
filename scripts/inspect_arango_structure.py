
import pkgutil
import arango
import inspect

print(f"Arango package path: {arango.__path__}")

def list_submodules(package):
    if hasattr(package, "__path__"):
        for importer, modname, ispkg in pkgutil.iter_modules(package.__path__):
            print(f"Found submodule: {modname} (is_pkg={ispkg})")

list_submodules(arango)

# Check for specific async classes if known, or just dump dirs
print("\nDir(arango):")
print(dir(arango))
