
import importlib.metadata

try:
    dist = importlib.metadata.distribution('python-arango-async')
    print(f"Distribution: {dist.metadata['Name']} {dist.version}")
    
    # Try to find top-level packages
    # This info is usually in top_level.txt
    top_level = dist.read_text('top_level.txt')
    if top_level:
        print(f"Top-level packages:\n{top_level}")
    else:
        print("top_level.txt not found in distribution metadata")
        
    # List files if top_level is missing to infer package name
    files = dist.files
    if files:
        # Get unique top-level directories
        top_dirs = set()
        for f in files:
            parts = str(f).split('/')
            if len(parts) > 1 and not parts[0].endswith('.dist-info'):
                top_dirs.add(parts[0])
        print(f"Inferred top-level dirs: {top_dirs}")

except importlib.metadata.PackageNotFoundError:
    print("python-arango-async not installed")
