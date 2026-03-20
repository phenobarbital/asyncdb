import sys
import re
import subprocess
from pathlib import Path

VERSION_FILE = Path("asyncdb/version.py")
BUMP_CONFIG = Path(".bumpversion.cfg")

def get_current_version():
    content = VERSION_FILE.read_text()
    match = re.search(r'__version__\s*=\s*"([^"]+)"', content)
    if not match:
        raise ValueError("Could not find version string in asyncdb/version.py")
    return match.group(1)

def bump_version(current_version, part):
    try:
        # Handle versions like 2.13.2 or 0.1.0
        # If version has extra info (rc, dev), strip it for simplicity or fail
        # Assuming semantic versioning X.Y.Z
        base_version = current_version.split('-')[0]
        major, minor, patch = map(int, base_version.split('.'))
    except ValueError:
        print(f"Error parsing version: {current_version}")
        sys.exit(1)

    if part == 'major':
        major += 1
        minor = 0
        patch = 0
    elif part == 'minor':
        minor += 1
        patch = 0
    elif part == 'patch':
        patch += 1
    else:
        raise ValueError(f"Invalid part: {part}")
    return f"{major}.{minor}.{patch}"

def update_version_file(new_version):
    content = VERSION_FILE.read_text()
    new_content = re.sub(r'(__version__\s*=\s*")[^"]+(")', f'\g<1>{new_version}\g<2>', content)
    VERSION_FILE.write_text(new_content)

def main():
    if len(sys.argv) != 2:
        print("Usage: python scripts/release.py [patch|minor|major]")
        sys.exit(1)
    
    part = sys.argv[1]
    if part not in ['patch', 'minor', 'major']:
        print(f"Invalid part: {part}")
        sys.exit(1)

    current_version = get_current_version()
    new_version = bump_version(current_version, part)
    
    print(f"Bumping version: {current_version} -> {new_version}")
    
    update_version_file(new_version)
    
    # Try to update .bumpversion.cfg if it exists
    if BUMP_CONFIG.exists():
        cfg_content = BUMP_CONFIG.read_text()
        print(f"Updating .bumpversion.cfg to {new_version}")
        # Simply replacing current_version line if it looks like standard bumpversion logic
        new_cfg = re.sub(r'(current_version\s*=\s*)[\d\.]+', f'\g<1>{new_version}', cfg_content)
        BUMP_CONFIG.write_text(new_cfg)

    # Git commit
    files_to_add = [str(VERSION_FILE)]
    if BUMP_CONFIG.exists():
        files_to_add.append(str(BUMP_CONFIG))
        
    subprocess.check_call(["git", "add"] + files_to_add)
    subprocess.check_call(["git", "commit", "-m", f"Bump version: {current_version} -> {new_version}"])
    
    # Check if tag exists (unlikely if sequential but good practice)
    tag_name = f"v{new_version}"
    try:
        subprocess.check_call(["git", "tag", tag_name])
    except subprocess.CalledProcessError:
        print(f"Tag {tag_name} already exists or failed to create.")

    print(f"Released {tag_name}")

if __name__ == "__main__":
    main()
