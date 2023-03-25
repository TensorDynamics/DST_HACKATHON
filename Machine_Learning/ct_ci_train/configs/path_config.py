"""
relating paths repo for the source code
"""
from os import getcwd, path, makedirs
root_path = getcwd()
resources_path = path.join(root_path, 'resources')
makedirs(resources_path, exist_ok=True)
