#!/usr/bin/env python3

# Copyright 2025 Elasticsearch B.V.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Internal utilities for the elastic.pipes.core package.

This module contains implementation details that are not part of the public API.
"""

import sys
from pathlib import Path
from types import ModuleType

try:
    import tomllib
except ModuleNotFoundError:
    try:
        import tomli as tomllib
    except ModuleNotFoundError:
        tomllib = None


def find_pyproject(base_dir):
    current = Path(base_dir)

    # Search upward through parent directories
    while True:
        pyproject_path = current / "pyproject.toml"
        if pyproject_path.exists():
            return pyproject_path

        parent = current.parent
        if parent == current:  # Reached root
            break
        current = parent


def find_and_parse_pyproject_toml(base_dir, logger):
    if pyproject_path := find_pyproject(base_dir):
        try:
            with open(pyproject_path, "rb") as f:
                data = tomllib.load(f)
        except Exception as e:
            logger.warning(f"failed to parse pyproject.toml: {e}")
            return None

        # Check for [tool.elastic-pipe.package-dir] section
        if elastic_pipe_config := data.get("tool", {}).get("elastic-pipe", {}).get("package-dir", None):
            logger.debug(f"found [tool.elastic-pipe.package-dir] in {pyproject_path}")
            return elastic_pipe_config, pyproject_path.parent


def setup_namespace_package(package_name, directory, logger):
    """Setup a namespace package mapping using standard Python mechanisms.

    Creates the package hierarchy in sys.modules and sets __path__ to point
    to the mapped directory, allowing Python's standard import machinery to
    find submodules.

    Example: mapping 'elastic.pipes.ec.auth' to 'ec/auth/' allows importing
    'elastic.pipes.ec.auth.foo' from 'ec/auth/foo.py'.
    """
    directory = Path(directory).absolute()
    logger.debug(f"  namespace package: '{package_name}' -> '{directory}'")

    # Split package name into parts
    parts = package_name.split(".")

    # Create all parent packages as namespace packages if they don't exist
    for i in range(len(parts)):
        parent_name = ".".join(parts[: i + 1])

        if parent_name not in sys.modules:
            # Create a new namespace package
            module = ModuleType(parent_name)
            module.__package__ = parent_name
            module.__path__ = []
            sys.modules[parent_name] = module

        # For the target package, set its __path__ to the mapped directory
        if parent_name == package_name:
            sys.modules[parent_name].__path__ = [str(directory)]
