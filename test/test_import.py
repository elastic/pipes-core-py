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

import os
import re
import sys
from contextlib import contextmanager
from importlib import import_module
from types import GeneratorType

import pytest

from core.errors import ConfigError
from core.util import serialize

from .util import run

import_module("core.import")

# All (guess, stdio) combinations for export tests
IMPORT_GUESS_STDIO = [
    (True, False),
    (False, True),
    (False, False),
]
IMPORT_GUESS_STDIO_IDS = [f"guess={g},stdio={s}" for g, s in IMPORT_GUESS_STDIO]
import_params = pytest.mark.parametrize("guess, stdio", IMPORT_GUESS_STDIO, ids=IMPORT_GUESS_STDIO_IDS)


@contextmanager
def run_import(format_, data, *, guess, stdio, streaming):
    from tempfile import NamedTemporaryFile

    filename = None
    old_stdin = None
    suffix = f".{format_}" if format_ else None
    try:
        config = {
            "streaming": streaming,
            "node@": "data",
        }
        state = {"data": {}}

        with NamedTemporaryFile(mode="w", suffix=suffix, delete=False) as f:
            filename = f.name
            serialize(f, data, format=format_)

        if stdio:
            old_stdin = sys.stdin
            sys.stdin = open(filename, "r")
        else:
            config["file"] = filename

        if format_ and not guess:
            config["format"] = format_

        with run("core.import", config, state, in_memory_state=streaming) as state:
            yield state["data"]
    finally:
        if old_stdin:
            sys.stdin.close()
            sys.stdin = old_stdin
        if filename:
            os.unlink(filename)


def test_import_streaming_unsupported():
    config = {
        "interactive": True,
        "streaming": True,
    }

    state = {}

    msg = "cannot use streaming import in UNIX pipe mode"
    with pytest.raises(ConfigError, match=msg):
        with run("core.import", config, state) as _:
            pass


@import_params
def test_import_yaml(guess, stdio):
    data = [{"doc1": "value1"}, {"doc2": "value2"}]

    with run_import("yaml", data, guess=guess, stdio=stdio, streaming=False) as data_:
        assert isinstance(data_, list)
        assert data_ == data

    msg = re.escape("cannot stream yaml (try ndjson)")
    with pytest.raises(ConfigError, match=msg):
        with run_import("yaml", data, guess=guess, stdio=stdio, streaming=True) as _:
            pass


@import_params
def test_import_json(guess, stdio):
    data = [{"doc1": "value1"}, {"doc2": "value2"}]

    with run_import("json", data, guess=guess, stdio=stdio, streaming=False) as data_:
        assert isinstance(data_, list)
        assert data_ == data

    msg = re.escape("cannot stream json (try ndjson)")
    with pytest.raises(ConfigError, match=msg):
        with run_import("json", data, guess=guess, stdio=stdio, streaming=True) as _:
            pass


@import_params
def test_import_ndjson(guess, stdio):
    data = [{"doc1": "value1"}, {"doc2": "value2"}]

    with run_import("ndjson", data, guess=guess, stdio=stdio, streaming=False) as data_:
        assert isinstance(data_, list)
        assert data_ == data

    with run_import("ndjson", data, guess=guess, stdio=stdio, streaming=True) as data_:
        assert isinstance(data_, GeneratorType)
        assert list(data_) == data
