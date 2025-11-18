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

import re
from collections import namedtuple
from contextlib import nullcontext

import pytest
from typing_extensions import Annotated

from core import Pipe
from core.errors import Error
from core.runner import parse_runtime_arguments

from .util import TestPipe


def cases(args_env):
    TC = namedtuple("TC", ["state", "config", "args_env", "exp", "exc", "err"], defaults=[None, None, None])
    defaults = {"NAME": "you", "AGE": -1}

    config = {
        "name@": f"runtime.{args_env}.NAME",
        "age@": f"runtime.{args_env}.AGE",
        "favourite-colors@": f"runtime.{args_env}.FAVOURITE_COLORS",
    }

    return (
        TC(
            state={},
            config={"name": "me", "age": 42},
            args_env={},
        ),
        TC(
            state={},
            config=config,
            args_env={},
            exp=None,
            exc=Error,
            err=f"param 'name': state node not found: 'runtime.{args_env}.NAME'",
        ),
        TC(
            state={},
            config=config,
            args_env={"NAME": None},
            exp=None,
            exc=Error,
            err=f"param 'name': state node not found: 'runtime.{args_env}.NAME'",
        ),
        TC(
            state={},
            config=config,
            args_env={"NAME": ""},
            exp=None,
            exc=Error,
            err=f"param 'name': state node not found: 'runtime.{args_env}.NAME'",
        ),
        TC(
            state={},
            config=config,
            args_env={"SHELL": "/bin/sh"},
            exp=None,
            exc=Error,
            err=f"param 'name': state node not found: 'runtime.{args_env}.NAME'",
        ),
        TC(
            state={},
            config=config,
            args_env={"NAME": "0"},
            exp={"NAME": "0"},
            exc=Error,
            err=f"param 'age': state node not found: 'runtime.{args_env}.AGE'",
        ),
        TC(
            state={},
            config=config,
            args_env={"NAME": "me", "AGE": "42"},
            exp={"NAME": "me", "AGE": 42},
        ),
        TC(
            state={"runtime": {args_env: defaults.copy()}},
            config=config,
            args_env={"NAME": "me"},
            exp={"NAME": "me", "AGE": -1},
        ),
        TC(
            state={"runtime": {args_env: defaults.copy()}},
            config=config,
            args_env={"AGE": "42"},
            exp={"NAME": "you", "AGE": 42},
        ),
        TC(
            state={"runtime": {args_env: defaults.copy()}},
            config=config,
            args_env={},
            exp={"NAME": "you", "AGE": -1},
        ),
        TC(
            state={"runtime": {args_env: defaults.copy()}},
            config=config,
            args_env={"NAME": None},
            exp={"NAME": "you", "AGE": -1},
        ),
        TC(
            state={"runtime": {args_env: defaults.copy()}},
            config=config,
            args_env={"AGE": ""},
            exp={"NAME": "you", "AGE": -1},
        ),
        TC(
            state={"runtime": {args_env: defaults.copy()}},
            config=config,
            args_env={"FAVOURITE_COLORS": "('blue', 'green')"},
            exp={"NAME": "you", "AGE": -1, "FAVOURITE_COLORS": ("blue", "green")},
        ),
        TC(
            state={"runtime": {args_env: defaults.copy()}},
            config=config,
            args_env={"NAME": "me", "AGE": "n/a"},
            exp={"NAME": "me", "AGE": "n/a"},
            exc=Error,
            err=re.escape(f"param 'age': state node 'runtime.{args_env}.AGE' type mismatch: 'str' (expected 'int')"),
        ),
    )


@pytest.mark.parametrize(
    "args, exp",
    (
        (
            ["key"],
            {"key": ""},
        ),
        (
            ["key="],
            {"key": ""},
        ),
        (
            ["key=''"],
            {"key": "''"},
        ),
        (
            ["key=val"],
            {"key": "val"},
        ),
        (
            ["key=3"],
            {"key": "3"},
        ),
        (
            ["key={'a': 3, 'pi': 3.14}"],
            {"key": "{'a': 3, 'pi': 3.14}"},
        ),
    ),
)
def test_parse_runtime_arguments(args, exp):
    assert dict(parse_runtime_arguments(args)) == exp


@pytest.mark.parametrize("tc", cases("arguments"))
def test_runtime_arguments(tc):
    @TestPipe
    def _fn(
        name: Annotated[str, Pipe.Config("name")],
        age: Annotated[int, Pipe.Config("age")],
        favourite_colors: Annotated[tuple, Pipe.Config("favourite-colors")] = (),
    ):
        pass

    args = [k if v is None else f"{k}={v}" for k, v in tc.args_env.items()]

    with pytest.raises(tc.exc, match=tc.err) if tc.exc else nullcontext():
        with _fn(tc.config, tc.state, arguments=args) as state:
            if tc.exp:
                assert state["runtime"]["arguments"] == tc.exp
            else:
                assert "arguments" not in state["runtime"]


@pytest.mark.parametrize("tc", cases("environment"))
def test_runtime_environment(tc):
    @TestPipe
    def _fn(
        name: Annotated[str, Pipe.Config("name")],
        age: Annotated[int, Pipe.Config("age")],
        favourite_colors: Annotated[tuple, Pipe.Config("favourite-colors")] = (),
    ):
        pass

    with pytest.raises(tc.exc, match=tc.err) if tc.exc else nullcontext():
        with _fn(tc.config, tc.state, environment=tc.args_env) as state:
            if tc.exp:
                assert state["runtime"]["environment"] == tc.exp
            else:
                assert "environment" not in state["runtime"]
