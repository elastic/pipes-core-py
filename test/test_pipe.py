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

import logging
import re

import pytest
from typing_extensions import Annotated

from core import Pipe
from core.errors import ConfigError

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(name)s - %(message)s"))

logger = logging.getLogger("elastic.pipes")
logger.addHandler(handler)
# logger.setLevel("DEBUG")


def test_dry_run():
    executions = 0

    @Pipe("test_no_dry_run")
    def _(pipe):
        nonlocal executions
        executions += 1

    @Pipe("test_dry_run_false")
    def _(pipe, dry_run):
        nonlocal executions
        executions += 1
        assert dry_run is False

    @Pipe("test_dry_run_true")
    def _(pipe, dry_run):
        nonlocal executions
        executions += 1
        assert dry_run is True

    Pipe.find("test_no_dry_run").run({}, {}, False, logger)
    assert executions == 1

    # if the pipe function does not have the `dry_run` argument,
    # then it's not executed on dry run
    Pipe.find("test_no_dry_run").run({}, {}, True, logger)
    assert executions == 1

    Pipe.find("test_dry_run_false").run({}, {}, False, logger)
    assert executions == 2

    Pipe.find("test_dry_run_true").run({}, {}, True, logger)
    assert executions == 3


def test_multiple():
    @Pipe("test_multiple")
    def _(pipe):
        pass

    msg = f"pipe 'test_multiple' is already defined in module '{__name__}'"
    with pytest.raises(ConfigError, match=msg):

        @Pipe("test_multiple")
        def _(pipe):
            pass


def test_config():
    @Pipe("test_config")
    def _(
        pipe: Pipe,
        name: Annotated[str, Pipe.Config("name")],
    ):
        assert name == "me"

    msg = "config node not found: 'name'"
    with pytest.raises(KeyError, match=msg):
        Pipe.find("test_config").run({}, {}, False, logger)

    Pipe.find("test_config").run({"name": "me"}, {}, False, logger)


def test_config_optional():
    @Pipe("test_config_optional")
    def _(
        pipe: Pipe,
        name: Annotated[str, Pipe.Config("name")] = "me",
    ):
        assert name == "me"

    Pipe.find("test_config_optional").run({}, {}, False, logger)


def test_state():
    @Pipe("test_state")
    def _(
        pipe: Pipe,
        name: Annotated[str, Pipe.State("name")],
    ):
        assert name == "me"

    msg = "state node not found: 'name'"
    with pytest.raises(KeyError, match=msg):
        Pipe.find("test_state").run({}, {}, False, logger)

    Pipe.find("test_state").run({}, {"name": "me"}, False, logger)


def test_state_optional():
    @Pipe("test_state_optional")
    def _(
        pipe: Pipe,
        name: Annotated[str, Pipe.State("name")] = "me",
    ):
        assert name == "me"

    Pipe.find("test_state_optional").run({}, {}, False, logger)


def test_state_setdefault():
    state = {}

    @Pipe("test_state_no_setdefault")
    def _(
        pipe: Pipe,
        names: Annotated[str, Pipe.State("names")] = [],
    ):
        names.extend(["me", "you"])

    @Pipe("test_state_setdefault")
    def _(
        pipe: Pipe,
        names: Annotated[str, Pipe.State("names", setdefault=True)] = [],
    ):
        names.extend(["me", "you"])

    Pipe.find("test_state_no_setdefault").run({}, state, False, logger)
    assert state == {}

    Pipe.find("test_state_setdefault").run({}, state, False, logger)
    assert state == {"names": ["me", "you"]}
