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

"""Elastic Pipes component to rewrite the timestamp of documents."""

from datetime import datetime, timezone

from elastic.pipes.core import Pipe
from elastic.pipes.core.errors import ConfigError
from typing_extensions import Annotated


class StrategyNow:
    def __init__(self, state):
        pass

    def __call__(self, ts):
        return datetime.now(timezone.utc)


class StrategyNowFirst:
    def __init__(self, state):
        self.start = datetime.now(timezone.utc)
        self.first = None

    def __call__(self, ts):
        if self.first is None:
            self.first = ts
        delta = ts - self.first
        return self.start + delta


def get_strategy(pipe):
    name = pipe.config("strategy", "now")
    strategies = {
        "now": StrategyNow,
        "now-first": StrategyNowFirst,
    }
    if name not in strategies:
        raise ConfigError(f"unknown strategy: {name} (allowed strategies: {', '.join(sorted(strategies))})")
    params = pipe.config("strategy-params", None) or {}
    return strategies[name](pipe.state, **params)


@Pipe("elastic.pipes.core.timestamp-rewrite", default={})
def main(
    pipe: Pipe,
    dry_run: bool = False,
    ts_field: Annotated[str, Pipe.Config("timestamp-field")] = "@timestamp",
    docs: Annotated[list, Pipe.State("documents")] = [],
):
    log = pipe.logger
    strategy = get_strategy(pipe)

    if dry_run:
        return

    log.debug(f"rewriting the '{ts_field}' field of {len(docs)} documents")

    for i, doc in enumerate(docs):
        ts = datetime.now(timezone.utc)
        try:
            if ts_field in doc:
                ts = datetime.strptime(doc[ts_field], "%Y-%m-%dT%H:%M:%S.%fZ")
        except Exception as e:
            log.warning(f"'{ts_field}' parse error, using current time: {e}")

        doc[ts_field] = strategy(ts).isoformat(timespec="milliseconds")


if __name__ == "__main__":
    main()
