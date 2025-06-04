#!/usr/bin/env python3

import asyncio
from logging import Logger

from elastic.pipes.core import Pipe
from fastmcp import Client
from typing_extensions import Annotated


def call_tool(client, tool, log):
    async def _tool():
        async with client:
            tools = await client.list_tools()
            log.debug(tools)
            return await client.call_tool(tool["name"], tool["args"])

    return asyncio.run(_tool())


class Context(Pipe.Context):
    url: Annotated[str, Pipe.Config("url"), Pipe.Help("url of the MCP server")]
    tool: Annotated[dict, Pipe.Config("tool"), Pipe.Help("tool invocation")] = None

    def __init__(self):
        self._client = Client(self.url)

    def call_tool(self):
        return call_tool(self._client, self.tool, self.logger)


@Pipe("elastic.pipes.mcp", default={}, notes="Use this example pipe as starting point for yours.")
def main(
    log: Logger,
    ctx: Context,
    dry_run: bool = False,
):
    """Say hello to someone."""

    result = ctx.call_tool()
    log.debug(f"result: {result}")


if __name__ == "__main__":
    main()
