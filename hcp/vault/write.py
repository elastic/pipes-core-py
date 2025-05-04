#!/usr/bin/env python3

import sys
from logging import Logger

import hvac
from elastic.pipes.core import Pipe
from typing_extensions import Annotated

from .common import Context


@Pipe("elastic.pipes.hcp.vault.write")
def main(
    log: Logger,
    ctx: Context,
    path: Annotated[
        str,
        Pipe.Config("path"),
        Pipe.Help("Vault path destination of the data"),
    ],
    vault: Annotated[
        dict,
        Pipe.State("vault", mutable=True),
        Pipe.Help("state node containing the source data"),
    ],
):
    """Write data to an HCP Vault instance."""

    log.info(f"connect to '{ctx.url}'")
    vc = hvac.Client(url=ctx.url, token=ctx.token)

    try:
        if not vc.is_authenticated():
            log.error("Vault could not authenticate")
            sys.exit(1)
    except Exception as e:
        log.error(e)
        sys.exit(1)

    log.info(f"write to path '{path}'")
    res = vc.write_data(path, data=vault)
    if res is None:
        log.error(f"could not write path: '{path}'")
        sys.exit(1)


if __name__ == "__main__":
    main()
