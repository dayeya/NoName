#!/usr/bin/env python
# FinalWall Author: Dayeya

import sys
import asyncio
import tracemalloc

from engine import Waf, WafConfig
from engine.errors import VersionError


async def main():
    """FinalWall-Engine creation."""
    configuration = WafConfig()
    engine = Waf(
        conf=configuration,
        ucid=8585,
        local=True,
        with_tunneling=False,
    )
    await engine.deploy()
    await engine.work()


if __name__ == "__main__":
    tracemalloc.start()

    if sys.version_info[0:2] != (3, 12):
        raise VersionError("Wrong python version. Please use +=3.12 only")

    tracemalloc.start()
    asyncio.run(main())
