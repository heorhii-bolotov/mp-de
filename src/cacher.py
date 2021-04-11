import logging
import logging.config
import yaml

from functools import wraps
from pathlib import Path
from typing import List, Callable, Sequence

import aiofiles as aiof
import asyncio


with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)

logger = logging.getLogger(__name__)


async def save_lines(fp: Path, lines: Sequence[str]) -> None:
    async with aiof.open(fp, 'w') as f:
        await f.writelines(map(lambda x: x + '\n', lines))


async def read_lines(fp: Path) -> List[str]:
    try:
        async with aiof.open(fp, 'r') as f:
            lines = await f.readlines()
            return [line.strip() for line in lines]
    except FileNotFoundError as e:
        logger.debug(e)
        return []


def cache_files(func: Callable) -> List[str]:
    @wraps(func)
    def with_cached(fp: Path, files: List[str]) -> List[str]:
        logger.debug('Cache new files')
        loop = asyncio.get_event_loop()
        old_files = set(loop.run_until_complete(read_lines(fp)))
        new_files = set(files) - old_files
        old_files.update(new_files)
        loop.run_until_complete(save_lines(fp, old_files))
        loop.close()
        logger.debug('Return new files to process')
        return func(fp, list(new_files))
    return with_cached


class Cache:
    @staticmethod
    @cache_files
    def new_files(fp: Path, files: List[str]) -> List[str]:
        return files
