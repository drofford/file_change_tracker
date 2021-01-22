import daiquiri
import hashlib
import inspect
import logging as __logging__
import os
import pprint
import re
import sys
import sqlite3

#
# set up logging
#
level = __logging__.DEBUG if os.getenv("DEBUG", "") == "Y" else __logging__.INFO
__logging__.basicConfig(format="%(asctime)s %(levelname)s : %(message)s", level=level)
logger = __logging__.getLogger()

logger.error(f"{logger=}")

#
# constants
#
FILE_TABLE_NAME = "files"


def debug(msg):
    f = inspect.currentframe()
    logger.debug(f"{f.f_back.f_code.co_name}[{f.f_back.f_lineno}]: {msg}")


def info(msg):
    f = inspect.currentframe()
    logger.info(f"{f.f_back.f_code.co_name}[{f.f_back.f_lineno}]: {msg}")


def warning(msg):
    f = inspect.currentframe()
    logger.warning(f"{f.f_back.f_code.co_name}[{f.f_back.f_lineno}]: {msg}")


def error(msg):
    f = inspect.currentframe()
    logger.error(f"{f.f_back.f_code.co_name}[{f.f_back.f_lineno}]: {msg}")


def fatal(msg):
    f = inspect.currentframe()
    logger.error(f"{f.f_back.f_code.co_name}[{f.f_back.f_lineno}]: {msg}")
    exit(1)
