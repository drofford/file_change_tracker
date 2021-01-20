import os
import os.path
import shutil
import sqlite3
import tempfile
import time
import traceback

from filechanges.filechanges2 import (
    logger,
    debug, info, warning, error, fatal
)


def test_001():
    logger.debug(f"test_001: debug")
    logger.info(f"test_001: info")
    logger.warning(f"test_001: warning")
    logger.error(f"test_001: error")
    # logger.fatal(f"test_001: fatal")

    debug(f"test_001: debug")
    info(f"test_001: info")
    warning(f"test_001: warning")
    error(f"test_001: error")
    # fatal(f"test_001: fatal")


    assert False
