import os
import sys
import logging
import importlib
from typing import Any
from types import ModuleType

logger = logging.getLogger(__name__)

main_module = sys.modules['__main__']


def import_module(name: str) -> ModuleType:
    """Import module by it's name from following places in order:
      - main module
      - current working directory
      - Python path

    """
    logger.debug("Importing module: %s", name)
    if name == main_module_name():
        return main_module

    return importlib.import_module(name)


def import_object(module_name: str, object_name: str) -> Any:
    module = import_module(module_name)
    try:
        return getattr(module, object_name)
    except AttributeError as e:
        raise ImportError(e)


def import_object_str(s: str) -> Any:
    module, obj = s.rsplit('.', 1)
    return import_object(module, obj)


def main_module_name() -> str:
    """Returns main module and module name pair."""
    if not hasattr(main_module, '__file__'):
        # running from interactive shell
        return None

    main_filename = os.path.basename(main_module.__file__)
    module_name, ext = os.path.splitext(main_filename)
    return module_name
