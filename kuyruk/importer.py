import os
import sys
import logging
import importlib
from collections import namedtuple

logger = logging.getLogger(__name__)


def import_module(name):
    """Import module by it's name from following places in order:
      - main module
      - current working directory
      - Python path

    """
    logger.debug("Importing module: %s", name)
    module, main_module_name = get_main_module()
    if name == main_module_name:
        return module
    return importlib.import_module(name)


def import_object(module_name, object_name):
    module = import_module(module_name)
    return getattr(module, object_name)


def import_object_str(s):
    module, obj = s.rsplit('.', 1)
    return import_object(module, obj)


def get_main_module():
    """Returns main module and module name pair."""
    if not hasattr(main_module, '__file__'):
        # if run from interactive shell
        return None, None
    main_filename = os.path.basename(main_module.__file__)
    module_name, ext = os.path.splitext(main_filename)
    return FakeModule(module=main_module, name=module_name)


FakeModule = namedtuple('MainModule', ['module', 'name'])
main_module = sys.modules['__main__']
