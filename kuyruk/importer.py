import os
import sys
import logging
import importlib
from collections import namedtuple

logger = logging.getLogger(__name__)


def import_task(module_name, class_name, function_name):
    """Find and return the function for given function name."""
    namespace = import_module(module_name)
    if class_name:
        namespace = getattr(namespace, class_name)
    return getattr(namespace, function_name)


def import_module(module_name):
    """Import module by searching main module, current working directory and
    Python path.

    """
    module, main_module_name = get_main_module()
    if module_name == main_module_name:
        return module
    return importlib.import_module(module_name)


def import_class_str(s):
    module, cls = s.rsplit('.', 1)
    module = import_module(module)
    return getattr(module, cls)


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
