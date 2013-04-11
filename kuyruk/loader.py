import os
import sys
import logging
import importlib
from contextlib import contextmanager

logger = logging.getLogger(__name__)


def get_fully_qualified_function_name(f):
    """For a given function return it's fully qualified name as str."""
    module_name = f.__module__
    if module_name == '__main__':
        module_name = get_main_module()[1]

    return module_name + '.' + f.__name__


def import_task(fully_qualified_function_name):
    """Find and return the function for given function name."""
    logger.debug('fq name: %s', fully_qualified_function_name)
    module_name, func_name = fully_qualified_function_name.rsplit('.', 1)
    module = import_task_module(module_name)
    return getattr(module, func_name)


def import_task_module(module_name):
    """Import module by searching main module, current working directory and
    python path."""
    main_module, main_module_name = get_main_module()
    if module_name == main_module_name:
        return main_module
    else:
        return import_from_cwd(module_name)


def import_from_cwd(module):
    """Import the module from current working directory."""
    with cwd_in_path():
        return importlib.import_module(module)


@contextmanager
def cwd_in_path():
    cwd = os.getcwd()
    if cwd in sys.path:
        yield
    else:
        sys.path.insert(0, cwd)
        try:
            yield
        finally:
            try:
                sys.path.remove(cwd)
            except ValueError:
                pass


def get_main_module():
    """:return main module and module name pair"""
    main_module = sys.modules['__main__']
    filename = os.path.basename(main_module.__file__)
    module_name = os.path.splitext(filename)[0]
    return main_module, module_name
