import os
import sys
import importlib
from contextlib import contextmanager


@contextmanager
def cwd_in_path():
    cwd = os.getcwd()
    if cwd in sys.path:
        yield
    else:
        sys.path.insert(0, cwd)
        try:
            yield cwd
        finally:
            try:
                sys.path.remove(cwd)
            except ValueError:  # pragma: no cover
                pass


def import_from_cwd(module):
    with cwd_in_path():
        return importlib.import_module(module)


def import_task_module(module):
    return import_from_cwd(module)


def import_task(fully_qualified_function_name):
    module_name, func_name = split_function_name(fully_qualified_function_name)
    module = import_task_module(module_name)
    return getattr(module, func_name)


def split_function_name(name):
    func_name, module_name = map(reverse_str, reverse_str(name).split('.', 1))
    return module_name, func_name


def reverse_str(s):
    return s[::-1]
