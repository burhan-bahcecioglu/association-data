"""
Module Runner for association modules.
"""
import importlib
import os
import sys

_USAGE = """
Command-line usage
------------------

$ module_runner.py <module> <class> [arg=value ...]

`class` should be a `module` declared in `association` library.

o Invoking a module:
    $ module_runner.py association.etl store run_date=2021-08-03
"""

if __name__ == "__main__":
    assert sys.argv
    # First value in argv is the script name if ModuleRunner is
    # invoked from command line. Otherwise, assume running in an
    # interactive environment and parse only arguments.
    if sys.argv[0].endswith(".py"):
        prog_name = os.path.basename(sys.argv[0])
    else:
        prog_name = None
        sys.argv.insert(0, None)
    # Try parsing required arguments and raise an error if they
    # are not given.
    try:
        module_name = sys.argv[1]
    except IndexError as exc:
        raise IndexError(_USAGE) from exc

    module = importlib.import_module(module_name)
    main_function = getattr(module, "main")
    main_function()
