import sys


# https://stackoverflow.com/a/39662359
def running_inside_notebook() -> bool:
    try:
        shell = get_ipython().__class__.__name__  # type: ignore
        if shell == "ZMQInteractiveShell":
            return True  # Jupyter notebook or qtconsole
        elif shell == "TerminalInteractiveShell":
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False  # Probably standard Python interpreter


def running_in_interactive_environment() -> bool:
    return running_inside_notebook() or sys.stdout.isatty()
