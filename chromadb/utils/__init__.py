import importlib
import weakref
from typing import Type, TypeVar, cast

C = TypeVar("C")


def get_class(fqn: str, type: Type[C]) -> Type[C]:
    """Given a fully qualifed class name, import the module and return the class"""
    module_name, class_name = fqn.rsplit(".", 1)
    module = importlib.import_module(module_name)
    cls = getattr(module, class_name)
    return cast(Type[C], cls)

def once(func):
    """ 
    Decorator that limits a function to one call. 
    """
    instance_ref = None
    def wrapper(*args, **kwargs):
        nonlocal instance_ref
        # if first run or the object created has been garbage collected. 
        if instance_ref is None or instance_ref() is None:
            instance = func(*args, **kwargs)
            instance_ref = weakref.ref(instance) 
            return instance
        else:
            raise RuntimeError(f"Function {func.__name__} has already been called. \
            You should try and use only one instance of the Client.")
            return None

    return wrapper