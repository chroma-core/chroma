from typing import Callable, ParamSpec, Concatenate, TypeVar

Param = ParamSpec("Param")
RetType = TypeVar("RetType")
OriginalFunc = Callable[Param, RetType]
DecoratedFunc = Callable[Concatenate[str, Param], RetType]

def get_authenticated_user(): return "John"

def inject_user() -> Callable[[OriginalFunc], DecoratedFunc]:
    def decorator(func: OriginalFunc) -> DecoratedFunc:
        def wrapper(*args, **kwargs) -> RetType:
            user = get_authenticated_user()
            if user is None:
                raise Exception("Don't!")
            return func(*args, user, **kwargs)  # <- call signature modified

        return wrapper

    return decorator


@inject_user()
def foo(a: int, username: str) -> bool:
    print(username)
    return bool(a % 2)


foo(2)
