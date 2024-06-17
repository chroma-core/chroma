import inspect
from abc import abstractmethod
from functools import wraps
from typing import Optional, Any, Dict, Callable

from chromadb.config import Component
from chromadb.quota import QuotaProvider, Resource


class RateLimitError(Exception):
    def __init__(self, resource: Resource, quota: int):
        super().__init__(f"rate limit error. resource: {resource} quota: {quota}")
        self.quota = quota
        self.resource = resource


class RateLimitingProvider(Component):
    @abstractmethod
    def is_allowed(self, key: str, quota: int, point: Optional[int] = 1) -> bool:
        """
        Determines if a request identified by `key` can proceed given the current rate limit.

        :param key: The identifier for the requestor (unused in this simplified implementation).
        :param quota: The quota which will be used for bucket size.
        :param point: The number of tokens required to fulfill the request.
        :return: True if the request can proceed, False otherwise.
        """
        pass


def rate_limit(
    subject: str, resource: Resource
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorator(f: Callable[..., Any]) -> Callable[..., Any]:
        args_name = inspect.getfullargspec(f)[0]
        if subject not in args_name:
            raise Exception(
                f'rate_limit decorator have unknown subject "{subject}", available {args_name}'
            )
        key_index = args_name.index(subject)

        @wraps(f)
        def wrapper(self, *args: Any, **kwargs: Dict[Any, Any]) -> Any:
            # If not rate limiting provider is present, just run and return the function.

            if self._system.settings.chroma_rate_limiting_provider_impl is None:
                return f(self, *args, **kwargs)

            if subject in kwargs:
                subject_value = kwargs[subject]
            else:
                if len(args) < key_index:
                    return f(self, *args, **kwargs)
                subject_value = args[key_index - 1]
            key_value = resource.value + "-" + str(subject_value)
            self._system.settings.chroma_rate_limiting_provider_impl
            quota_provider = self._system.require(QuotaProvider)
            rate_limiter = self._system.require(RateLimitingProvider)
            quota = quota_provider.get_for_subject(
                resource=resource, subject=str(subject_value)
            )
            if quota is None:
                return f(self, *args, **kwargs)
            is_allowed = rate_limiter.is_allowed(key_value, quota)
            if is_allowed is False:
                raise RateLimitError(resource=resource.value, quota=quota)
            return f(self, *args, **kwargs)

        return wrapper

    return decorator
