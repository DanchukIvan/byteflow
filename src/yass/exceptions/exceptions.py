__all__ = [
    "CriticalException",
    "EndOfResource",
    "InterceptedException",
    "YassException",
]


class YassException(BaseException): ...


class InterceptedException(YassException): ...


class CriticalException(YassException): ...


class EndOfResource(InterceptedException): ...
