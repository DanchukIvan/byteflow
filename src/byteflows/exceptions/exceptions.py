__all__ = [
    "CriticalException",
    "EndOfResource",
    "InterceptedException",
    "ByteflowException",
]


class ByteflowException(BaseException): ...


class InterceptedException(ByteflowException): ...


class CriticalException(ByteflowException): ...


class EndOfResource(InterceptedException): ...
