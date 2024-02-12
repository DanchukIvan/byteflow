from abc import abstractmethod
from collections.abc import Iterable

from ..core import YassCore


class BaseStorageManager(YassCore):
    @abstractmethod
    def put(self, content: Iterable[bytes]) -> None:
        ...

    @abstractmethod
    def merge_to_backend(self):
        ...
