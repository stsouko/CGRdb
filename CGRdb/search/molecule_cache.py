from collections.abc import MutableMapping
from typing import Iterator


class MoleculeCache(MutableMapping):

    def __init__(self) -> None:
        self._dict = {}

    def __setitem__(self, k: str, v) -> None:
        self._dict[k] = v

    def __delitem__(self, k) -> None:
        del self._dict[k]

    def __getitem__(self, k: str):
        return self._dict[k]

    def __len__(self) -> int:
        return len(self._dict)

    def __iter__(self) -> Iterator[str]:
        return iter(self._dict)
