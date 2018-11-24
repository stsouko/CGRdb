from collections.abc import MutableMapping
from typing import Iterator


class MoleculeCache(MutableMapping):

    def __init__(self) -> None:
        super().__init__()
        self._dict = {}

    def __setitem__(self, k: str, v) -> None:
        self._dict.__setitem__(k, v)

    def __delitem__(self, v) -> None:
        self._dict.__delitem__(v)

    def __getitem__(self, k: str):
        return self._dict[k]

    def __len__(self) -> int:
        return self._dict.__len__()

    def __iter__(self) -> Iterator[str]:
        return self._dict.__iter__()
