from collections.abc import MutableMapping
from datetime import datetime, timedelta
from typing import Iterator


class MoleculeCache(MutableMapping):
    def __init__(self) -> None:
        self._dict = {}
        self._expiration = {}

    def __setitem__(self, k: str, v) -> None:
        self._clean_old()
        self._dict[k] = v
        self._expiration[datetime.now()] = k

    def __delitem__(self, k) -> None:
        del self._dict[k]

    def __getitem__(self, k: str):
        return self._dict[k]

    def __len__(self) -> int:
        return len(self._dict)

    def __iter__(self) -> Iterator[str]:
        return iter(self._dict)

    def _clean_old(self):
        for time in self._expiration:
            if time < datetime.now() - timedelta(days=1):
                del self._dict[self._expiration[time]]
