# -*- coding: utf-8 -*-
#
#  Copyright 2016-2018 Ramil Nugmanov <stsouko@live.ru>
#  Copyright 2018 Salavat Zabirov <zab.sal42@gmail.com>
#  This file is part of CGRdb.
#
#  CGRdb is free software; you can redistribute it and/or modify
#  it under the terms of the GNU Affero General Public License as published by
#  the Free Software Foundation; either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Affero General Public License for more details.
#
#  You should have received a copy of the GNU Affero General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
#
from collections.abc import MutableMapping
from datetime import datetime, timedelta
from typing import Iterator


class QueryCache(MutableMapping):
    def __init__(self) -> None:
        self._dict = {}
        self._expiration = {}

    def __setitem__(self, k: str, v) -> None:
        self._clean_old()
        self._dict[k] = v
        self._expiration[k] = datetime.now()

    def __delitem__(self, k) -> None:
        del self._expiration[k]
        del self._dict[k]

    def __getitem__(self, k: str):
        return self._dict[k]

    def __len__(self) -> int:
        return len(self._dict)

    def __iter__(self) -> Iterator[str]:
        return iter(self._dict)

    def _clean_old(self):
        time_limit = datetime.now() - timedelta(days=1)
        for k, time in list(self._expiration.items()):
            if time < time_limit:
                del self._dict[k]
                del self._expiration[k]


__all__ = ['QueryCache']
