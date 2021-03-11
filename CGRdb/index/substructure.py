# -*- coding: utf-8 -*-
#
#  Copyright 2021 Ramil Nugmanov <nougmanoff@protonmail.com>
#  This file is part of CGRdb.
#
#  CGRdb is free software; you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published by
#  the Free Software Foundation; either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with this program; if not, see <https://www.gnu.org/licenses/>.
#
from collections import defaultdict
from operator import itemgetter
from pyroaring import BitMap
from tqdm import tqdm
from typing import Collection, Tuple, List, Union


class SubstructureIndex:
    def __init__(self, fingerprints: Collection[Tuple[int, Collection[int]]], sort_by_tanimoto: bool = True):
        """
        Inverted search index.

        :param fingerprints: pairs of id and fingerprints of data
        :param sort_by_tanimoto: descending sort of found results. Required more memory for data storing.
        """
        self._index = index = defaultdict(BitMap)
        self._fingerprints = fps = {} if sort_by_tanimoto else None
        for n, fp in tqdm(fingerprints):
            for x in fp:
                index[x].add(n)
            if sort_by_tanimoto:
                fps[n] = BitMap(fp)
        self._sizes = {k: len(v) for k, v in index.items()}

    def __getstate__(self):
        return {'index': self._index, 'fingerprints': self._fingerprints}

    def __setstate__(self, state):
        self._index = state['index']
        self._fingerprints = state['fingerprints']
        self._sizes = {k: len(v) for k, v in state['index'].items()}

    def search(self, query: List[int]) -> Union[List[int], List[Tuple[int, float]]]:
        index = self._index
        sizes = self._sizes
        fb, *sq = sorted(query, key=lambda x: sizes.get(x, 0))

        records = index[fb].copy()
        for k in sq:
            records &= index[k]
            if not records:
                return []
        if self._fingerprints:
            bm = BitMap(query)
            fps = self._fingerprints
            return sorted(((x, bm.jaccard_index(fps[x])) for x in records), key=itemgetter(1), reverse=True)
        return list(records)


__all__ = ['SubstructureIndex']
