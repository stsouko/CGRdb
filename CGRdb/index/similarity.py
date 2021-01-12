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
from datasketch import MinHash, MinHashLSH
from multiprocessing import Pool
from operator import itemgetter
from pyroaring import BitMap
from tqdm import tqdm
from typing import Collection, Tuple, List, Optional, Union


class SimilarityIndex:
    def __init__(self, fingerprints: Collection[Tuple[int, Collection[int]]],
                 check_threshold: Optional[float] = .7, threshold: float = .6, num_perm: int = 64,
                 n_workers: int = 1, chunk_size: int = 10000):
        """
        MinHashLSH based similarity search index.

        :param fingerprints: pairs of id and fingerprints of data
        :param check_threshold: do additional Tanimoto filtering and descending sorting on original data.
            Required more memory for data storing.
        :param threshold: MinHashLSH threshold
        :param num_perm: MinHashLSH num_perm
        :param n_workers: multiprocessing.Pool processes. Doesn't use Pool when equal to 1
        :param chunk_size: Chunk size of MinHashLSH.insertion_session and Pool.imap
        """
        def get_minhash(fp):
            n, x = fp
            h = MinHash(num_perm=num_perm, hashfunc=hash)
            h.update_batch(x)
            if check_threshold:
                return n, h, BitMap(x)
            return n, h, None

        self._lsh = lsh = MinHashLSH(threshold=threshold, num_perm=num_perm)
        self._fingerprints = fps = {} if check_threshold else None
        self._threshold = check_threshold

        if n_workers != 1:
            with Pool(processes=n_workers) as pool:
                for n, h, b in pool.imap_unordered(get_minhash, tqdm(fingerprints), chunk_size):
                    lsh.insert(n, h, check_duplication=False)
                    if check_threshold:
                        fps[n] = b
        else:
            for fp in tqdm(fingerprints):
                n, h, b = get_minhash(fp)
                lsh.insert(n, h, check_duplication=False)
                if check_threshold:
                    fps[n] = b

    def search(self, query: List[int]) -> Union[List[int], List[Tuple[int, float]]]:
        h = MinHash(num_perm=self._lsh.h, hashfunc=hash)
        h.update_batch(query)
        found = self._lsh.query(h)
        if self._threshold:
            threshold = self._threshold
            fps = self._fingerprints
            bm = BitMap(query)
            return sorted(((x, j) for x in found if (j := bm.jaccard_index(fps[x])) >= threshold),
                          key=itemgetter(1), reverse=True)
        return found


__all__ = ['SimilarityIndex']
