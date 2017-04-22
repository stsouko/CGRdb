# -*- coding: utf-8 -*-
#
#  Copyright 2017 Ramil Nugmanov <stsouko@live.ru>
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
from hashlib import md5
from bitstring import BitArray


class Fingerprints(object):
    def __init__(self, size, active_bits=2):
        self.__size = size
        self.__bits = list(range(active_bits))

    def get_fingerprints(self, df, bit_array=True):
        bits_map = {}
        for fragment in df.columns:
            b = BitArray(md5(fragment.encode()).digest())
            bits_map[fragment] = [b[r * self.__size: (r + 1) * self.__size].uint for r in self.__bits]

        result = []
        for _, s in df.iterrows():
            active_bits = set()
            for k, v in s.items():
                if v:
                    active_bits.update(bits_map[k])

            if bit_array:
                fp = BitArray(2 ** self.__size)
                fp.set(True, active_bits)
            else:
                fp = active_bits
            result.append(fp)

        return result
