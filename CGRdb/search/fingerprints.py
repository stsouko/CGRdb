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
from CGRtools.containers import MoleculeContainer
from CIMtools.descriptors.fragmentor import Fragmentor
from ..config import (FRAGMENTOR_VERSION, FRAGMENT_TYPE_MOL, FRAGMENT_MIN_MOL, FRAGMENT_MAX_MOL,
                      FRAGMENT_TYPE_CGR, FRAGMENT_MIN_CGR, FRAGMENT_MAX_CGR, FRAGMENT_DYNBOND_CGR, WORKPATH,
                      FP_SIZE, FP_ACTIVE_BITS)


class Fingerprints(object):
    @classmethod
    def descriptors_to_fingerprints(cls, descriptors, bit_array=True):
        return cls.__get_fingerprints(descriptors, bit_array=bit_array)

    @staticmethod
    def __get_fingerprints(df, bit_array=True):
        bits_map = {}
        for fragment in df.columns:
            b = BitArray(md5(fragment.encode()).digest())
            bits_map[fragment] = [b[r * FP_SIZE: (r + 1) * FP_SIZE].uint for r in FP_ACTIVE_BITS]

        result = []
        for _, s in df.iterrows():
            active_bits = set()
            for k, v in s.items():
                if v:
                    active_bits.update(bits_map[k])

            if bit_array:
                fp = BitArray(2 ** FP_SIZE)
                fp.set(True, active_bits)
            else:
                fp = active_bits
            result.append(fp)

        return result

    __cached_fingerprint = None


class FingerprintsMolecule(Fingerprints):
    @classmethod
    def get_fingerprints(cls, structures, bit_array=True):
        f = cls.__fragmentor.get(structures).X
        return cls.descriptors_to_fingerprints(f, bit_array=bit_array)

    __fragmentor = Fragmentor(version=FRAGMENTOR_VERSION, header=False, fragment_type=FRAGMENT_TYPE_MOL,
                              workpath=WORKPATH, min_length=FRAGMENT_MIN_MOL, max_length=FRAGMENT_MAX_MOL,
                              useformalcharge=True)


class FingerprintsReaction(Fingerprints):
    @classmethod
    def get_fingerprints(cls, structures, bit_array=True):
        cgrs = [x if isinstance(x, MoleculeContainer) else cls.get_cgr(x) for x in structures]
        f = cls.__fragmentor.get(cgrs).X
        return cls.descriptors_to_fingerprints(f, bit_array=bit_array)

    __fragmentor = Fragmentor(version=FRAGMENTOR_VERSION, header=False, fragment_type=FRAGMENT_TYPE_CGR,
                              min_length=FRAGMENT_MIN_CGR, max_length=FRAGMENT_MAX_CGR, workpath=WORKPATH,
                              cgr_dynbonds=FRAGMENT_DYNBOND_CGR, useformalcharge=True)


class FingerprintsIndex(object):
    @classmethod
    def get_bits_list(cls, fingerprint):
        return list(fingerprint.findall([1]) if isinstance(fingerprint, BitArray) else fingerprint)

    @property
    def fingerprint(self):
        if self.__cached_fingerprint is None:
            self.__cached_fingerprint = self.__list2bit_array(self.bit_array)
        return self.__cached_fingerprint

    def _flush_fingerprints_cache(self):
        self.__cached_fingerprint = None

    @staticmethod
    def __list2bit_array(bits):
        fp = BitArray(2 ** FP_SIZE)
        fp.set(True, bits)
        return fp

    __cached_fingerprint = None
