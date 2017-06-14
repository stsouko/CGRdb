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
from bitstring import BitArray
from CGRtools.reactor import CGRreactor
from ..config import FP_SIZE, FP_ACTIVE_BITS, DATA_ISOTOPE, DATA_STEREO
from ..utils.fingerprints import Fingerprints


class ReactionMoleculeMixin(object):
    __fingerprints = Fingerprints(FP_SIZE, active_bits=FP_ACTIVE_BITS)
    __cgr_reactor = CGRreactor(isotope=DATA_ISOTOPE, stereo=DATA_STEREO)

    @classmethod
    def descriptors_to_fingerprints(cls, descriptors, bit_array=True):
        return cls.__fingerprints.get_fingerprints(descriptors, bit_array=bit_array)

    @classmethod
    def get_cgr_matcher(cls, g, h):
        return cls.__cgr_reactor.get_cgr_matcher(g, h)

    @classmethod
    def match_structures(cls, g, h):
        return next(cls.get_cgr_matcher(g, h).isomorphisms_iter())


class FingerprintMixin(object):
    @property
    def fingerprint(self):
        if self.__cached_fingerprint is None:
            fp = self.__list2bitarray(self.bit_array)
            self.__cached_fingerprint = fp
        return self.__cached_fingerprint

    @fingerprint.setter
    def fingerprint(self, fingerprint):
        self.__cached_fingerprint = fingerprint

    @staticmethod
    def __list2bitarray(bits):
        fp = BitArray(2 ** FP_SIZE)
        fp.set(True, bits)
        return fp

    @classmethod
    def _init_fingerprint(cls, fingerprint):
        if not isinstance(fingerprint, BitArray):
            bit_set = list(fingerprint)
            fingerprint = cls.__list2bitarray(bit_set)
        else:
            bit_set = list(fingerprint.findall([1]))

        return fingerprint, bit_set

    __cached_fingerprint = None
