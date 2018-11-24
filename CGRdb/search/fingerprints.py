# -*- coding: utf-8 -*-
#
#  Copyright 2017, 2018 Ramil Nugmanov <stsouko@live.ru>
#  Copyright 2018 Adelia Fatykhova <adelik21979@gmail.com>
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
from CGRtools.containers import ReactionContainer, MergedReaction
from CIMtools.preprocessing import Fragmentor
from pandas import DataFrame


def _mixin_factory(fp_size, fp_count, fp_active_bits):
    fp_count_i = fp_count + 1

    class Fingerprints:
        @classmethod
        def descriptors_to_fingerprints(cls, descriptors, bit_array=True):
            return cls.__get_fingerprints(descriptors, bit_array=bit_array)

        @classmethod
        def get_fingerprints(cls, structures, bit_array=True):
            f = cls._get_descriptors(structures)
            return cls.descriptors_to_fingerprints(f, bit_array=bit_array)

        @classmethod
        def get_fingerprint(cls, structures, bit_array=True):
            return cls.get_fingerprints([structures], bit_array)[0]

        @staticmethod
        def __get_fingerprints(df, bit_array=True):
            bits_map = {}
            for f in df.columns:
                prev = []
                for i in range(1, fp_count_i):
                    b = BitArray(md5(('{}_{}'.format(i, f)).encode()).digest())
                    bits_map[(f, i)] = prev = [b[r * fp_size: (r + 1) * fp_size].uint for r in
                                               range(fp_active_bits)] + prev

            result = []
            for _, s in df.iterrows():
                active_bits = set()
                for k, v in s.items():
                    if v:
                        active_bits.update(bits_map[(k, v if v < fp_count_i else fp_count)])

                if bit_array:
                    fp = BitArray(2 ** fp_size)
                    fp.set(True, active_bits)
                else:
                    fp = active_bits
                result.append(fp)

            return result

        __cached_fingerprint = None

    class FingerprintsIndex:
        @staticmethod
        def get_bits_list(fingerprint):
            return list(fingerprint.findall([1]) if isinstance(fingerprint, BitArray) else fingerprint)

        @property
        def fingerprint(self):
            if self.__cached_fingerprint is None:
                self.__cached_fingerprint = self.__list2bit_array(self.bit_array)
            return self.__cached_fingerprint

        @fingerprint.setter
        def fingerprint(self, fingerprint):
            self.bit_array = self.get_bits_list(fingerprint)
            self._flush_fingerprints_cache()

        def _flush_fingerprints_cache(self):
            self.__cached_fingerprint = None

        @staticmethod
        def __list2bit_array(bits):
            fp = BitArray(2 ** fp_size)
            fp.set(True, bits)
            return fp

        __cached_fingerprint = None

    return Fingerprints, FingerprintsIndex


def molecule_mixin_factory(fragmentor_version, fragment_type, fragment_min, fragment_max, fp_size, fp_active_bits,
                           fp_count, workpath):
    Fingerprints, FingerprintsIndex = _mixin_factory(fp_size, fp_count, fp_active_bits)

    class FingerprintsMolecule(Fingerprints):
        @classmethod
        def _get_descriptors(cls, structures):
            return cls.__fragmentor.fit_transform(structures)

        __fragmentor = Fragmentor(version=fragmentor_version, header=False, fragment_type=fragment_type,
                                  workpath=workpath, min_length=fragment_min, max_length=fragment_max,
                                  useformalcharge=True)

    return FingerprintsMolecule, FingerprintsIndex


def reaction_mixin_factory(fragmentor_version, fragment_type, fragment_min, fragment_max, fragment_dynbond, fp_size,
                           fp_active_bits, fp_count, workpath):
    Fingerprints, FingerprintsIndex = _mixin_factory(fp_size, fp_count, fp_active_bits)

    class FingerprintsReaction(Fingerprints):
        @classmethod
        def _get_descriptors(cls, structures):
            cgrs = [cls.get_cgr(x) if isinstance(x, (ReactionContainer, MergedReaction)) else x for x in structures]
            try:
                f = cls.__fragmentor.fit_transform(cgrs)
            except:
                f = DataFrame(index=range(len(structures)))
            return f

        __fragmentor = Fragmentor(version=fragmentor_version, header=False, fragment_type=fragment_type,
                                  min_length=fragment_min, max_length=fragment_max, workpath=workpath,
                                  cgr_dynbonds=fragment_dynbond, useformalcharge=True)

    return FingerprintsReaction, FingerprintsIndex


__all__ = [molecule_mixin_factory.__name__, reaction_mixin_factory.__name__]
