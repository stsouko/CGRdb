# -*- coding: utf-8 -*-
#
#  Copyright 2017, 2018 Ramil Nugmanov <stsouko@live.ru>
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
from datetime import datetime
from CGRtools.containers import CGRContainer
from pony.orm import PrimaryKey, Required, Optional, Set, Json, IntArray, FloatArray
from .user import mixin_factory as um
from ..management.molecule.merge_molecules import mixin_factory as mmm
from ..management.molecule.new_structure import mixin_factory as nsm
from ..search.fingerprints import molecule_mixin_factory as mfp
from ..search.graph_matcher import mixin_factory as gmm
from ..search.molecule import mixin_factory as msm


def load_tables(db, schema, user_entity, fragmentor_version, fragment_type, fragment_min, fragment_max, fp_size,
                fp_active_bits, fp_count, workpath='.', isotope=False, stereo=False, extralabels=False):

    FingerprintsMolecule, FingerprintsIndex = mfp(fragmentor_version, fragment_type, fragment_min, fragment_max,
                                                  fp_size, fp_active_bits, fp_count, workpath)

    class Molecule(db.Entity, FingerprintsMolecule, gmm(isotope, stereo, extralabels), msm(db, schema), um(user_entity),
                   mmm(db), nsm(db)):
        _table_ = (schema, 'molecule')
        id = PrimaryKey(int, auto=True)
        date = Required(datetime, default=datetime.utcnow)
        user_id = Required(int, column='user')
        _structures = Set('MoleculeStructure')
        reactions = Set('MoleculeReaction')
        metadata = Set('MoleculeProperties')
        classes = Set('MoleculeClass')
        special = Optional(Json)

        def __init__(self, structure, user, metadata=None, special=None, fingerprint=None, signature=None):
            if signature is None:
                signature = self.get_signature(structure)
            if fingerprint is None:
                fingerprint = self.get_fingerprint(structure, bit_array=False)

            db.Entity.__init__(self, user_id=user.id)

            if metadata:
                for p in metadata:
                    db.MoleculeProperties(p, self, user)

            if special:
                self.special = special

            self.__last = MoleculeStructure(self, structure, user, fingerprint, signature)

        @classmethod
        def get_signature(cls, structure):
            return structure.get_signature_hash(isotope=isotope, stereo=stereo, hybridization=extralabels,
                                                neighbors=extralabels)

        @property
        def structure_raw(self):
            return self.raw_edition.structure

        @property
        def structure(self):
            return self.last_edition.structure

        @property
        def fingerprint_raw(self):
            return self.raw_edition.fingerprint

        @property
        def fingerprint(self):
            return self.last_edition.fingerprint

        @property
        def last_edition(self):
            if self.__last is None:
                self.__last = self._structures.filter(lambda x: x.last).first()
            return self.__last

        @property
        def raw_edition(self):
            assert self.__raw is not None, 'available in entities from queries results only'
            return self.__raw

        @last_edition.setter
        def last_edition(self, structure):
            self.__last = structure

        @raw_edition.setter
        def raw_edition(self, structure):
            self.__raw = structure

        def add_metadata(self, data, user):
            return db.MoleculeProperties(data, self, user)

        __last = None
        __raw = None

    class MoleculeStructure(db.Entity, FingerprintsIndex, um(user_entity)):
        _table_ = (schema, 'molecule_structure')
        id = PrimaryKey(int, auto=True)
        user_id = Required(int, column='user')
        molecule = Required('Molecule')
        reaction_indexes = Set('ReactionIndex')
        date = Required(datetime, default=datetime.utcnow)
        last = Required(bool, default=True)
        data = Required(Json, optimistic=False)
        signature = Required(bytes, unique=True)
        bit_array = Required(IntArray, optimistic=False, index=False, lazy=True)

        def __init__(self, molecule, structure, user, fingerprint, signature):
            data = structure.pickle()
            bs = self.get_bits_list(fingerprint)

            db.Entity.__init__(self, data=data, user_id=user.id, signature=signature, bit_array=bs, molecule=molecule)
            self.__cached_structure = structure

        @property
        def structure(self):
            if self.__cached_structure is None:
                self.__cached_structure = CGRContainer.unpickle(self.data)
            return self.__cached_structure

        __cached_structure = None

    class SearchCache(db.Entity):
        _table_ = (schema, 'molecule_structure_save')
        id = PrimaryKey(int, auto=True)
        signature = Required(bytes)
        molecules = Required(IntArray)
        structures = Required(IntArray)
        tanimotos = Required(FloatArray)
        date = Required(datetime)
        operator = Required(str)

    return Molecule


__all__ = [load_tables.__name__]
