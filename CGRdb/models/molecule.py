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
from CGRtools.containers import MoleculeContainer
from pony.orm import PrimaryKey, Required, Optional, Set, Json
from .user import mixin_factory as um
from ..config import DEBUG
from ..management.molecule.merge_molecules import mixin_factory as mmm
from ..management.molecule.new_structure import mixin_factory as nsm
from ..search.fingerprints import FingerprintsMolecule, FingerprintsIndex
from ..search.graph_matcher import mixin_factory as gmm
from ..search.molecule import mixin_factory as msm


def load_tables(db, schema, user_entity, isotope=False, stereo=False):
    class Molecule(db.Entity, FingerprintsMolecule, gmm(isotope, stereo), msm(db), um(user_entity), mmm(db), nsm(db)):
        _table_ = '%s_molecule' % schema if DEBUG else (schema, 'molecule')
        id = PrimaryKey(int, auto=True)
        date = Required(datetime, default=datetime.utcnow)
        user_id = Required(int, column='user')
        _structures = Set('MoleculeStructure')
        reactions = Set('MoleculeReaction')
        properties = Set('MoleculeProperties')
        classes = Set('MoleculeClass')
        special = Optional(Json)

        def __init__(self, structure, user, properties=None, special=None, fingerprint=None, signature=None):
            if signature is None:
                signature = self.get_signature(structure)
            if fingerprint is None:
                fingerprint = self.get_fingerprint(structure, bit_array=False)

            db.Entity.__init__(self, user_id=user.id)

            if properties:
                for p in properties:
                    db.MoleculeProperties(p, self, user)

            if special:
                self.special = special

            self.__last = MoleculeStructure(self, structure, user, fingerprint, signature)

        @classmethod
        def get_signature(cls, structure):
            return structure.get_signature_hash(isotope=isotope, stereo=stereo)

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

        __last = None
        __raw = None

    class MoleculeStructure(db.Entity, FingerprintsIndex, um(user_entity)):
        _table_ = '%s_molecule_structure' % schema if DEBUG else (schema, 'molecule_structure')
        id = PrimaryKey(int, auto=True)
        user_id = Required(int, column='user')
        molecule = Required('Molecule')
        reaction_indexes = Set('ReactionIndex')
        date = Required(datetime, default=datetime.utcnow)
        last = Required(bool, default=True)
        data = Required(Json, optimistic=False)
        signature = Required(bytes, unique=True)
        bit_array = Required(Json, column='bit_list', optimistic=False)

        def __init__(self, molecule, structure, user, fingerprint, signature):
            data = structure.pickle()
            bs = self.get_bits_list(fingerprint)

            db.Entity.__init__(self, data=data, user_id=user.id, signature=signature, bit_array=bs, molecule=molecule)
            self.__cached_structure = structure

        @property
        def structure(self):
            if self.__cached_structure is None:
                self.__cached_structure = MoleculeContainer.unpickle(self.data)
            return self.__cached_structure

        __cached_structure = None

    return Molecule


__all__ = [load_tables.__name__]
