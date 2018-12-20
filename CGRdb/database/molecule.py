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
from CGRtools.containers.common import BaseContainer
from LazyPony import LazyEntityMeta, DoubleLink
from pony.orm import PrimaryKey, Required, Optional, Set, Json, IntArray, FloatArray
from ..search import FingerprintMolecule


class Molecule(metaclass=LazyEntityMeta, database='CGRdb'):
    id = PrimaryKey(int, auto=True)
    date = Required(datetime, default=datetime.utcnow)
    user = DoubleLink(Required('User', reverse='molecules'), Set('Molecule'))
    _structures = Set('MoleculeStructure')
    reactions = Set('MoleculeReaction')
    special = Optional(Json)

    def __init__(self, structure, user, special=None):
        super().__init__(user=user)
        self.__cached_structure = self._database_.MoleculeStructure(self, structure, user)
        if special:
            self.special = special

    def __str__(self):
        """
        signature of last edition of molecule
        """
        return str(self.structure)

    def __bytes__(self):
        """
        hashed signature of last edition of molecule
        """
        return bytes(self.structure)

    @property
    def structure(self):
        return self.last_edition.structure

    @property
    def structure_raw(self):
        return self.raw_edition.structure

    @property
    def last_edition(self):
        if self.__cached_structure is None:
            self.__cached_structure = self._structures.filter(lambda x: x.last).first()
        return self.__cached_structure

    @property
    def raw_edition(self):
        if self.__cached_structure_raw is not None:
            return self.__cached_structure_raw
        raise AttributeError('available in entities from queries results only')

    @last_edition.setter
    def last_edition(self, structure):
        self.__cached_structure = structure

    @raw_edition.setter
    def raw_edition(self, structure):
        self.__cached_structure_raw = structure

    __cached_structure = __cached_structure_raw = None


class MoleculeStructure(FingerprintMolecule, metaclass=LazyEntityMeta, database='CGRdb'):
    id = PrimaryKey(int, auto=True)
    user = DoubleLink(Required('User', reverse='molecule_structures'), Set('MoleculeStructure'))
    molecule = Required('Molecule')
    reaction_indexes = Set('ReactionIndex')
    date = Required(datetime, default=datetime.utcnow)
    last = Required(bool, default=True)
    data = Required(Json, optimistic=False)
    signature = Required(bytes, unique=True)
    bit_array = Required(IntArray, optimistic=False, index=False, lazy=True)

    def __init__(self, molecule, structure, user):
        super().__init__(molecule=molecule, data=structure.pickle(), user=user, signature=bytes(structure),
                         bit_array=self.get_fingerprint(structure))

    @property
    def structure(self):
        if self.__cached_structure is None:
            self.__cached_structure = BaseContainer.unpickle(self.data)
        return self.__cached_structure

    __cached_structure = None


class MoleculeSearchCache(metaclass=LazyEntityMeta, database='CGRdb'):
    id = PrimaryKey(int, auto=True)
    signature = Required(bytes)
    molecules = Required(IntArray)
    structures = Required(IntArray)
    tanimotos = Required(FloatArray)
    date = Required(datetime)
    operator = Required(str)


__all__ = ['Molecule', 'MoleculeStructure', 'MoleculeSearchCache']
