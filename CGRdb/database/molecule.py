# -*- coding: utf-8 -*-
#
#  Copyright 2017, 2018 Ramil Nugmanov <stsouko@live.ru>
#  Copyright 2019 Adelia Fatykhova <adelik21979@gmail.com>
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
from LazyPony import LazyEntityMeta, DoubleLink
from pickle import dumps, loads
from pony.orm import PrimaryKey, Required, Optional, Set, Json, IntArray, FloatArray
from ..search import FingerprintMolecule, SearchMolecule


class Molecule(SearchMolecule, metaclass=LazyEntityMeta, database='CGRdb'):
    id = PrimaryKey(int, auto=True)
    date = Required(datetime, default=datetime.utcnow)
    user = DoubleLink(Required('User', reverse='molecules'), Set('Molecule'))
    _structures = Set('MoleculeStructure')
    reactions = Set('MoleculeReaction')
    special = Optional(Json)

    def __init__(self, structure, user, special=None):
        super().__init__(user=user)
        self._cached_structure = self._database_.MoleculeStructure(self, structure, user)
        self._cached_structures_all = (self._cached_structure,)
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
    def structures_all(self):
        return tuple(x.structure for x in self.all_editions)

    @property
    def last_edition(self):
        if self._cached_structure is None:
            self._cached_structure = self._structures.filter(lambda x: x.last).first()
        return self._cached_structure

    @property
    def raw_edition(self):
        if self._cached_structure_raw is not None:
            return self._cached_structure_raw
        raise AttributeError('available in entities from queries results only')

    @property
    def all_editions(self):
        if self._cached_structures_all is None:
            s = tuple(self._structures.select())
            self._cached_structures_all = s
            if self._cached_structure is None:
                self._cached_structure = next(x for x in s if x.last)
        return self._cached_structures_all

    _cached_structure = _cached_structure_raw = _cached_structures_all = None


class MoleculeStructure(FingerprintMolecule, metaclass=LazyEntityMeta, database='CGRdb'):
    id = PrimaryKey(int, auto=True)
    user = DoubleLink(Required('User', reverse='molecule_structures'), Set('MoleculeStructure'))
    molecule = Required('Molecule')
    date = Required(datetime, default=datetime.utcnow)
    last = Required(bool, default=True)
    data = Required(bytes, optimistic=False)
    signature = Required(bytes, unique=True)
    bit_array = Required(IntArray, optimistic=False, index=False, lazy=True)

    def __init__(self, molecule, structure, user):
        super().__init__(molecule=molecule, data=dumps(structure), user=user, signature=bytes(structure),
                         bit_array=self.get_fingerprint(structure))

    @property
    def structure(self):
        if self.__cached_structure is None:
            self.__cached_structure = loads(self.data)
        return self.__cached_structure

    __cached_structure = None


class MoleculeSearchCache(metaclass=LazyEntityMeta, database='CGRdb'):
    id = PrimaryKey(int, auto=True)
    signature = Required(bytes)
    molecules = Required(IntArray, optimistic=False, index=False)
    structures = Required(IntArray, optimistic=False, index=False)
    tanimotos = Required(FloatArray, optimistic=False, index=False)
    date = Required(datetime)
    operator = Required(str)


__all__ = ['Molecule', 'MoleculeStructure', 'MoleculeSearchCache']
