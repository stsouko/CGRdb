# -*- coding: utf-8 -*-
#
#  Copyright 2017-2019 Ramil Nugmanov <stsouko@live.ru>
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
from CachedMethods import cached_property
from datetime import datetime
from LazyPony import LazyEntityMeta, DoubleLink
from pony.orm import PrimaryKey, Required, Set, IntArray, FloatArray
from pickle import dumps, loads
from ..search import FingerprintMolecule, SearchMolecule


class Molecule(SearchMolecule, metaclass=LazyEntityMeta, database='CGRdb'):
    id = PrimaryKey(int, auto=True)
    date = Required(datetime, default=datetime.utcnow)
    user = DoubleLink(Required('User', reverse='molecules'), Set('Molecule'))
    _structures = Set('MoleculeStructure')
    reactions = Set('MoleculeReaction')

    def __init__(self, structure, user):
        super().__init__(user=user)
        self.__dict__['last_edition'] = x = self._database_.MoleculeStructure(self, structure, user)
        self.__dict__['all_editions'] = (x,)

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

    @cached_property
    def structure(self):
        """
        canonical structure of molecule
        """
        return self.last_edition.structure

    @cached_property
    def structure_raw(self):
        """
        matched structure of molecule
        """
        return self.raw_edition.structure

    @cached_property
    def structures(self):
        """
        all structures of molecule
        """
        return tuple(x.structure for x in self.all_editions)

    @cached_property
    def last_edition(self):
        """
        canonical structure entity of molecule
        """
        return self._structures.filter(lambda x: x.last).first()

    @cached_property
    def raw_edition(self):
        """
        matched structure entity of molecule
        """
        raise AttributeError('available in entities from queries results only')

    @cached_property
    def all_editions(self):
        """
        canonical structure entity of molecule
        """
        s = tuple(self._structures.select())
        if 'last_edition' not in self.__dict__:  # caching last structure
            self.__dict__['last_edition'] = next(x for x in s if x.last)
        return s


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

    @cached_property
    def structure(self):
        return loads(self.data)


class MoleculeSearchCache(metaclass=LazyEntityMeta, database='CGRdb'):
    id = PrimaryKey(int, auto=True)
    signature = Required(bytes)
    molecules = Required(IntArray, optimistic=False, index=False)
    structures = Required(IntArray, optimistic=False, index=False)
    tanimotos = Required(FloatArray, optimistic=False, index=False)
    date = Required(datetime, default=datetime.utcnow)
    operator = Required(str)


__all__ = ['Molecule', 'MoleculeStructure', 'MoleculeSearchCache']
