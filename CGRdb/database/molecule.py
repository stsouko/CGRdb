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
from CGRtools.containers import MoleculeContainer, QueryContainer
from compress_pickle import dumps, loads
from datetime import datetime
from LazyPony import LazyEntityMeta
from pony.orm import PrimaryKey, Required, Set, IntArray, FloatArray, composite_key, left_join, select, raw_sql


class Molecule(metaclass=LazyEntityMeta, database='CGRdb'):
    id = PrimaryKey(int, auto=True)
    _structures = Set('MoleculeStructure')
    _reactions = Set('MoleculeReaction')

    def __init__(self, structure):
        super().__init__()
        self.__dict__['structure_entity'] = x = self._database_.MoleculeStructure(molecule=self, structure=structure)
        self.__dict__['structures_entities'] = (x,)

    def __str__(self):
        """
        signature of canonical structure of molecule
        """
        return str(self.structure)

    def __bytes__(self):
        """
        hashed signature of canonical structure of molecule
        """
        return bytes(self.structure)

    @cached_property
    def structure(self):
        """
        canonical structure of molecule
        """
        return self.structure_entity.structure

    @cached_property
    def structures(self):
        """
        all structures of molecule
        """
        return tuple(x.structure for x in self.structures_entities)

    def reactions(self, page=1, pagesize=100, product=None):
        """
        list of reactions including this molecule. chunks-separated for memory saving

        :param page: slice of reactions
        :param pagesize: maximum number of reactions in list
        :param product: if True - reactions including this molecule in product side returned.
        if None any reactions including this molecule.
        :return: list of reactions
        """
        return [x.structure for x in self.reactions_entities(page, pagesize, product)]

    @classmethod
    def structure_exists(cls, structure):
        if not isinstance(structure, MoleculeContainer):
            raise TypeError('Molecule expected')
        elif not len(structure):
            raise ValueError('empty query')

        return cls._database_.MoleculeStructure.exists(signature=bytes(structure))

    @classmethod
    def find_structure(cls, structure):
        if not isinstance(structure, MoleculeContainer):
            raise TypeError('Molecule expected')
        elif not len(structure):
            raise ValueError('empty query')

        ms = cls._database_.MoleculeStructure.get(signature=bytes(structure))
        if ms:
            molecule = ms.molecule
            if ms.is_canonic:  # save if structure is canonical
                molecule.__dict__['structure_entity'] = ms
            return molecule

    @classmethod
    def find_substructures(cls, structure):
        """
        substructure search

        substructure search is 2-step process. first step is screening procedure. next step is isomorphism testing.

        :param structure: CGRtools MoleculeContainer or QueryContainer
        :return: MoleculeSearchCache object with all found molecules or None
        """
        if not isinstance(structure, (MoleculeContainer, QueryContainer)):
            raise TypeError('Molecule or Query expected')
        elif not len(structure):
            raise ValueError('empty query')

        structure = dumps(structure, compression='gzip').hex()
        ci, fnd = cls._database_.select(f" * FROM test.cgrdb_search_substructure_molecules('\\x{structure}'::bytea)")[0]
        if fnd:
            c = cls._database_.MoleculeSearchCache[ci]
            c.__dict__['_size'] = fnd
            return c

    @classmethod
    def find_similar(cls, structure):
        """
        similarity search

        :param structure: CGRtools MoleculeContainer
        :return: MoleculeSearchCache object with all found molecules or None
        """
        if not isinstance(structure, MoleculeContainer):
            raise TypeError('Molecule expected')
        elif not len(structure):
            raise ValueError('empty query')

        structure = dumps(structure, compression='gzip').hex()
        ci, fnd = cls._database_.select(f" * FROM test.cgrdb_search_similar_molecules('\\x{structure}'::bytea)")[0]
        if fnd:
            c = cls._database_.MoleculeSearchCache[ci]
            c.__dict__['_size'] = fnd
            return c

    @cached_property
    def structure_entity(self):
        """
        canonical structure entity of molecule
        """
        return self._structures.filter(lambda x: x.is_canonic).first()

    @cached_property
    def structures_entities(self):
        """
        canonical structure entity of molecule
        """
        s = tuple(self._structures.select())
        if 'structure_entity' not in self.__dict__:  # caching canonic structure
            self.__dict__['structure_entity'] = next(x for x in s if x.is_canonic)
        return s

    def reactions_entities(self, page=1, pagesize=100, product=None):
        """
        list of reactions entities including this molecule. chunks-separated for memory saving

        :param page: slice of reactions
        :param pagesize: maximum number of reactions in list
        :param product: if True - reactions including this molecule in product side returned.
        if None any reactions including this molecule.
        :return: list of reaction entities
        """
        q = left_join(x.reaction for x in self._database_.MoleculeReaction
                      if x.molecule == self).order_by(lambda x: x.id)
        if product is not None:
            q = q.where(lambda x: x.is_product == product)

        reactions = q.page(page, pagesize)
        if not reactions:
            return []
        self._database_.Reaction.prefetch_structure(reactions)
        return list(reactions)


class MoleculeStructure(metaclass=LazyEntityMeta, database='CGRdb'):
    id = PrimaryKey(int, auto=True)
    molecule = Required('Molecule')
    is_canonic = Required(bool, default=True)
    signature = Required(bytes, unique=True, volatile=True, lazy=True)
    fingerprint = Required(IntArray, optimistic=False, index=False, lazy=True, volatile=True)
    _structure = Required(bytes, optimistic=False, column='structure')

    def __init__(self, **kwargs):
        structure = kwargs.pop('structure')
        if not isinstance(structure, MoleculeContainer):
            raise TypeError('molecule expected')
        super().__init__(_structure=dumps(structure, compression='gzip'), **kwargs)

    @cached_property
    def structure(self):
        return loads(self._structure, compression='gzip')


class MoleculeSearchCache(metaclass=LazyEntityMeta, database='CGRdb'):
    id = PrimaryKey(int, auto=True)
    signature = Required(bytes)
    operator = Required(str)
    date = Required(datetime, default=datetime.utcnow)
    _molecules = Required(IntArray, optimistic=False, index=False, column='molecules', lazy=True)
    _tanimotos = Required(FloatArray, optimistic=False, index=False, column='tanimotos', lazy=True, sql_type='real[]')
    composite_key(signature, operator)

    def molecules(self, page=1, pagesize=100):
        if page < 1:
            raise ValueError('page should be greater or equal than 1')
        elif pagesize < 1:
            raise ValueError('pagesize should be greater or equal than 1')

        start = (page - 1) * pagesize
        end = start + pagesize
        mis = select(x._molecules[start:end] for x in self.__class__ if x.id == self.id).first()
        if not mis:
            return []

        # preload molecules
        ms = {x.id: x for x in self._database_.Molecule.select(lambda x: x.id in mis)}

        # preload molecules canonical structures
        for x in self._database_.MoleculeStructure.select(lambda x: x.molecule.id in mis and x.is_canonic):
            x.molecule.__dict__['structure_entity'] = x

        return [ms[x] for x in mis]

    def tanimotos(self, page=1, pagesize=100):
        if page < 1:
            raise ValueError('page should be greater or equal than 1')
        elif pagesize < 1:
            raise ValueError('pagesize should be greater or equal than 1')

        start = (page - 1) * pagesize
        end = start + pagesize
        sts = select(x._tanimotos[start:end] for x in self.__class__ if x.id == self.id).first()
        return list(sts)

    def __len__(self):
        return self._size

    @cached_property
    def _size(self):
        return select(raw_sql('array_length(x.molecules, 1)') for x in self.__class__ if x.id == self.id).first()


__all__ = ['Molecule', 'MoleculeStructure', 'MoleculeSearchCache']
