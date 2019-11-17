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
from CGRtools.containers import MoleculeContainer
from datetime import datetime
from LazyPony import LazyEntityMeta
from pony.orm import PrimaryKey, Required, Set, IntArray, FloatArray, composite_key, left_join
from pickle import dumps, loads


class Molecule(metaclass=LazyEntityMeta, database='CGRdb'):
    id = PrimaryKey(int, auto=True)
    _structures = Set('MoleculeStructure')
    # _reactions = Set('MoleculeReaction')

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
    def structure_raw(self):
        """
        matched structure of molecule
        """
        return self.structure_raw_entity.structure

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

    @cached_property
    def structure_entity(self):
        """
        canonical structure entity of molecule
        """
        return self._structures.filter(lambda x: x.last).first()

    @cached_property
    def structure_raw_entity(self):
        """
        matched structure entity of molecule
        """
        raise AttributeError('available in entities from queries results only')

    @cached_property
    def structures_entities(self):
        """
        canonical structure entity of molecule
        """
        s = tuple(self._structures.select())
        if 'structure_entity' not in self.__dict__:  # caching last structure
            self.__dict__['structure_entity'] = next(x for x in s if x.last)
        return s

    def reactions_entities(self, page=1, pagesize=100, product=None, set_all=False):
        """
        list of reactions entities including this molecule. chunks-separated for memory saving

        :param page: slice of reactions
        :param pagesize: maximum number of reactions in list
        :param product: if True - reactions including this molecule in product side returned.
        if None any reactions including this molecule.
        :param set_all: preload all structure combinations
        :return: list of reaction entities
        """
        q = left_join(x.reaction for x in self._database_.MoleculeReaction
                      if x.molecule == self).order_by(lambda x: x.id)
        if product is not None:
            q = q.where(lambda x: x.is_product == product)

        reactions = q.page(page, pagesize)
        if not reactions:
            return []
        if set_all:
            self._database_.Reaction.prefetch_structures(reactions)
        else:
            self._database_.Reaction.prefetch_structure(reactions)
        return list(reactions)


class MoleculeStructure(metaclass=LazyEntityMeta, database='CGRdb'):
    id = PrimaryKey(int, auto=True)
    molecule = Required('Molecule')
    last = Required(bool, default=True)
    signature = Required(bytes, unique=True, volatile=True)
    fingerprint = Required(IntArray, optimistic=False, index=False, lazy=True, volatile=True)
    _structure = Required(bytes, optimistic=False, column='structure')

    def __init__(self, **kwargs):
        structure = kwargs.pop('structure')
        if not isinstance(structure, MoleculeContainer):
            raise TypeError('molecule expected')
        super().__init__(_structure=dumps(structure), **kwargs)

    @cached_property
    def structure(self):
        return loads(self._structure)


class MoleculeSearchCache(metaclass=LazyEntityMeta, database='CGRdb'):
    id = PrimaryKey(int, auto=True)
    signature = Required(bytes)
    operator = Required(str)
    date = Required(datetime, default=datetime.utcnow)
    molecules = Required(IntArray, optimistic=False, index=False)
    structures = Required(IntArray, optimistic=False, index=False)
    tanimotos = Required(FloatArray, optimistic=False, index=False)
    composite_key(signature, operator)


__all__ = ['Molecule', 'MoleculeStructure', 'MoleculeSearchCache']
