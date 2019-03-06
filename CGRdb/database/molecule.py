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
from pony.orm import PrimaryKey, Required, Set, IntArray, FloatArray, composite_key, left_join
from pickle import dumps, loads
from ..search import FingerprintMolecule, SearchMolecule


class Molecule(SearchMolecule, metaclass=LazyEntityMeta, database='CGRdb'):
    id = PrimaryKey(int, auto=True)
    date = Required(datetime, default=datetime.utcnow)
    user = DoubleLink(Required('User', reverse='molecules'), Set('Molecule'))
    _structures = Set('MoleculeStructure')
    _reactions = Set('MoleculeReaction')

    def __init__(self, structure, user):
        super().__init__(user=user)
        self.__dict__['structure_entity'] = x = self._database_.MoleculeStructure(self, structure, user)
        self.__dict__['structures_entities'] = (x,)

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
            self._database_.Reaction.load_structures_combinations(reactions)
        else:
            self._database_.Reaction.load_structures(reactions)
        return list(reactions)


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
    operator = Required(str)
    date = Required(datetime, default=datetime.utcnow)
    molecules = Required(IntArray, optimistic=False, index=False)
    structures = Required(IntArray, optimistic=False, index=False)
    tanimotos = Required(FloatArray, optimistic=False, index=False)
    composite_key(signature, operator)


__all__ = ['Molecule', 'MoleculeStructure', 'MoleculeSearchCache']
