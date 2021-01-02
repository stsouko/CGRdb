# -*- coding: utf-8 -*-
#
#  Copyright 2017-2021 Ramil Nugmanov <nougmanoff@protonmail.com>
#  This file is part of CGRdb.
#
#  CGRdb is free software; you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published by
#  the Free Software Foundation; either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with this program; if not, see <https://www.gnu.org/licenses/>.
#
from CachedMethods import cached_property
from CGRtools.containers import ReactionContainer, MoleculeContainer, QueryContainer
from collections import defaultdict
from compress_pickle import dumps
from datetime import datetime
from itertools import product
from LazyPony import LazyEntityMeta
from pony.orm import PrimaryKey, Required, Optional, Set, Json, select, IntArray, FloatArray, composite_key, raw_sql
from typing import Optional as tOptional


class Reaction(metaclass=LazyEntityMeta, database='CGRdb'):
    id = PrimaryKey(int, auto=True)
    _structure = Required(bytes, index=False, lazy=True, volatile=True, column='structure', nullable=True)
    _molecules = Set('MoleculeReaction')
    _reaction_indexes = Set('ReactionIndex')

    def __init__(self, structure):
        """
        storing reaction in DB.
        :param structure: CGRtools ReactionContainer
        """
        super().__init__(_structure=dumps(structure, compression='lzma'))

    def __str__(self):
        """
        canonical signature of reaction
        """
        return str(self.structure)

    def __bytes__(self):
        """
        hashed CGR signature of reaction
        """
        return bytes(self.cgr)

    @cached_property
    def structure(self):
        """
        canonical structure of reaction
        """
        # mapping and molecules preload
        mrs = self._molecules.order_by(lambda x: x.id).prefetch(self._database_.Molecule)[:]
        # canonic molecule structures preload
        ms = {x.molecule for x in mrs}
        for x in self._database_.MoleculeStructure.select(lambda x: x.is_canonic and x.molecule in ms):
            x.molecule.__dict__['structure_entity'] = x

        r, p = [], []
        for m in mrs:
            s = m.molecule.structure
            if m.mapping:
                s = s.remap(m.mapping, copy=True)
            if m.is_product:
                p.append(s)
            else:
                r.append(s)
        return ReactionContainer(r, p)

    @cached_property
    def structures(self):
        # mapping and molecules preload
        mrs = self._molecules.order_by(lambda x: x.id).prefetch(self._database_.Molecule)[:]

        # structures preload
        ms = {x.molecule: [] for x in mrs}
        for x in self._database_.MoleculeStructure.select(lambda x: x.molecule in ms.keys()):
            if x.is_canonic:
                x.molecule.__dict__['structure_entity'] = x
            ms[x.molecule].append(x)
        for m, s in ms.items():
            m.__dict__['structures_entities'] = tuple(s)

        # all possible reaction structure combinations
        combinations = tuple(product(*(x.molecule.structures for x in mrs)))

        structures = []
        for x in combinations:
            r, p = [], []
            for s, m in zip(x, mrs):
                if m.mapping:
                    s = s.remap(m.mapping, copy=True)
                if m.is_product:
                    p.append(s)
                else:
                    r.append(s)
            structures.append(ReactionContainer(r, p))

        if 'structure' not in self.__dict__:
            if len(structures) == 1:  # optimize
                self.__dict__['structure'] = structures[0]
            else:
                r, p = [], []
                for m in mrs:
                    s = m.molecule.structure
                    if m.mapping:
                        s = s.remap(m.mapping, copy=True)
                    if m.is_product:
                        p.append(s)
                    else:
                        r.append(s)
                self.__dict__['structure'] = ReactionContainer(r, p)
        return tuple(structures)

    @cached_property
    def cgr(self):
        """
        CRG of reaction canonical structure
        """
        return ~self.structure

    @cached_property
    def cgrs(self):
        """
        CGRs of all possible structures of reaction
        """
        return tuple(~x for x in self.structures)

    @classmethod
    def structure_exists(cls, structure):
        if not isinstance(structure, ReactionContainer):
            raise TypeError('Reaction expected')
        elif not structure.reactants or not structure.products:
            raise ValueError('empty query')

        structure = dumps(structure, compression='lzma').hex()
        schema = cls._table_[0]  # define DB schema
        fnd = cls._database_.select(
                f'''SELECT * FROM "{schema}".cgrdb_search_structure_reaction('\\x{structure}'::bytea)''')[0]
        return bool(fnd)

    @classmethod
    def find_structure(cls, structure):
        if not isinstance(structure, ReactionContainer):
            raise TypeError('Reaction expected')
        elif not structure.reactants or not structure.products:
            raise ValueError('empty query')

        structure = dumps(structure, compression='lzma').hex()
        schema = cls._table_[0]  # define DB schema
        fnd = cls._database_.select(
                f'''SELECT * FROM "{schema}".cgrdb_search_structure_reaction('\\x{structure}'::bytea)''')[0]
        if fnd:
            reaction = cls[fnd]
            reaction.structure  # prefetch structure
            return reaction

    @classmethod
    def find_substructures(cls, structure):
        """
        substructure search

        substructure search is 2-step process. first step is screening procedure. next step is isomorphism testing.

        :param structure: CGRtools ReactionContainer
        :return: ReactionSearchCache object with all found reactions or None
        """
        if not isinstance(structure, ReactionContainer):
            raise TypeError('Reaction expected')
        elif not structure.reactants or not structure.products:
            raise ValueError('empty query')

        structure = dumps(structure, compression='lzma').hex()
        schema = cls._table_[0]  # define DB schema
        ci, fnd = cls._database_.select(
            f'''SELECT * FROM "{schema}".cgrdb_search_substructure_reactions('\\x{structure}'::bytea)''')[0]
        if fnd:
            c = cls._database_.ReactionSearchCache[ci]
            c.__dict__['_size'] = fnd
            return c

    @classmethod
    def find_similar(cls, structure, *, threshold=.7):
        """
        similarity search

        :param structure: CGRtools ReactionContainer
        :param threshold: Tanimoto similarity threshold
        :return: ReactionSearchCache object with all found reactions or None
        """
        if not isinstance(structure, ReactionContainer):
            raise TypeError('Reaction expected')
        elif not structure.reactants or not structure.products:
            raise ValueError('empty query')

        structure = dumps(structure, compression='lzma').hex()
        schema = cls._table_[0]  # define DB schema
        ci, fnd = cls._database_.select(
            f'''SELECT * FROM "{schema}".cgrdb_search_similar_reactions('\\x{structure}'::bytea, {threshold})''')[0]
        if fnd:
            c = cls._database_.ReactionSearchCache[ci]
            c.__dict__['_size'] = fnd
            return c

    @classmethod
    def find_mappingless_substructures(cls, structure):
        """
        search reactions by substructures of molecules

        :param structure: CGRtools ReactionContainer
        :return: ReactionSearchCache object with all found reactions or None
        """
        if not isinstance(structure, ReactionContainer):
            raise TypeError('Reaction expected')
        elif not structure.reactants and not structure.products:
            raise ValueError('empty query')

        structure = dumps(structure, compression='lzma').hex()
        schema = cls._table_[0]  # define DB schema
        ci, fnd = cls._database_.select(
            f'''SELECT * FROM "{schema}".cgrdb_search_mappingless_substructure_reactions('\\x{structure}'::bytea)''')[0]
        if fnd:
            c = cls._database_.ReactionSearchCache[ci]
            c.__dict__['_size'] = fnd
            return c

    @classmethod
    def find_substructure_reactions(cls, structure, is_product: tOptional[bool] = None):
        """
        search reactions including substructure molecules

        :param structure: CGRtools MoleculeContainer or QueryContainer
        :param is_product: role of molecule: Reactant = False, Product = True, Any = None
        :return:ReactionSearchCache object with all found reactions or None
        """
        if not isinstance(structure, (MoleculeContainer, QueryContainer)):
            raise TypeError('Molecule or Query expected')
        elif not len(structure):
            raise ValueError('empty query')

        if is_product is None:
            role = 0
        elif isinstance(is_product, bool):
            role = 2 if is_product else 1
        else:
            raise ValueError('invalid role')

        structure = dumps(structure, compression='lzma').hex()
        schema = cls._table_[0]  # define DB schema
        ci, fnd = cls._database_.select(f'''SELECT * FROM 
            "{schema}".cgrdb_search_reactions_by_molecule('\\x{structure}'::bytea, {role}, 1, 0)''')[0]
        if fnd:
            c = cls._database_.ReactionSearchCache[ci]
            c.__dict__['_size'] = fnd
            return c

    @classmethod
    def find_similar_reactions(cls, structure, is_product: tOptional[bool] = None, *, threshold=.7):
        """
        search reactions including similar molecules

        :param structure: CGRtools MoleculeContainer
        :param is_product: role of molecule: Reactant = False, Product = True, Any = None
        :param threshold: Tanimoto similarity threshold
        :return:ReactionSearchCache object with all found reactions or None
        """
        if not isinstance(structure, MoleculeContainer):
            raise TypeError('Molecule expected')
        elif not len(structure):
            raise ValueError('empty query')

        if is_product is None:
            role = 0
        elif isinstance(is_product, bool):
            role = 2 if is_product else 1
        else:
            raise ValueError('invalid role')

        structure = dumps(structure, compression='lzma').hex()
        schema = cls._table_[0]  # define DB schema
        ci, fnd = cls._database_.select(f'''SELECT * FROM
            "{schema}".cgrdb_search_reactions_by_molecule('\\x{structure}'::bytea, {role}, 2, {threshold})''')[0]
        if fnd:
            c = cls._database_.ReactionSearchCache[ci]
            c.__dict__['_size'] = fnd
            return c

    @classmethod
    def prefetch_structure(cls, reactions):
        """
        preload reaction canonical structures
        :param reactions: Reaction entities list
        """
        # preload all molecules and canonic structures
        for x in select(ms for ms in cls._database_.MoleculeStructure for mr in cls._database_.MoleculeReaction
                        if ms.molecule == mr.molecule and ms.is_canonic and
                           mr.reaction in reactions).prefetch(cls._database_.Molecule):
            x.molecule.__dict__['structure_entity'] = x

        # load mapping and fill reaction
        rxn = defaultdict(lambda: ([], []))
        for x in cls._database_.MoleculeReaction.select(lambda x: x.reaction in reactions).order_by(lambda x: x.id):
            s = x.molecule.structure
            if x.mapping:
                s = s.remap(x.mapping, copy=True)
            rxn[x.reaction][x.is_product].append(s)
        for r, rp in rxn.items():
            r.__dict__['structure'] = ReactionContainer(*rp)


class MoleculeReaction(metaclass=LazyEntityMeta, database='CGRdb'):
    """ molecule to reaction mapping data and role (reactant, product)
    """
    id = PrimaryKey(int, auto=True)
    reaction = Required('Reaction')
    molecule = Required('Molecule')
    is_product = Required(bool)
    _mapping = Optional(Json, column='mapping', nullable=True)

    @cached_property
    def mapping(self):
        return dict(self._mapping) if self._mapping else {}


class ReactionIndex(metaclass=LazyEntityMeta, database='CGRdb'):
    id = PrimaryKey(int, auto=True)
    reaction = Required('Reaction')
    _signature = Required(bytes, unique=True, volatile=True, lazy=True, column='signature')
    _fingerprint = Required(IntArray, optimistic=False, index=False, lazy=True, volatile=True, column='fingerprint')
    _structures = Required(IntArray, optimistic=False, index=False, lazy=True, volatile=True, column='structures')


class ReactionSearchCache(metaclass=LazyEntityMeta, database='CGRdb'):
    id = PrimaryKey(int, auto=True)
    signature = Required(bytes)
    operator = Required(str)
    date = Required(datetime, default=datetime.utcnow)
    _reactions = Required(IntArray, optimistic=False, index=False, column='reactions', lazy=True)
    _tanimotos = Required(FloatArray, optimistic=False, index=False, column='tanimotos', lazy=True, sql_type='real[]')
    composite_key(signature, operator)

    def reactions(self, page=1, pagesize=100):
        if page < 1:
            raise ValueError('page should be greater or equal than 1')
        elif pagesize < 1:
            raise ValueError('pagesize should be greater or equal than 1')

        start = (page - 1) * pagesize
        end = start + pagesize
        ris = select(x._reactions[start:end] for x in self.__class__ if x.id == self.id).first()
        if not ris:
            return []

        # preload molecules
        rs = {x.id: x for x in self._database_.Reaction.select(lambda x: x.id in ris)}
        rs = [rs[x] for x in ris]
        self._database_.Reaction.prefetch_structure(rs)
        return rs

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
        return select(raw_sql('array_length(x.reactions, 1)') for x in self.__class__ if x.id == self.id).first()


__all__ = ['Reaction', 'MoleculeReaction', 'ReactionIndex', 'ReactionSearchCache']
