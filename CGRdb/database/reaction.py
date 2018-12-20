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
from CGRtools.containers import ReactionContainer
from collections import defaultdict
from datetime import datetime
from itertools import product
from LazyPony import LazyEntityMeta, DoubleLink
from pony.orm import PrimaryKey, Required, Optional, Set, Json, select, IntArray, FloatArray
from ..search import FingerprintReaction


class Reaction(metaclass=LazyEntityMeta, database='CGRdb'):
    id = PrimaryKey(int, auto=True)
    date = Required(datetime, default=datetime.utcnow)
    user = DoubleLink(Required('User', reverse='reactions'), Set('Reaction'))
    molecules = Set('MoleculeReaction')
    reaction_indexes = Set('ReactionIndex')
    special = Optional(Json)

    def __init__(self, structure, user, special=None):
        """
        storing reaction in DB.
        :param structure: CGRtools ReactionContainer
        :param user: user entity
        :param special: Json serializable Data (expected dict)
        """
        super().__init__(user=user)

        signatures = {bytes(m) for m in structure.reagents} | {bytes(m) for m in structure.products}
        m2ms_mapping, signature2ms_mapping = self.__preload_molecules(signatures)

        combinations, duplicates = [], {}
        for sl, is_p in ((structure.reagents, False), (structure.products, True)):
            for s in sl:
                sig = bytes(s)
                ms = signature2ms_mapping.get(sig)
                if ms:
                    mapping = ms.structure.get_mapping(s)
                    self._database_.MoleculeReaction(reaction=self, molecule=ms.molecule, is_product=is_p,
                                                     mapping=mapping)

                    tmp = [(s, ms)]
                    if ms.molecule in m2ms_mapping:
                        tmp.extend((x.structure.remap(mapping, copy=True), x) for x in m2ms_mapping[ms.molecule])
                    combinations.append(tmp)
                else:
                    if sig not in duplicates:
                        m = duplicates[sig] = self._database_.Molecule(s, user)
                        mapping = None
                    else:
                        m = duplicates[sig]
                        mapping = m.structure.get_mapping(s)

                    self._database_.MoleculeReaction(reaction=self, molecule=m, is_product=is_p, mapping=mapping)
                    combinations.append([(s, m.last_edition)])

        reagents_len = len(structure.reagents)
        combinations = list(product(*combinations))
        if len(combinations) == 1:  # optimize
            structures = [structure]
        else:
            structures = [ReactionContainer(reagents=[x for x, _ in x[:reagents_len]],
                                            products=[x for x, _ in x[reagents_len:]]) for x in combinations]

        for s, mss in zip(structures, ((x for _, x in c) for c in combinations)):
            self._database_.ReactionIndex(self, mss, s)

        if special:
            self.special = special

    @classmethod
    def __preload_molecules(cls, signatures):
        # preload molecules entities. pony caching it.
        select(x.molecule for x in cls._database_.MoleculeStructure if x.signature in signatures)[:]
        # preload all molecules structures entities
        m2ms_mapping, signature2ms_mapping = defaultdict(list), {}
        for ms in select(x for x in cls._database_.MoleculeStructure if x.molecule in
                         select(y.molecule for y in cls._database_.MoleculeStructure if y.signature in signatures)):
            # NEED PR
            # select(y for x in db.MoleculeStructure if x.signature in signatures_set
            #        for y in db.MoleculeStructure if y.molecule == x.molecule)
            if ms.signature in signatures:
                signature2ms_mapping[ms.signature] = ms
            else:
                m2ms_mapping[ms.molecule].append(ms)
        return dict(m2ms_mapping), signature2ms_mapping

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

    @property
    def structure(self):
        """
        ReactionContainer object
        """
        if self.__cached_structure is None:
            mrs = list(self.molecules.order_by(lambda x: x.id))
            mss = {x.molecule.id: x for x in
                   select(ms for ms in self._database_.MoleculeStructure for mr in self._database_.MoleculeReaction
                          if ms.molecule == mr.molecule and mr.reaction == self and ms.last)}

            r = ReactionContainer()
            for mr in mrs:
                ms = mss[mr.molecule.id]
                r['products' if mr.is_product else 'reagents'].append(
                    ms.structure.remap(mr.mapping, copy=True) if mr.mapping else ms.structure)
            self.__cached_structure = r
        return self.__cached_structure

    @property
    def cgr(self):
        """
        CRG of reaction
        """
        if self.__cached_cgr is None:
            self.__cached_cgr = ~self.structure
        return self.__cached_cgr

    @property
    def structures_raw(self):
        if self.__cached_structures_raw is not None:
            return self.__cached_structures_raw
        raise AttributeError('available in entities from queries results only')

    @property
    def cgrs_raw(self):
        if self.__cached_cgrs_raw is None:
            self.__cached_cgrs_raw = [~x for x in self.structures_raw]
        return self.__cached_cgrs_raw

    @structure.setter
    def structure(self, structure):
        self.__cached_structure = structure

    @structures_raw.setter
    def structures_raw(self, structures):
        self.__cached_structures_raw = structures

    __cached_structure = __cached_cgr = __cached_structures_raw = __cached_cgrs_raw = None


class MoleculeReaction(metaclass=LazyEntityMeta, database='CGRdb'):
    """ molecule to reaction mapping data and role (reagent, reactant, product)
    """
    id = PrimaryKey(int, auto=True)
    reaction = Required('Reaction')
    molecule = Required('Molecule')
    is_product = Required(bool, default=False)
    _mapping = Optional(Json, column='mapping', nullable=True)

    def __init__(self, *, mapping=None, **kwargs):
        super().__init__(_mapping=self._compressed_mapping(mapping), **kwargs)

    @property
    def mapping(self):
        if self.__cached_mapping is None:
            self.__cached_mapping = dict(self._mapping) if self._mapping else {}
        return self.__cached_mapping

    @mapping.setter
    def mapping(self, mapping):
        self._mapping = self._compressed_mapping(mapping)
        self.__cached_mapping = None

    @staticmethod
    def _compressed_mapping(mapping):
        return mapping and [(k, v) for k, v in mapping.items() if k != v] or None

    __cached_mapping = None


class ReactionIndex(FingerprintReaction, metaclass=LazyEntityMeta, database='CGRdb'):
    id = PrimaryKey(int, auto=True)
    reaction = Required('Reaction')
    structures = Set('MoleculeStructure', table='ReactionIndex_MoleculeStructure')
    signature = Required(bytes, unique=True)
    bit_array = Required(IntArray, optimistic=False, index=False, lazy=True)

    def __init__(self, reaction, structures, reaction_container):
        cgr = ~reaction_container
        super().__init__(reaction=reaction, signature=bytes(cgr),
                         bit_array=self.get_fingerprint(cgr))
        for m in set(structures):
            self.structures.add(m)

    @classmethod
    def get_signature(cls, structure):
        """
        get CGR signature of reaction. SUGAR
        """
        return bytes(~structure)


class ReactionSearchCache(metaclass=LazyEntityMeta, database='CGRdb'):
    id = PrimaryKey(int, auto=True)
    signature = Required(bytes)
    reactions = Required(IntArray)
    reaction_indexes = Required(IntArray)
    tanimotos = Required(FloatArray)
    date = Required(datetime)
    operator = Required(str)


__all__ = ['Reaction', 'MoleculeReaction', 'ReactionIndex', 'ReactionSearchCache']
