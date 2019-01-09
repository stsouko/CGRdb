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
from ..search import FingerprintReaction, SearchReaction


class Reaction(SearchReaction, metaclass=LazyEntityMeta, database='CGRdb'):
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

        # preload all molecules and structures
        signatures = {bytes(m) for m in structure.reagents} | {bytes(m) for m in structure.products}
        ms, s2ms = defaultdict(list), {}
        for x in select(x for x in self._database_.MoleculeStructure
                        if x.molecule in select(y.molecule for y in self._database_.MoleculeStructure
                                                if y.signature in signatures)).prefetch(self._database_.Molecule):
            # NEED PR
            # select(y for x in db.MoleculeStructure if x.signature in signatures_set
            #        for y in db.MoleculeStructure if y.molecule == x.molecule)
            if x.signature in signatures:
                s2ms[x.signature] = x
            if x.last:
                x.molecule._cached_structure = x
            ms[x.molecule].append(x)
        for m, s in ms.items():
            m._cached_structures_all = tuple(s)

        combinations, duplicates = [], {}
        for sl, is_p in ((structure.reagents, False), (structure.products, True)):
            for s in sl:
                sig = bytes(s)
                ms = s2ms.get(sig)
                if ms:
                    mapping = ms.structure.get_mapping(s)
                    self._database_.MoleculeReaction(reaction=self, molecule=ms.molecule, is_product=is_p,
                                                     mapping=mapping)
                    # first MoleculeStructure always last
                    if ms.last:  # last structure equal to reaction structure
                        c = [s]
                        c.extend(x.structure.remap(mapping, copy=True) for x in ms.molecule.all_editions if not x.last)
                    else:  # last structure remapping
                        c = [ms.molecule.structure.remap(mapping, copy=True)]
                        c.extend(x.structure.remap(mapping, copy=True) if x != ms else s
                                 for x in ms.molecule.all_editions if not x.last)
                    combinations.append(c)
                else:  # New molecule
                    if sig not in duplicates:
                        m = duplicates[sig] = self._database_.Molecule(s, user)
                        mapping = None
                    else:
                        m = duplicates[sig]
                        mapping = m.structure.get_mapping(s)

                    self._database_.MoleculeReaction(reaction=self, molecule=m, is_product=is_p, mapping=mapping)
                    combinations.append([s])

        reagents_len = len(structure.reagents)
        combinations = tuple(product(*combinations))
        if len(combinations) == 1:  # optimize
            self._cached_structures_all = (structure,)
            self._cached_structure = structure
            self._database_.ReactionIndex(self, structure, True)
        else:
            x = combinations[0]
            self._cached_structure = ReactionContainer(reagents=x[:reagents_len], products=x[reagents_len:])
            self._database_.ReactionIndex(self, self._cached_structure, True)

            cgr = {}
            for x in combinations[1:]:
                x = ReactionContainer(reagents=x[:reagents_len], products=x[reagents_len:])
                cgr[~x] = x

            self._cached_structures_all = (self._cached_structure, *cgr.values())
            for x in cgr:
                self._database_.ReactionIndex(self, x, False)

        if special:
            self.special = special

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
        if self._cached_structure is None:
            # mapping and molecules preload
            mrs = self.molecules.order_by(lambda x: x.id).prefetch(self._database_.Molecule)[:]
            # last molecule structures preload
            ms = {x.molecule for x in mrs}
            for x in self._database_.MoleculeStructure.select(lambda x: x.last and x.molecule in ms):
                x.molecule._cached_structure = x

            self._cached_structure = r = ReactionContainer()
            for m in mrs:
                s = m.molecule.structure
                r['products' if m.is_product else 'reagents'].append(s.remap(m.mapping, copy=True) if m.mapping else s)
        return self._cached_structure

    @property
    def structures_all(self):
        if self._cached_structures_all is None:
            # mapping and molecules preload
            mrs = self.molecules.order_by(lambda x: x.id).prefetch(self._database_.Molecule)[:]

            # structures preload
            ms = {x.molecule: [] for x in mrs}
            for x in self._database_.MoleculeStructure.select(lambda x: x.molecule in ms.keys()):
                if x.last:
                    x.molecule._cached_structure = x
                ms[x.molecule].append(x)
            for m, s in ms.items():
                m._cached_structures_all = tuple(s)

            # all possible reaction structure combinations
            combinations = tuple(product(*(x.molecule.structures_all for x in mrs)))

            structures = []
            for x in combinations:
                r = ReactionContainer()
                structures.append(r)
                for s, m in zip(x, mrs):
                    r['products' if m.is_product else 'reagents'].append(
                        s.remap(m.mapping, copy=True) if m.mapping else s)
            self._cached_structures_all = tuple(structures)

            if self._cached_structure is None:
                if len(structures) == 1:  # optimize
                    self._cached_structure = structures[0]
                else:
                    self._cached_structure = r = ReactionContainer()
                    for m in mrs:
                        s = m.molecule.structure
                        r['products' if m.is_product else 'reagents'].append(
                            s.remap(m.mapping, copy=True) if m.mapping else s)
        return self._cached_structures_all

    @property
    def structure_raw(self):
        if self._cached_structure_raw is not None:
            return self._cached_structure_raw
        raise AttributeError('available in entities from queries results only')

    @property
    def cgr(self):
        """
        CRG of reaction
        """
        if self.__cached_cgr is None:
            self.__cached_cgr = ~self.structure
        return self.__cached_cgr

    @property
    def cgrs_all(self):
        if self.__cached_cgrs_all is None:
            self.__cached_cgrs_all = tuple(~x for x in self.structures_all)
        return self.__cached_cgrs_all

    @property
    def cgr_raw(self):
        if self.__cached_cgr_raw is None:
            self.__cached_cgr_raw = ~self.structure_raw
        return self.__cached_cgr_raw

    _cached_structure = _cached_structures_all = _cached_structure_raw = None
    __cached_cgr = __cached_cgrs_all = __cached_cgr_raw = None


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
    signature = Required(bytes, unique=True)
    last = Required(bool)
    bit_array = Required(IntArray, optimistic=False, index=False, lazy=True)

    def __init__(self, reaction, structure, last):
        if isinstance(structure, ReactionContainer):
            structure = ~structure
        super().__init__(reaction=reaction, signature=bytes(structure), last=last,
                         bit_array=self.get_fingerprint(structure))


class ReactionSearchCache(metaclass=LazyEntityMeta, database='CGRdb'):
    id = PrimaryKey(int, auto=True)
    signature = Required(bytes)
    reactions = Required(IntArray, optimistic=False, index=False)
    tanimotos = Required(FloatArray, optimistic=False, index=False)
    date = Required(datetime)
    operator = Required(str)


__all__ = ['Reaction', 'MoleculeReaction', 'ReactionIndex', 'ReactionSearchCache']
