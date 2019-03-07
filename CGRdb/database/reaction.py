# -*- coding: utf-8 -*-
#
#  Copyright 2017-2019 Ramil Nugmanov <stsouko@live.ru>
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
from CGRtools.containers import ReactionContainer
from collections import defaultdict
from datetime import datetime
from itertools import product
from LazyPony import LazyEntityMeta, DoubleLink
from pony.orm import PrimaryKey, Required, Optional, Set, Json, select, IntArray, FloatArray, composite_key
from ..search import FingerprintReaction, SearchReaction


class Reaction(SearchReaction, metaclass=LazyEntityMeta, database='CGRdb'):
    id = PrimaryKey(int, auto=True)
    date = Required(datetime, default=datetime.utcnow)
    user = DoubleLink(Required('User', reverse='reactions'), Set('Reaction'))
    molecules = Set('MoleculeReaction')
    reaction_indexes = Set('ReactionIndex')

    def __init__(self, structure, user):
        """
        storing reaction in DB.
        :param structure: CGRtools ReactionContainer
        :param user: user entity
        """
        super().__init__(user=user)

        # preload all molecules and structures
        signatures = {bytes(m) for m in structure.reactants} | {bytes(m) for m in structure.products}
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
                x.molecule.__dict__['structure_entity'] = x
            ms[x.molecule].append(x)
        for m, s in ms.items():
            m.__dict__['structures_entities'] = tuple(s)

        combinations, duplicates = [], {}
        for sl, is_p in ((structure.reactants, False), (structure.products, True)):
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
                        c.extend(x.structure.remap(mapping, copy=True)
                                 for x in ms.molecule.structures_entities if not x.last)
                    else:  # last structure remapping
                        c = [ms.molecule.structure.remap(mapping, copy=True)]
                        c.extend(x.structure.remap(mapping, copy=True) if x != ms else s
                                 for x in ms.molecule.structures_entities if not x.last)
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

        reactants_len = len(structure.reactants)
        combinations = tuple(product(*combinations))
        if len(combinations) == 1:  # optimize
            self.__dict__['structures'] = (structure,)
            self.__dict__['structure'] = structure
            self._database_.ReactionIndex(self, structure, True)
        else:
            x = combinations[0]
            self.__dict__['structure'] = r = ReactionContainer(x[:reactants_len], x[reactants_len:])
            self._database_.ReactionIndex(self, r, True)

            cgr = {}
            for x in combinations[1:]:
                x = ReactionContainer(x[:reactants_len], x[reactants_len:])
                cgr[~x] = x

            self.__dict__['structures'] = (r, *cgr.values())
            for x in cgr:
                self._database_.ReactionIndex(self, x, False)

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
        mrs = self.molecules.order_by(lambda x: x.id).prefetch(self._database_.Molecule)[:]
        # last molecule structures preload
        ms = {x.molecule for x in mrs}
        for x in self._database_.MoleculeStructure.select(lambda x: x.last and x.molecule in ms):
            x.molecule.__dict__['structure_entity'] = x

        r = ReactionContainer()
        for m in mrs:
            s = m.molecule.structure
            r[m.is_product].append(s.remap(m.mapping, copy=True) if m.mapping else s)
        return r

    @cached_property
    def structures(self):
        # mapping and molecules preload
        mrs = self.molecules.order_by(lambda x: x.id).prefetch(self._database_.Molecule)[:]

        # structures preload
        ms = {x.molecule: [] for x in mrs}
        for x in self._database_.MoleculeStructure.select(lambda x: x.molecule in ms.keys()):
            if x.last:
                x.molecule.__dict__['structure_entity'] = x
            ms[x.molecule].append(x)
        for m, s in ms.items():
            m.__dict__['structures_entities'] = tuple(s)

        # all possible reaction structure combinations
        combinations = tuple(product(*(x.molecule.structures for x in mrs)))

        structures = []
        for x in combinations:
            r = ReactionContainer()
            structures.append(r)
            for s, m in zip(x, mrs):
                r[m.is_product].append(s.remap(m.mapping, copy=True) if m.mapping else s)

        if 'structure' not in self.__dict__:
            if len(structures) == 1:  # optimize
                self.__dict__['structure'] = structures[0]
            else:
                self.__dict__['structure'] = r = ReactionContainer()
                for m in mrs:
                    s = m.molecule.structure
                    r[m.is_product].append(s.remap(m.mapping, copy=True) if m.mapping else s)
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
    def prefetch_structure(cls, reactions):
        """
        preload reaction canonical structures
        :param reactions: Reaction entities list
        """
        # preload all molecules and last structures
        for x in select(ms for ms in cls._database_.MoleculeStructure for mr in cls._database_.MoleculeReaction
                        if ms.molecule == mr.molecule and ms.last and
                           mr.reaction in reactions).prefetch(cls._database_.Molecule):
            x.molecule.__dict__['structure_entity'] = x

        for x in reactions:
            x.__dict__['structure'] = ReactionContainer()

        # load mapping and fill reaction
        for x in cls._database_.MoleculeReaction.select(lambda x: x.reaction in reactions).order_by(lambda x: x.id):
            s = x.molecule.structure
            x.reaction.structure[x.is_product].append(s.remap(x.mapping, copy=True) if x.mapping else s)

    @classmethod
    def prefetch_structures(cls, reactions):
        """
        preload all combinations of reaction structures
        :param reactions: reactions entities list
        """
        # preload all molecules and structures
        ms = defaultdict(list)
        for x in select(ms for ms in cls._database_.MoleculeStructure for mr in cls._database_.MoleculeReaction
                        if ms.molecule == mr.molecule and mr.reaction in reactions).prefetch(cls._database_.Molecule):
            if x.last:
                x.molecule.__dict__['structure_entity'] = x
            ms[x.molecule].append(x)

        for m, s in ms.items():
            m.__dict__['structures_entities'] = tuple(s)

        combos, mapping, last = defaultdict(list), defaultdict(list), defaultdict(list)
        for x in cls._database_.MoleculeReaction.select(lambda x: x.reaction in reactions).order_by(lambda x: x.id):
            r = x.reaction
            combos[r].append(x.molecule.structures)
            mapping[r].append((x.is_product, x.mapping))
            last[r].append(x.molecule.structure)

        for x in reactions:
            # load last structure
            x.__dict__['structure'] = r = ReactionContainer()
            for s, (is_p, m) in zip(last[x], mapping[x]):
                r[is_p].append(s.remap(m, copy=True) if m else s)

            # load all structures
            combos_x = list(product(*combos[x]))
            if len(combos_x) == 1:
                x.__dict__['structures'] = (r,)
            else:
                rs = []
                for combo in combos_x:
                    r = ReactionContainer()
                    rs.append(r)
                    for s, (is_p, m) in zip(combo, mapping[x]):
                        r[is_p].append(s.remap(m, copy=True) if m else s)
                x.__dict__['structures'] = tuple(rs)


class MoleculeReaction(metaclass=LazyEntityMeta, database='CGRdb'):
    """ molecule to reaction mapping data and role (reactant, product)
    """
    id = PrimaryKey(int, auto=True)
    reaction = Required('Reaction')
    molecule = Required('Molecule')
    is_product = Required(bool, default=False)
    _mapping = Optional(Json, column='mapping', nullable=True)

    def __init__(self, *, mapping=None, **kwargs):
        super().__init__(_mapping=self._compressed_mapping(mapping), **kwargs)

    @cached_property
    def mapping(self):
        return dict(self._mapping) if self._mapping else {}

    @staticmethod
    def _compressed_mapping(mapping):
        return mapping and [(k, v) for k, v in mapping.items() if k != v] or None


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
    operator = Required(str)
    date = Required(datetime, default=datetime.utcnow)
    reactions = Required(IntArray, optimistic=False, index=False)
    tanimotos = Required(FloatArray, optimistic=False, index=False)
    composite_key(signature, operator)


__all__ = ['Reaction', 'MoleculeReaction', 'ReactionIndex', 'ReactionSearchCache']
