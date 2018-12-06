# -*- coding: utf-8 -*-
#
#  Copyright 2018 Ramil Nugmanov <stsouko@live.ru>
#  Copyright 2018 Adelia Fatykhova <adelik21979@gmail.com>
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
from itertools import product
from operator import itemgetter
from pony.orm import select, raw_sql, left_join
from .molecule_cache import QueryCache


def mixin_factory(db, schema):
    class Search:
        @classmethod
        def unmapped_structure_exists(cls, structure):
            return db.ReactionIndex.exists(signature=structure if isinstance(structure, bytes) else
                                           cls.get_signature(structure))

        @classmethod
        def structure_exists(cls, structure):
            return db.ReactionIndex.exists(cgr_signature=structure if isinstance(structure, bytes) else
                                           cls.get_cgr_signature(structure))

        @classmethod
        def find_unmapped_structures(cls, structure):
            signature = structure if isinstance(structure, bytes) else cls.get_signature(structure)
            return list(select(x.reaction for x in db.ReactionIndex if x.signature == signature))

        @classmethod
        def find_structure(cls, structure):
            ri = db.ReactionIndex.get(cgr_signature=structure if isinstance(structure, bytes) else
                                      cls.get_cgr_signature(structure))
            return ri and ri.reaction

        @classmethod
        def find_substructures(cls, structure, number=10):
            """
            cgr substructure search
            :param structure: CGRtools ReactionContainer
            :param number: top limit of returned results. not guarantee returning of all available data.
            set bigger value for this. negative value returns generator for all data in db.
            :return: list of tuples of Reaction entities and Tanimoto indexes
            """
            cgr = cls.get_cgr(structure)
            q = ((x, y) for x, y in cls._get_reactions(cgr, 'substructure', number, set_raw=True)
                 if any(cls.is_substructure(rs, cgr) for rs in x.cgrs_raw))

            return q

        @classmethod
        def find_similar(cls, structure, number=10):
            """
            cgr similar search
            :param structure: CGRtools ReactionContainer
            :param number: top limit of returned results. not guarantee returning of all available data.
            negative value returns generator for all data in db.
            :return: list of tuples of Reaction entities and Tanimoto indexes
            """
            q = cls._get_reactions(structure, 'similar', number)
            return q

        @classmethod
        def _get_reactions(cls, structure, operator, number, set_raw=False, page=1):
            """
            extract Reaction entities from ReactionIndex entities.
            cache reaction structure in Reaction entities
            :param structure: query structure
            :param operator: raw sql operator (similar or substructure)
            :param number: number of results. if negative - return all data
            :param page: starting page in pagination
            :return: Reaction entities
            """
            reaction_cache = cls.__substructure_cache if operator == 'substructure' else cls.__similarity_cache
            start = (page - 1) * number
            end = (page - 1) * number + number
            se = slice(start, end)
            sig = cls.get_signature(structure)
            if sig in reaction_cache:
                ris, iis, its = reaction_cache[sig]
                if number >= 0:
                    ris = ris[se]
                    iis = iis[se]
                    its = its[se]
            else:
                if not db.ReactionSearchCache.exists(signature=sig, operator=operator):
                    bit_set = cls.get_fingerprint(structure, bit_array=False)
                    q = db.select(f"SELECT * FROM {schema}.get_reactions('{bit_set}', '{operator}', $sig)")[0]
                    if not db.ReactionSearchCache.exists(signature=sig, operator=operator):
                        ris, iis, its = reaction_cache[sig] = q
                        if number >= 0:
                            ris = ris[se]
                            iis = iis[se]
                            its = its[se]
                    else:
                        if number >= 0:
                            ris, iis, its = select(
                                (x.reactions[start:end], x.reaction_indexes[start:end], x.tanimotos[start:end]) for x in
                                db.ReactionSearchCache if
                                x.signature == sig and x.operator == operator).first()
                        else:
                            mis, sis, sts = select((x.reactions, x.reaction_indexes, x.tanimotos) for x in
                                                   db.ReactionSearchCache if
                                                   x.signature == sig and x.operator == operator).first()
                else:
                    if number >= 0:
                        ris, iis, its = select(
                            (x.reactions[start:end], x.reaction_indexes[start:end], x.tanimotos[start:end]) for x in
                            db.ReactionSearchCache if
                            x.signature == sig and x.operator == operator).first()
                    else:
                        mis, sis, sts = select((x.reactions, x.reaction_indexes, x.tanimotos) for x in
                                               db.ReactionSearchCache if
                                               x.signature == sig and x.operator == operator).first()

            reactions = list(cls.select(lambda x: x.id in ris))
            if set_raw:
                rsr, sis = defaultdict(list), set()
                for si, ri in left_join((x.structures.id, x.reaction.id) for x in db.ReactionIndex if x.id in iis):
                    sis.add(si)
                    rsr[ri].append(si)

                mss, mis = {}, defaultdict(list)
                for structure in db.MoleculeStructure.select(lambda x: x.id in sis):
                    mis[structure.molecule.id].append(structure)
                    if structure.last:
                        mss[structure.molecule.id] = structure

                not_last = set(mis).difference(mss)
                if not_last:
                    for structure in db.MoleculeStructure.select(lambda x: x.molecule.id in not_last and x.last):
                        mss[structure.molecule.id] = structure

                mrs = cls._get_molecule_reaction_entities(reactions)
                cls._load_structures(reactions, mss, mrs)
                cls._load_structures_raw(reactions, mis, rsr, mrs)
            else:
                cls._load_structures(reactions)
            yield from zip(reactions, its)

        @staticmethod
        def _get_molecule_reaction_entities(reactions):
            return list(db.MoleculeReaction.select(lambda x: x.reaction in reactions).order_by(lambda x: x.id))

        @staticmethod
        def _get_last_molecule_structure_entities(reactions):
            return list(select(ms for ms in db.MoleculeStructure for mr in db.MoleculeReaction
                               if ms.molecule == mr.molecule and mr.reaction in reactions and ms.last))

        @classmethod
        def _load_structures_raw(cls, reactions, mis=None, rsr=None, mrs=None):
            """
            preload reaction structures
            :param reactions: reactions entities
            :param mis: dict of molecule id with list of raw MoleculeStructure found in reaction by query
            :param rsr: dict of reaction id with list of raw MoleculeStructure ids
            :param mrs: list of MoleculeReaction entities
            """
            if not mrs:
                mrs = cls._get_molecule_reaction_entities(reactions)

            combos, mapping = defaultdict(list), defaultdict(list)
            for mr in mrs:
                ri = mr.reaction.id
                combos[ri].append([x for x in mis[mr.molecule.id] if x.id in rsr[ri]])
                mapping[ri].append((mr.is_product, mr.mapping))

            for r in reactions:
                r.structures_raw = rrcs = []
                for combo in product(*combos[r.id]):
                    rc = ReactionContainer()
                    rrcs.append(rc)
                    for ms, (is_p, ms_map) in zip(combo, mapping[r.id]):
                        rc['products' if is_p else 'reagents'].append(ms.structure.remap(ms_map, copy=True)
                                                                      if ms_map else ms.structure)

        @classmethod
        def _load_structures(cls, reactions, mss=None, mrs=None):
            """
            preload reaction structures
            :param reactions: Reaction entities
            :param mss: dict of molecule id: last MoleculeStructure
            :param mrs: list of MoleculeReaction entities
            """
            rs = {x.id: x for x in reactions}
            if not mrs:
                mrs = cls._get_molecule_reaction_entities(reactions)

            if not mss:
                mss = {x.molecule.id: x for x in cls._get_last_molecule_structure_entities(reactions)}

            for r in reactions:
                r.structure = ReactionContainer()

            for mr in mrs:
                ms = mss[mr.molecule.id]
                rs[mr.reaction.id].structure['products' if mr.is_product else 'reagents'].append(
                    ms.structure.remap(mr.mapping, copy=True) if mr.mapping else ms.structure)

        __similarity_cache = QueryCache()
        __substructure_cache = QueryCache()

    return Search


__all__ = [mixin_factory.__name__]
