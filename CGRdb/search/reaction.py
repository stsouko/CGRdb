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
from pony.orm import select
from ..utils import QueryCache


class SearchReaction:
    @classmethod
    def structure_exists(cls, structure):
        return cls._database_.ReactionIndex.exists(signature=bytes(~structure))

    @classmethod
    def find_structure(cls, structure):
        ri = cls._database_.ReactionIndex.get(signature=bytes(~structure))
        return ri and ri.reaction

    @classmethod
    def find_substructures(cls, structure, number=10):
        """
        cgr substructure search
        :param structure: CGRtools ReactionContainer
        :param number: top limit of returned results.
        zero or negative value returns generator for all data in db.
        :return: list of tuples of Reaction entities and Tanimoto indexes
        """
        q = cls.__substructure_filter(structure, number)
        if number > 0:
            return list(q)
        return q

    @classmethod
    def __substructure_filter(cls, structure, number):
        cgr = ~structure
        for x, y in cls._get_reactions(cgr, 'substructure', number, set_raw=True):
            for s, c in zip(x.structures_all, x.cgrs_all):
                if cgr < c:
                    x._cached_structure_raw = s
                    yield x, y
                    break

    @classmethod
    def find_similar(cls, structure, number=10):
        """
        cgr similar search
        :param structure: CGRtools ReactionContainer
        :param number: top limit of returned results.
        zero or negative value returns generator for all data in db.
        :return: list of tuples of Reaction entities and Tanimoto indexes
        """
        q = cls._get_reactions(structure, 'similar', number)
        if number > 0:
            return list(q)
        return q

    @classmethod
    def _get_reactions(cls, cgr, operator, number, page=1, set_raw=False):
        """
        extract Reaction entities from ReactionIndex entities.
        cache reaction structure in Reaction entities
        :param cgr: query structure CGR
        :param operator: raw sql operator (similar or substructure)
        :param number: number of results. if zero or negative - return all data
        :param page: starting page in pagination
        :return: Reaction entities
        """
        cache = cls.__substructure_cache if operator == 'substructure' else cls.__similarity_cache
        operator = '@>' if operator == 'substructure' else '%'
        start = (page - 1) * number
        end = start + number
        sig = bytes(cgr)
        key = (sig, operator)
        if key in cache:
            ris, its = cache[key]
            if number > 0:
                ris = ris[start:end]
                its = its[start:end]
        elif not cls._database_.ReactionSearchCache.exists(signature=sig, operator=operator):
            schema = cls._table_  # define DB schema
            if schema and isinstance(schema, tuple):
                schema = schema[0]

            fingerprint = cls._database_.ReactionIndex.get_fingerprint(cgr) or '{}'
            cls._database_.execute(
                f"""CREATE TEMPORARY TABLE temp_hits ON COMMIT DROP AS
                    SELECT hit.reaction, hit.tanimoto
                    FROM (
                        SELECT DISTINCT ON (max_found.reaction) reaction, max_found.tanimoto
                        FROM (
                            SELECT found.reaction, found.tanimoto,
                                   max(found.tanimoto) OVER (PARTITION BY found.reaction) max_tanimoto
                            FROM (
                                SELECT rs.reaction as reaction,
                                       smlar(rs.bit_array, '{fingerprint}', 'N.i / (N.a + N.b - N.i)') AS tanimoto
                                FROM "{schema}"."ReactionIndex" rs
                                WHERE rs.bit_array {operator} '{fingerprint}'
                            ) found
                        ) max_found
                        WHERE max_found.tanimoto = max_found.max_tanimoto
                    ) hit
                    ORDER BY hit.tanimoto DESC
                """)
            found = cls._database_.select('SELECT COUNT(*) FROM temp_hits')[0]
            if found > 1000:
                cls._database_.execute(
                    f"""INSERT INTO 
                        "{schema}"."ReactionSearchCache"(signature, operator, date, reactions, indexes, tanimotos)
                        VALUES (
                            $sig, {operator}, CURRENT_TIMESTAMP,
                            (SELECT array_agg(reaction) FROM temp_hits),
                            (SELECT array_agg(tanimoto) FROM temp_hits)
                        )
                    """)
                if number > 0:
                    ris, its = select((x.reactions[start:end], x.tanimotos[start:end])
                                      for x in cls._database_.ReactionSearchCache
                                      if x.signature == sig and x.operator == operator).first()
                else:
                    ris, its = select((x.reactions, x.tanimotos)
                                      for x in cls._database_.ReactionSearchCache
                                      if x.signature == sig and x.operator == operator).first()
            elif found:
                ris, its = cache[key] = cls._database_.select(
                    "SELECT array_agg(reaction), array_agg(tanimoto) FROM temp_hits"
                )[0]
                if number > 0:
                    ris = ris[start:end]
                    its = its[start:end]
            else:
                raise StopIteration('not found')
        elif number > 0:
            ris, its = select((x.reactions[start:end], x.tanimotos[start:end])
                              for x in cls._database_.ReactionSearchCache
                              if x.signature == sig and x.operator == operator).first()
        else:
            ris, its = select((x.reactions, x.tanimotos)
                              for x in cls._database_.ReactionSearchCache
                              if x.signature == sig and x.operator == operator).first()

        rs = {x.id: x for x in cls.select(lambda x: x.id in ris)}
        if set_raw:
            cls.load_structures_combinations(rs.values())
        else:
            cls.load_structures(rs.values())
        yield from zip((rs[x] for x in ris), its)

    @classmethod
    def load_structures_combinations(cls, reactions):
        """
        preload all combinations of reaction structures
        :param reactions: reactions entities
        """
        # preload all molecules and structures
        ms = defaultdict(list)
        for x in select(ms for ms in cls._database_.MoleculeStructure for mr in cls._database_.MoleculeReaction
                        if ms.molecule == mr.molecule and mr.reaction in reactions).prefetch(cls._database_.Molecule):
            if x.last:
                x.molecule._cached_structure = x
            ms[x.molecule].append(x)

        for m, s in ms.items():
            m._cached_structures_all = tuple(s)

        combos, mapping, last = defaultdict(list), defaultdict(list), defaultdict(list)
        for x in cls._database_.MoleculeReaction.select(lambda x: x.reaction in reactions).order_by(lambda x: x.id):
            r = x.reaction
            combos[r].append(x.molecule.structures_all)
            mapping[r].append((x.is_product, x.mapping))
            last[r].append(x.molecule.structure)

        for x in reactions:
            # load last structure
            x._cached_structure = r = ReactionContainer()
            for s, (is_p, m) in zip(last[x], mapping[x]):
                r['products' if is_p else 'reagents'].append(s.remap(m, copy=True) if m else s)

            # load all structures
            combos_x = list(product(*combos[x]))
            if len(combos_x) == 1:
                x._cached_structures_all = (r,)
            else:
                rs = []
                for combo in combos_x:
                    r = ReactionContainer()
                    rs.append(r)
                    for s, (is_p, m) in zip(combo, mapping[x]):
                        r['products' if is_p else 'reagents'].append(s.remap(m, copy=True) if m else s)
                x._cached_structures_all = tuple(rs)

    @classmethod
    def load_structures(cls, reactions):
        """
        preload reaction last structures
        :param reactions: Reaction entities
        """
        # preload all molecules and last structures
        for x in select(ms for ms in cls._database_.MoleculeStructure for mr in cls._database_.MoleculeReaction
                        if ms.molecule == mr.molecule and ms.last and
                           mr.reaction in reactions).prefetch(cls._database_.Molecule):
            x.molecule._cached_structure = x

        for x in reactions:
            x._cached_structure = ReactionContainer()

        # load mapping and fill reaction
        for x in cls._database_.MoleculeReaction.select(lambda x: x.reaction in reactions).order_by(lambda x: x.id):
            s = x.molecule.structure
            x.reaction.structure['products' if x.is_product else 'reagents'].append(
                s.remap(x.mapping, copy=True) if x.mapping else s)

    __similarity_cache = QueryCache()
    __substructure_cache = QueryCache()


__all__ = ['SearchReaction']
