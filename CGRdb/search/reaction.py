# -*- coding: utf-8 -*-
#
#  Copyright 2018, 2019 Ramil Nugmanov <stsouko@live.ru>
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
from pony.orm import select


class SearchReaction:
    @classmethod
    def structure_exists(cls, structure):
        return cls._database_.ReactionIndex.exists(signature=bytes(~structure))

    @classmethod
    def find_structure(cls, structure):
        ri = cls._database_.ReactionIndex.get(signature=bytes(~structure))
        return ri and ri.reaction

    @classmethod
    def find_substructures(cls, structure, page=1, pagesize=100):
        """
        cgr substructure search

        :param structure: CGRtools ReactionContainer
        :param pagesize: number of results on page
        :param page: number of page in pagination
        :return: list of tuples of Reaction entities and Tanimoto indexes
        """
        def substructure_filter(cgr):
            for x, y in cls._structure_query(cgr, 'substructure', page, pagesize, set_raw=True):
                for s, c in zip(x.structures, x.cgrs):
                    if cgr <= c:
                        x.__dict__.update(structure_raw=s, cgr_raw=c)
                        yield x, y
                        break
        return list(substructure_filter(~structure))

    @classmethod
    def find_similar(cls, structure, page=1, pagesize=100):
        """
        cgr similar search

        :param structure: CGRtools ReactionContainer
        :param pagesize: number of results on page
        :param page: number of page in pagination
        :return: list of tuples of Reaction entities and Tanimoto indexes
        """
        return cls._structure_query(~structure, 'similar', page, pagesize)

    @classmethod
    def _structure_query(cls, cgr, operator, page=1, pagesize=100, set_raw=False):
        """
        extract Reaction entities from ReactionIndex entities.
        cache reaction structure in Reaction entities

        :param cgr: query structure CGR
        :param operator: raw sql operator (similar or substructure)
        :return: Reaction entities
        """
        signature = bytes(cgr)
        if not cls._database_.ReactionSearchCache.exists(signature=signature, operator=operator):
            fingerprint = cls._database_.ReactionIndex.get_fingerprint(cgr)
            if not cls._fingerprint_query(fingerprint, signature, operator):
                return []  # nothing found
        return cls._load_found(signature, operator, page, pagesize, set_raw)

    @classmethod
    def _load_found(cls, signature, operator, page=1, pagesize=100, set_raw=False):
        if page < 1:
            raise ValueError('page should be greater or equal than 1')
        if pagesize < 1:
            raise ValueError('pagesize should be greater or equal than 1')

        start = (page - 1) * pagesize
        end = start + pagesize
        ris, its = select((x.reactions[start:end], x.tanimotos[start:end])
                          for x in cls._database_.ReactionSearchCache
                          if x.signature == signature and x.operator == operator).first()
        if not ris:
            return []

        rs = {x.id: x for x in cls.select(lambda x: x.id in ris)}

        if set_raw:
            cls.load_structures_combinations(rs.values())
        else:
            cls.load_structures(rs.values())

        return list(zip((rs[x] for x in ris), its))

    @classmethod
    def _fingerprint_query(cls, fingerprint, signature, operator):
        if operator not in ('substructure', 'similar'):
            raise ValueError('invalid operator')
        schema = cls._table_[0]  # define DB schema

        cls._database_.execute(
            f"""CREATE TEMPORARY TABLE cgrdb_query ON COMMIT DROP AS
                SELECT hit.reaction, hit.tanimoto
                FROM (
                    SELECT DISTINCT ON (max_found.reaction) reaction, max_found.tanimoto
                    FROM (
                        SELECT found.reaction, found.tanimoto,
                               max(found.tanimoto) OVER (PARTITION BY found.reaction) max_tanimoto
                        FROM (
                            SELECT rs.reaction as reaction,
                                   smlar(rs.bit_array, '{fingerprint or '{}'}') AS tanimoto
                            FROM "{schema}"."ReactionIndex" rs
                            WHERE rs.bit_array {'@>' if operator == 'substructure' else '%'} '{fingerprint or '{}'}'
                        ) found
                    ) max_found
                    WHERE max_found.tanimoto = max_found.max_tanimoto
                ) hit
                ORDER BY hit.tanimoto DESC
            """)

        found = cls._database_.select('SELECT COUNT(*) FROM cgrdb_query')[0]
        if found:
            cls._database_.execute(
                f"""INSERT INTO 
                    "{schema}"."ReactionSearchCache"(signature, operator, date, reactions, indexes, tanimotos)
                    VALUES (
                        '\\x{signature.hex()}'::bytea, '{operator}', CURRENT_TIMESTAMP,
                        (SELECT array_agg(reaction) FROM cgrdb_query),
                        (SELECT array_agg(tanimoto) FROM cgrdb_query)
                    )
                """)
        else:
            cls._database_.ReactionSearchCache(signature=signature, operator=operator, reactions=[], tanimotos=[])
        return found


__all__ = ['SearchReaction']
