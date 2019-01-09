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
from pony.orm import select, left_join
from ..utils import QueryCache


class SearchMolecule:
    @classmethod
    def structure_exists(cls, structure):
        return cls._database_.MoleculeStructure.exists(signature=bytes(structure))

    @classmethod
    def find_structure(cls, structure):
        ms = cls._database_.MoleculeStructure.get(signature=bytes(structure))
        if ms:
            molecule = ms.molecule
            if ms.last:  # save if structure is canonical
                molecule._cached_structure = ms
            molecule._cached_structure_raw = ms
            return molecule

    @classmethod
    def find_substructures(cls, structure, number=10, page=1):
        """
        substructure search

        :param structure: CGRtools MoleculeContainer
        :param number: number of results. if zero or negative - return all data
        :param page: starting page in pagination
        :return: list of tuples of Molecule entities and Tanimoto indexes
        """
        q = ((x, y) for x, y in cls._get_molecules(structure, 'substructure', number, page, set_raw=True)
             if structure < x.structure_raw)
        if number > 0:
            return list(q)
        return q

    @classmethod
    def find_similar(cls, structure, number=10, page=1):
        """
        similarity search

        :param structure: CGRtools MoleculeContainer
        :param number: number of results. if zero or negative - return all data
        :param page: starting page in pagination
        :return: list of tuples of Molecule entities and Tanimoto indexes
        """
        q = cls._get_molecules(structure, 'similar', number, page)
        if number > 0:
            return list(q)
        return q

    @classmethod
    def find_reactions_by_reagent(cls, structure, number=10, page=1):
        return cls.find_reactions_by_molecule(structure, number, page, product=False)

    @classmethod
    def find_reactions_by_product(cls, structure, number=10, page=1):
        return cls.find_reactions_by_molecule(structure, number, page, product=True)

    @classmethod
    def find_reactions_by_similar_reagent(cls, structure, number=10, page_molecule=1, page_reaction=1):
        return cls.find_reaction_by_similar_molecule(structure, number, page_molecule, page_reaction, product=False)

    @classmethod
    def find_reactions_by_similar_product(cls, structure, number=10, page_molecule=1, page_reaction=1):
        return cls.find_reaction_by_similar_molecule(structure, number, page_molecule, page_reaction, product=True)

    @classmethod
    def find_reactions_by_substructure_reagent(cls, structure, number=10, page_molecule=1, page_reaction=1):
        return cls.find_reaction_by_substructure_molecule(structure, number, page_molecule, page_reaction, product=False)

    @classmethod
    def find_reactions_by_substructure_product(cls, structure, number=10, page_molecule=1, page_reaction=1):
        return cls.find_reaction_by_substructure_molecule(structure, number, page_molecule, page_reaction, product=True)

    @classmethod
    def find_reactions_by_molecule(cls, structure, number=10, page=1, *, product=None):
        """
        reaction search for molecule
        it is also possible to search reactions with molecule in proper role: reagent/product

        :param structure: CGRtools MoleculeContainer
        :param number: number of results. if zero or negative - return all data
        :param page: starting page in pagination
        :param product: boolean. if True, find reactions with current molecule in products
        :return: list of Reaction entities
        """
        molecule = cls.find_structure(structure)
        if molecule:
            q = cls._get_reactions(molecule, number, page, product)
            if number > 0:
                return list(q)
            return q

    @classmethod
    def find_reaction_by_similar_molecule(cls, structure, number=10, page_molecule=1, page_reaction=1, *, product=None):
        """
        search for reactions with similar molecule structure
        molecule may be a reagent/product or whatever

        :param structure: CGRtools MoleculeContainer
        :param number: number of molecules and reactions for each one. if zero or negative - return all data
        :param page_molecule: starting page in pagination of found molecules
        :param page_reaction: starting page in pagination of found reactions
        :param product: boolean. if True, find reactions with current molecule in products
        :return: list of tuples of Reaction entities and Tanimoto indexes
        """
        q = ((r, t) for m, t in cls.find_similar(structure, number, page_molecule) 
             for r in cls._get_reactions(m, number, page_reaction, product))
        if number > 0:
            return list(q)
        return q

    @classmethod
    def find_reaction_by_substructure_molecule(cls, structure, number=10, page_molecule=1, page_reaction=1, *,
                                               product=None):
        """
        search for reactions with supergraph of current molecule structure
        molecule may be a reagent/product or whatever

        :param structure: CGRtools MoleculeContainer
        :param number: number of molecules and reactions for each one. if zero or negative - return all data.
        :param page_molecule: starting page in pagination of found molecules
        :param page_reaction: starting page in pagination of found reactions
        :param product: boolean. if True, find reactions with current molecule in products
        :return: list of tuples of Reaction entities and Tanimoto indexes
        """
        q = ((r, t) for m, t in cls.find_substructures(structure, number, page_molecule)
             for r in cls._get_reactions(m, number, page_reaction, product, True))
        if number > 0:
            return q
        return q

    @classmethod
    def _get_reactions(cls, molecule, number, page, product=None, set_all=False):
        q = left_join(x.reaction for x in cls._database_.MoleculeReaction
                      if x.molecule == molecule).order_by(lambda x: x.id)
        if product is not None:
            q = q.where(lambda x: x.is_product == product)

        if number > 0:
            reactions = q.page(page, number)
            if not reactions:
                raise StopIteration
            if set_all:
                cls._database_.Reaction.load_structures_combinations(reactions)
            else:
                cls._database_.Reaction.load_structures(reactions)
            yield from reactions
        else:
            while True:  # emulate buffered stream
                reactions = q.page(page, 100)
                if not reactions:
                    raise StopIteration
                page += 1

                if set_all:
                    cls._database_.Reaction.load_structures_combinations(reactions)
                else:
                    cls._database_.Reaction.load_structures(reactions)
                yield from reactions

    @classmethod
    def _get_molecules(cls, structure, operator, number, page=1, set_raw=False):
        """
        find Molecule entities from MoleculeStructure entities.
        set to Molecule entities raw_structure property's found MoleculeStructure entities
        and preload canonical MoleculeStructure entities
        :param structure: query structure
        :param operator: raw sql operator (similar or substructure)

        :return: Molecule entities
        """
        cache = cls.__substructure_cache if operator == 'substructure' else cls.__similarity_cache
        operator = '@>' if operator == 'substructure' else '%'
        start = (page - 1) * number
        end = start + number
        sig = bytes(structure)
        key = (sig, operator)
        if key in cache:
            mis, sis, sts = cache[key]
            if number > 0:
                mis = mis[start:end]
                sis = sis[start:end]
                sts = sts[start:end]
                if not mis:
                    raise StopIteration
        elif not cls._database_.MoleculeSearchCache.exists(signature=sig, operator=operator):
            schema = cls._table_  # define DB schema
            if schema and isinstance(schema, tuple):
                schema = schema[0]

            fingerprint = cls._database_.MoleculeStructure.get_fingerprint(structure) or '{}'

            cls._database_.execute(
                f"""CREATE TEMPORARY TABLE temp_hits ON COMMIT DROP AS
                    SELECT hit.molecule, hit.structure, hit.tanimoto
                    FROM (
                        SELECT DISTINCT ON (max_found.molecule) molecule, max_found.structure, max_found.tanimoto
                        FROM (
                            SELECT found.molecule, found.structure, found.tanimoto,
                                   max(found.tanimoto) OVER (PARTITION BY found.molecule) max_tanimoto
                            FROM (
                                SELECT ms.molecule as molecule, ms.id as structure,
                                       smlar(ms.bit_array, '{fingerprint}', 'N.i / (N.a + N.b - N.i)') AS tanimoto
                                FROM "{schema}"."MoleculeStructure" ms
                                WHERE ms.bit_array {operator} '{fingerprint}'
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
                        "{schema}"."MoleculeSearchCache"(signature, operator, date, molecules, structures, tanimotos)
                        VALUES (
                            $sig, {operator}, CURRENT_TIMESTAMP,
                            (SELECT array_agg(molecule) FROM temp_hits),
                            (SELECT array_agg(structure) FROM temp_hits),
                            (SELECT array_agg(tanimoto) FROM temp_hits)
                        )
                    """)
                if number > 0:
                    mis, sis, sts = select((x.molecules[start:end], x.structures[start:end], x.tanimotos[start:end])
                                           for x in cls._database_.MoleculeSearchCache
                                           if x.signature == sig and x.operator == operator).first()
                else:
                    mis, sis, sts = select((x.molecules, x.structures, x.tanimotos)
                                           for x in cls._database_.MoleculeSearchCache
                                           if x.signature == sig and x.operator == operator).first()
            elif found:
                mis, sis, sts = cache[key] = cls._database_.select(
                    "SELECT array_agg(molecule), array_agg(structure), array_agg(tanimoto) FROM temp_hits"
                )[0]
                if number > 0:
                    mis = mis[start:end]
                    sis = sis[start:end]
                    sts = sts[start:end]
            else:
                raise StopIteration('not found')
        elif number > 0:
            mis, sis, sts = select((x.molecules[start:end], x.structures[start:end], x.tanimotos[start:end])
                                   for x in cls._database_.MoleculeSearchCache
                                   if x.signature == sig and x.operator == operator).first()
            if not mis:
                raise StopIteration
        else:
            mis, sis, sts = select((x.molecules, x.structures, x.tanimotos)
                                   for x in cls._database_.MoleculeSearchCache
                                   if x.signature == sig and x.operator == operator).first()

        # preload molecules
        ms = {x.id: x for x in cls.select(lambda x: x.id in mis)}

        if set_raw:
            not_last = []
            # preload hit molecules structures
            for x in cls._database_.MoleculeStructure.select(lambda x: x.id in sis):
                m = x.molecule
                m._cached_structure_raw = x
                if x.last:
                    m._cached_structure = x
                else:
                    not_last.append(m)

            if not_last:
                for x in cls._database_.MoleculeStructure.select(lambda x: x.molecule in not_last and x.last):
                    x.molecule._cached_structure = x
        else:
            # preload molecules last structures
            for x in cls._database_.MoleculeStructure.select(lambda x: x.molecule.id in mis and x.last):
                x.molecule._cached_structure = x

        yield from zip((ms[x] for x in mis), sts)

    __similarity_cache = QueryCache()
    __substructure_cache = QueryCache()


__all__ = ['SearchMolecule']
