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
from functools import partialmethod
from pony.orm import select


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
                molecule.__dict__['structure_entity'] = ms
            molecule.__dict__['structure_raw_entity'] = ms
            return molecule

    @classmethod
    def find_substructures(cls, structure, page=1, pagesize=100):
        """
        substructure search

        substructure search is 2-step process. first step is screening procedure. next step is isomorphism testing.
        according to previously said number of elements on page can be less than pagesize.

        :param structure: CGRtools MoleculeContainer
        :param pagesize: number of results on page
        :param page: number of page in pagination
        :return: list of tuples of Molecule entities and Tanimoto indexes
        """
        return [(x, y) for x, y in cls._structure_query(structure, 'substructure', page, pagesize, set_raw=True)
                if structure < x.structure_raw]

    @classmethod
    def find_similar(cls, structure, page=1, pagesize=100):
        """
        similarity search

        :param structure: CGRtools MoleculeContainer
        :param pagesize: number of results on page
        :param page: number of page in pagination
        :return: list of tuples of Molecule entities and Tanimoto indexes
        """
        return cls._structure_query(structure, 'similar', page, pagesize)

    @classmethod
    def find_reactions_by_molecule(cls, structure, page=1, pagesize=100, *, product=None):
        """
        reaction search for molecule
        it is also possible to search reactions with molecule in proper role: reagent/product

        :param structure: CGRtools MoleculeContainer
        :param pagesize: number of results on page
        :param page: number of page in pagination
        :param product: boolean. if True, find reactions with current molecule in products
        :return: list of Reaction entities
        """
        molecule = cls.find_structure(structure)
        if molecule:
            return molecule.reactions(page, pagesize, product)
        return []

    find_reactions_by_reagent = partialmethod(find_reactions_by_molecule, product=False)
    find_reactions_by_product = partialmethod(find_reactions_by_molecule, product=True)

    @classmethod
    def find_reaction_by_similar_molecule(cls, structure, page_molecule=1, page_reaction=1, pagesize=100, *,
                                          product=None):
        """
        search for reactions with similar molecule structure
        molecule may be a reagent/product or whatever

        :param structure: CGRtools MoleculeContainer
        :param page_molecule: number of page in pagination of found molecules
        :param page_reaction: number of page in pagination of found reactions
        :param pagesize: number of molecules and reactions for each one
        :param product: boolean. if True, find reactions with current molecule in products
        :return: list of tuples of Reaction entities and Tanimoto indexes
        """
        return [(r, t) for m, t in cls.find_similar(structure, page_molecule, pagesize)
                for r in m.reactions(page_reaction, pagesize, product)]

    find_reactions_by_similar_reagent = partialmethod(find_reaction_by_similar_molecule, product=False)
    find_reactions_by_similar_product = partialmethod(find_reaction_by_similar_molecule, product=True)

    @classmethod
    def find_reaction_by_substructure_molecule(cls, structure, page_molecule=1, page_reaction=1, pagesize=100, *,
                                               product=None):
        """
        search for reactions with supergraph of current molecule structure
        molecule may be a reagent/product or whatever

        :param structure: CGRtools MoleculeContainer
        :param page_molecule: number of page in pagination of found molecules
        :param page_reaction: number of page in pagination of found reactions
        :param pagesize: number of molecules and reactions for each one
        :param product: boolean. if True, find reactions with current molecule in products
        :return: list of tuples of Reaction entities and Tanimoto indexes
        """
        return [(r, t) for m, t in cls.find_substructures(structure, page_molecule, pagesize)
                for r in m.reactions(page_reaction, pagesize, product)]

    find_reactions_by_substructure_reagent = partialmethod(find_reaction_by_substructure_molecule, product=False)
    find_reactions_by_substructure_product = partialmethod(find_reaction_by_substructure_molecule, product=True)

    @classmethod
    def _structure_query(cls, structure, operator, page=1, pagesize=100, set_raw=False):
        """
        find Molecule entities from MoleculeStructure entities.
        set to Molecule entities raw_structure property's found MoleculeStructure entities
        and preload canonical MoleculeStructure entities
        :param structure: query structure
        :param operator: raw sql operator (similar or substructure)

        :return: Molecule entities
        """
        signature = bytes(structure)
        if not cls._database_.MoleculeSearchCache.exists(signature=signature, operator=operator):
            fingerprint = cls._database_.MoleculeStructure.get_fingerprint(structure)
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
        mis, sis, sts = select((x.molecules[start:end], x.structures[start:end], x.tanimotos[start:end])
                               for x in cls._database_.MoleculeSearchCache
                               if x.signature == signature and x.operator == operator).first()
        if not mis:
            return []

        # preload molecules
        ms = {x.id: x for x in cls.select(lambda x: x.id in mis)}

        if set_raw:
            not_last = []
            # preload hit molecules structures
            for x in cls._database_.MoleculeStructure.select(lambda x: x.id in sis):
                m = x.molecule
                m.__dict__['raw_edition'] = x
                if x.last:
                    m.__dict__['last_edition'] = x
                else:
                    not_last.append(m)

            if not_last:
                for x in cls._database_.MoleculeStructure.select(lambda x: x.molecule in not_last and x.last):
                    x.molecule.__dict__['last_edition'] = x
        else:
            # preload molecules last structures
            for x in cls._database_.MoleculeStructure.select(lambda x: x.molecule.id in mis and x.last):
                x.molecule.__dict__['last_edition'] = x

        return list(zip((ms[x] for x in mis), sts))

    @classmethod
    def _fingerprint_query(cls, fingerprint, signature, operator):
        if operator not in ('substructure', 'similar'):
            raise ValueError('invalid operator')
        schema = cls._table_[0]  # define DB schema

        cls._database_.execute(
            f"""CREATE TEMPORARY TABLE cgrdb_query ON COMMIT DROP AS
                SELECT hit.molecule, hit.structure, hit.tanimoto
                FROM (
                    SELECT DISTINCT ON (max_found.molecule) molecule, max_found.structure, max_found.tanimoto
                    FROM (
                        SELECT found.molecule, found.structure, found.tanimoto,
                               max(found.tanimoto) OVER (PARTITION BY found.molecule) max_tanimoto
                        FROM (
                            SELECT ms.molecule as molecule, ms.id as structure,
                                   smlar(ms.bit_array, '{fingerprint or '{}'}') AS tanimoto
                            FROM "{schema}"."MoleculeStructure" ms
                            WHERE ms.bit_array {'@>' if operator == 'substructure' else '%'} '{fingerprint or '{}'}'
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
                    "{schema}"."MoleculeSearchCache"(signature, operator, date, molecules, structures, tanimotos)
                    VALUES (
                        '\\x{signature.hex()}'::bytea, '{operator}', CURRENT_TIMESTAMP,
                        (SELECT array_agg(molecule) FROM cgrdb_query),
                        (SELECT array_agg(structure) FROM cgrdb_query),
                        (SELECT array_agg(tanimoto) FROM cgrdb_query)
                    )
                """)
        else:
            cls._database_.MoleculeSearchCache(signature=signature, operator=operator,
                                               molecules=[], structures=[], tanimotos=[])
        return found


__all__ = ['SearchMolecule']
