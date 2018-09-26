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
from collections import defaultdict
from operator import itemgetter
from pony.orm import select, raw_sql, left_join


def mixin_factory(db):
    class Search:
        @classmethod
        def structure_exists(cls, structure):
            return db.MoleculeStructure.exists(signature=structure if isinstance(structure, bytes) else
                                               cls.get_signature(structure))

        @classmethod
        def find_structure(cls, structure):
            ms = db.MoleculeStructure.get(signature=structure if isinstance(structure, bytes) else
                                          cls.get_signature(structure))
            if ms:
                molecule = ms.molecule
                if ms.last:  # save if structure is canonical
                    molecule.last_edition = ms
                molecule.raw_edition = ms
                return molecule

        @classmethod
        def find_substructures(cls, structure, number=10):
            """
            graph substructure search
            :param structure: CGRtools MoleculeContainer
            :param number: top limit of returned results. not guarantee returning of all available data.
            set bigger value for this. negative value returns generator for all data in db.
            :return: list of tuples of Molecule entities and Tanimoto indexes
            """
            q = ((x, y) for x, y in cls._get_molecules(structure, 'substructure', number, set_raw=True)
                 if cls.is_substructure(x.structure_raw, structure))

            if number < 0:
                return q
            return sorted(q, reverse=True, key=itemgetter(1))

        @classmethod
        def find_similar(cls, structure, number=10):
            """
            graph similar search
            :param structure: CGRtools MoleculeContainer
            :param number: top limit of returned results. not guarantee returning of all available data.
            negative value returns generator for all data in db.
            :return: list of tuples of Molecule entities and Tanimoto indexes
            """
            q = cls._get_molecules(structure, 'similar', number)
            if number < 0:
                return q
            return sorted(q, reverse=True, key=itemgetter(1))

        @classmethod
        def find_reaction_by_reagent(cls, structure, number=10):
            return cls.find_reaction_by_molecule(structure, number, product=False)

        @classmethod
        def find_reaction_by_product(cls, structure, number=10):
            return cls.find_reaction_by_molecule(structure, number, product=True)

        @classmethod
        def find_reaction_by_similar_reagent(cls, structure, number=10):
            return cls.find_reaction_by_similar_molecule(structure, number, product=False)

        @classmethod
        def find_reaction_by_similar_product(cls, structure, number=10):
            return cls.find_reaction_by_similar_molecule(structure, number, product=True)

        @classmethod
        def find_reaction_by_substructure_reagent(cls, structure, number=10):
            return cls.find_reaction_by_substructure_molecule(structure, number, product=False)

        @classmethod
        def find_reaction_by_substructure_product(cls, structure, number=10):
            return cls.find_reaction_by_substructure_molecule(structure, number, product=True)

        @classmethod
        def find_reaction_by_molecule(cls, structure, number=10, *, product=None):
            """
            reaction search for molecule
            it is also possible to search reactions with molecule in proper role: reagent/product

            :param structure: CGRtools MoleculeContainer
            :param number: top limit number of returned reactions
            :param product: boolean. if True, find reactions with current molecule in products
            :return: list of Reaction entities
            """
            molecule = cls.find_structure(structure)
            if molecule:
                q = cls._get_reactions(molecule, number, product, True)
                if number < 0:
                    return q
                return list(q)

        @classmethod
        def find_reaction_by_similar_molecule(cls, structure, number=10, *, product=None):
            """
            search for reactions with similar molecule structure
            molecule may be a reagent/product or whatever

            :param structure: CGRtools MoleculeContainer
            :param number: top limit of returned results. not guarantee returning of all available data.
            negative value returns generator for all data in db.
            :param product: boolean. if True, find reactions with current molecule in products
            :return: list of tuples of Reaction entities and Tanimoto indexes
            """
            q = ((r, t) for m, t in cls.find_similar(structure, number) for r in cls._get_reactions(m, number, product))
            if number < 0:
                return q
            return sorted(q, reverse=True, key=itemgetter(1))

        @classmethod
        def find_reaction_by_substructure_molecule(cls, structure, number=10, *, product=None):
            """
            search for reactions with supergraph of current molecule structure
            molecule may be a reagent/product or whatever

            :param structure: CGRtools MoleculeContainer
            :param number: top limit of returned results. not guarantee returning of all available data.
            set bigger value for this. negative value returns generator for all data in db.
            :param product: boolean. if True, find reactions with current molecule in products
            :return: list of tuples of Reaction entities and Tanimoto indexes
            """
            q = ((r, t) for m, t in cls.find_substructures(structure, number)
                 for r in cls._get_reactions(m, number, product, True))
            if number < 0:
                return q
            return sorted(q, reverse=True, key=itemgetter(1))

        @classmethod
        def _get_reactions(cls, molecule, number, product=None, set_raw=False):
            q = left_join(x.reaction for x in db.MoleculeReaction if x.molecule == molecule).order_by(lambda x: x.id)
            if product is not None:
                q = q.where(lambda x: x.is_product == product)

            load = 100 if number < 0 or number > 100 else number
            page = 1
            while number:
                reactions = q.page(page, load)
                if not reactions:
                    break  # no more data available

                page += 1
                if number > 0:
                    l_reactions = len(reactions)
                    if l_reactions > number:
                        reactions = reactions[:number]
                        number = 0
                    else:
                        number -= l_reactions

                if set_raw:
                    mrs = db.Reaction._get_molecule_reaction_entities(reactions)
                    mss = {x.molecule.id: x for x in db.Reaction._get_last_molecule_structure_entities(reactions)}
                    mis = {mi: [ms] if mi != molecule.id else [molecule.raw_edition] for mi, ms in mss.items()}

                    rsr = defaultdict(list)
                    for mr in mrs:
                        mi = mr.molecule.id
                        rsr[mr.reaction.id].append(mss[mi].id if mi != molecule.id else molecule.raw_edition.id)

                    db.Reaction._load_structures(reactions, mss, mrs)
                    db.Reaction._load_structures_raw(reactions, mis, rsr, mrs)
                else:
                    db.Reaction._load_structures(reactions)

                yield from reactions

        @classmethod
        def _get_molecules(cls, structure, operator, number, set_raw=False, overload=1.5, page=1):
            """
            find Molecule entities from MoleculeStructure entities.
            set to Molecule entities raw_structure property's found MoleculeStructure entities
            and preload canonical MoleculeStructure entities
            :param structure: query structure
            :param operator: raw sql operator (similar or substructure)
            :param number: number of results. if negative - return all data
            :param page: starting page in pagination
            :return: Molecule entities
            """
            bit_set = cls.get_fingerprint(structure, bit_array=False)
            sql_select = "x.bit_array::int[] %s '%s'::int[]" % ('%%' if operator == 'similar' else '@>', bit_set)
            sql_smlar = "smlar(x.bit_array::int[], '%s'::int[], 'N.i / (N.a + N.b - N.i)') as T" % bit_set
            q = select((x.molecule.id, x.id, raw_sql(sql_smlar)) for x in db.MoleculeStructure if raw_sql(sql_select))

            if number < 0:
                load = 100
            else:
                load = int(number * overload)
                if load > 100:
                    load = 100

            while number:
                data = sorted(q.page(page, load), key=itemgetter(2), reverse=True)
                if not data:
                    break  # no more data available

                mis, sis, sts = [], [], []
                for mi, si, st in data:
                    if mi not in mis:
                        mis.append(mi)
                        sis.append(si)
                        sts.append(st)
                        if number == len(mis):
                            number = 0
                            break  # limit of results len to given number
                else:
                    page += 1
                    if number > 0:
                        number -= len(mis)

                ms = {x.id: x for x in cls.select(lambda x: x.id in mis)}  # preload Molecule entities
                if set_raw:
                    not_last = []
                    for x in db.MoleculeStructure.select(lambda x: x.id in sis):
                        m = ms[x.molecule.id]
                        m.raw_edition = x
                        if x.last:
                            m.last_edition = x
                        else:
                            not_last.append(m.id)

                    if not_last:
                        for x in db.MoleculeStructure.select(lambda x: x.molecule.id in not_last and x.last):
                            ms[x.molecule.id].last_edition = x
                else:
                    for x in db.MoleculeStructure.select(lambda x: x.molecule.id in mis and x.last):
                        ms[x.molecule.id].last_edition = x

                yield from zip((ms[x] for x in mis), sts)

    return Search


__all__ = [mixin_factory.__name__]
