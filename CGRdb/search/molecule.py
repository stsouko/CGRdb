# -*- coding: utf-8 -*-
#
#  Copyright 2018 Ramil Nugmanov <stsouko@live.ru>
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
from operator import itemgetter
from pony.orm import select, raw_sql


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
        def find_substructures(cls, structure, number=10, trials=3):
            """
            graph substructure search
            :param structure: CGRtools MoleculeContainer
            :param number: top limit of returned results. not guarantee returning of all available data.
            set bigger value for this
            :return: list of Molecule entities, list of Tanimoto indexes
            """
            mol, tan = [], []
            for page in range(1, trials+1):
                for x, y in zip(*cls.__get_molecules(structure, '@>', number, page, set_raw=True, overload=2)):
                    if cls.is_substructure(x.structure_raw, structure):
                        mol.append(x)
                        tan.append(y)
                if len(mol) >= number:
                    break
            _map = sorted(zip(mol, tan), reverse=True, key=itemgetter(1))
            mol, tan = [i for i, _ in _map], [i for _, i in _map]
            return mol[:number], tan[:number]

        @classmethod
        def find_similar(cls, structure, number=10):
            """
            graph similar search
            :param structure: CGRtools MoleculeContainer
            :param number: top limit of returned results. not guarantee returning of all available data.
            set bigger value for this
            :return: list of Molecule entities, list of Tanimoto indexes
            """
            return cls.__get_molecules(structure, '%%', number)

        @classmethod
        def find_reaction_by_reagent(cls, structure, number=10):
            return cls.__find_reaction_by_molecule(structure, number, product=False)

        @classmethod
        def find_reaction_by_product(cls, structure, number=10):
            return cls.__find_reaction_by_molecule(structure, number, product=True)

        @classmethod
        def find_reaction_by_similar_reagent(cls, structure, number=10):
            return cls.__find_similar_in_reaction(structure, number, product=False)

        @classmethod
        def find_reaction_by_similar_product(cls, structure, number=10):
            return cls.__find_similar_in_reaction(structure, number, product=True)

        @classmethod
        def find_reaction_by_substructure_reagent(cls, structure, number=10):
            return cls.__find_substructures_in_reaction(structure, number, product=False)

        @classmethod
        def find_reaction_by_substructure_product(cls, structure, number=10):
            return cls.__find_substructures_in_reaction(structure, number, product=True)

        @classmethod
        def __find_reaction_by_molecule(cls, structure, number, product=None):
            """
            reaction search for molecule
            it is also possible to search reactions with molecule in proper role: reagent/product

            :param structure: CGRtools MoleculeContainer
            :param number: top limit number of returned reactions
            :param product: boolean. if True, find reactions with current molecule in products
            :return: list of Reaction entities
            """
            molecule = cls.find_structure(structure)
            return cls.__find_reactions(molecule, number, product)

        @classmethod
        def __find_similar_in_reaction(cls, structure, number, product=None):
            """
            search for reactions with similar molecule structure
            molecule may be a reagent/product or whatever

            :param structure: CGRtools MoleculeContainer
            :param number: top limit number of returned reactions
            :return: list of Reaction entities
            """
            molecules = cls.find_similar(structure, number)
            return cls.__find_reactions(molecules, number, product)

        @classmethod
        def __find_substructures_in_reaction(cls, structure, number, product=None):
            """
            search for reactions with supergraph of current molecule structure
            molecule may be a reagent/product or whatever

            :param structure: CGRtools MoleculeContainer
            :param number: top limit number of returned reactions
            :return: list of Reaction entities
            """
            molecules = cls.find_substructures(structure, number)
            return cls.__find_reactions(molecules, number, product)

        @classmethod
        def __find_reactions(cls, molecules, number, product=None):
            reactions = []
            tanimoto = []
            for m, t in zip(*molecules) if isinstance(molecules, tuple) else [[molecules, None]]:
                q = db.MoleculeReaction.select(lambda mr: mr.molecule == m)
                if product is not None:
                    q = q.filter(lambda mr: mr.is_product == product)
                for mr in q:
                    if len(reactions) == number:
                        break
                    if mr.reaction not in reactions:
                        reactions.append(mr.reaction)
                        if t is not None:
                            tanimoto.append(t)
            return (reactions, tanimoto) if isinstance(molecules, tuple) else reactions

        @classmethod
        def __get_molecules(cls, structure, operator, number, page=1, set_raw=False, overload=2):
            """
            find Molecule entities from MoleculeStructure entities.
            set to Molecule entities raw_structure property's found MoleculeStructure entities
            and preload canonical MoleculeStructure entities
            :param structure: query structure
            :param operator: raw sql operator
            :return: Molecule entities
            """
            bit_set = cls.get_fingerprint(structure, bit_array=False)
            sql_select = "x.bit_array %s '%s'::int2[]" % (operator, bit_set)
            sql_smlar = "smlar(x.bit_array, '%s'::int2[], 'N.i / (N.a + N.b - N.i)') as T" % bit_set
            mis, sis, sts = [], [], []
            for mi, si, st in sorted(select((x.molecule.id, x.id, raw_sql(sql_smlar)) for x in db.MoleculeStructure
                                     if x.molecule.id not in mis and raw_sql(sql_select)).page(page, number * overload),
                                     key=itemgetter(2), reverse=True):
                if len(mis) == number:
                    break  # limit of results len to given number
                if mi not in mis:
                    mis.append(mi)
                    sis.append(si)
                    sts.append(st)

            ms = {x.id: x for x in cls.select(lambda x: x.id in mis)}  # preload Molecule entities
            if set_raw:
                ss = {x.molecule.id: x for x in db.MoleculeStructure.select(lambda x: x.id in sis)}
                not_last = []
            else:
                ss = {x.molecule.id: x for x in db.MoleculeStructure.select(lambda x: x.molecule.id in mis and x.last)}
                not_last = False

            for mi in mis:
                molecule = ms[mi]
                structure = ss[mi]
                if set_raw:
                    molecule.raw_edition = structure
                    if structure.last:
                        molecule.last_edition = structure
                    else:
                        not_last.append(mi)
                else:
                    molecule.last_edition = structure

            if not_last:
                for structure in db.MoleculeStructure.select(lambda x: x.molecule.id in not_last and x.last):
                    ms[structure.molecule.id].last_edition = structure

            return [ms[x] for x in mis], sts

    return Search


__all__ = [mixin_factory.__name__]
