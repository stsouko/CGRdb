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
        def find_substructures(cls, structure, number=10):
            """
            graph substructure search
            :param structure: CGRtools MoleculeContainer
            :param number: top limit of returned results. not guarantee returning of all available data.
            set bigger value for this
            :return: list of Molecule entities, list of Tanimoto indexes
            """
            mol, tan = [], []
            for x, y in zip(*cls.__get_molecules(structure, '@>', number, set_raw=True, overload=3)):
                if cls.get_matcher(x.structure_raw, structure).subgraph_is_isomorphic():
                    mol.append(x)
                    tan.append(y)
            return mol, tan

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
        def __get_molecules(cls, structure, operator, number, set_raw=False, overload=2):
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
                                     if raw_sql(sql_select)).limit(number * overload), key=itemgetter(2), reverse=True):
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
