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
from CGRtools.containers import MoleculeContainer, QueryContainer
from compress_pickle import dumps


class SearchMolecule:
    @classmethod
    def structure_exists(cls, structure):
        if not isinstance(structure, MoleculeContainer):
            raise TypeError('Molecule expected')
        elif not len(structure):
            raise ValueError('empty query')

        return cls._database_.MoleculeStructure.exists(signature=bytes(structure))

    @classmethod
    def find_structure(cls, structure):
        if not isinstance(structure, MoleculeContainer):
            raise TypeError('Molecule expected')
        elif not len(structure):
            raise ValueError('empty query')

        ms = cls._database_.MoleculeStructure.get(signature=bytes(structure))
        if ms:
            molecule = ms.molecule
            if ms.is_canonic:  # save if structure is canonical
                molecule.__dict__['structure_entity'] = ms
            return molecule

    @classmethod
    def find_substructures(cls, structure):
        """
        substructure search

        substructure search is 2-step process. first step is screening procedure. next step is isomorphism testing.

        :param structure: CGRtools MoleculeContainer or QueryContainer
        :return: MoleculeSearchCache object with all found molecules or None
        """
        if not isinstance(structure, (MoleculeContainer, QueryContainer)):
            raise TypeError('Molecule or Query expected')
        elif not len(structure):
            raise ValueError('empty query')

        structure = dumps(structure, compression='gzip').hex()
        ci, fnd = cls._database_.select(f" * FROM test.cgrdb_search_substructure_molecules('\\x{structure}'::bytea)")[0]
        if fnd:
            c = cls._database_.MoleculeSearchCache[ci]
            c.__dict__['_size'] = fnd
            return c

    @classmethod
    def find_similar(cls, structure):
        """
        similarity search

        :param structure: CGRtools MoleculeContainer
        :return: MoleculeSearchCache object with all found molecules or None
        """
        if not isinstance(structure, MoleculeContainer):
            raise TypeError('Molecule or Query expected')
        elif not len(structure):
            raise ValueError('empty query')

        structure = dumps(structure, compression='gzip').hex()
        ci, fnd = cls._database_.select(f" * FROM test.cgrdb_search_similar_molecules('\\x{structure}'::bytea)")[0]
        if fnd:
            c = cls._database_.MoleculeSearchCache[ci]
            c.__dict__['_size'] = fnd
            return c


__all__ = ['SearchMolecule']
