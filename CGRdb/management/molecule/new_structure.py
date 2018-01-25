# -*- coding: utf-8 -*-
#
#  Copyright 2017, 2018 Ramil Nugmanov <stsouko@live.ru>
#  Copyright 2017 Adelia Fatykhova <adelik21979@gmail.com>
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
from .common import mixin_factory as mmm


def mixin_factory(db):
    class NewStructure(mmm(db)):
        def new_structure(self, structure, user=None):
            """
            Add new representation of molecule into database / set structure as canonical if it already exists.
            Generate new indexes for all reaction with this molecule(only for new structure) and add them into database.

            :param structure: CGRtools MoleculeContainer. Structure must be mapped as it's molecule in database.
            :param user: user entity
            """
            assert set(self.structure) == set(structure), 'structure has invalid mapping'

            signature = self.get_signature(structure)
            in_db = self.find_structure(signature)
            if in_db:
                assert in_db == self, 'structure already exists in another Molecule'
                assert in_db.last_edition != in_db.raw_edition, 'structure already canonical in current Molecule'

                in_db.last_edition.last = False
                in_db.raw_edition.last = True
                in_db.last_edition = in_db.raw_edition
                return True

            fingerprint = self.get_fingerprint(structure, bit_array=False)

            # now structure look like valid. starting addition
            reactions, molecules_structures, reactions_reagents_len = self._preload_reactions_structures()
            new_structure = db.MoleculeStructure(self, structure, self.user if user is None else user,
                                                 fingerprint, signature)
            self.last_edition.last = False
            self.last_edition = new_structure

            combinations = self._reactions_combinations(reactions, molecules_structures, [new_structure], self)
            self._create_reactions_indexes(combinations, reactions_reagents_len)

    return NewStructure


__all__ = [mixin_factory.__name__]
