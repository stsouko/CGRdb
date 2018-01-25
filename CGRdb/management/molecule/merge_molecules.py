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
from pony.orm import flush, left_join
from .common import mixin_factory as mmm


def mixin_factory(db):
    class MergeMolecules(mmm(db)):
        def merge_molecules(self, structure):
            """
            Merge two molecules into one. Delete the unnecessary molecule from db.
            Generate new necessary indexes for reactions with self molecule and add them to database.
            Update molecule to reaction mapping data for all reactions with old molecule.

            :param structure: CGRtools MoleculeContainer of molecule to merge. It must exist in database and mapped as
            structure in database.
            Use when two izomers of one molecule stored in db as two different molecules.
            """
            assert set(self.structure) == set(structure), 'structure has invalid mapping'

            in_db = self.find_structure(structure)
            assert in_db, 'structure not found'
            assert in_db != self, 'structure already exists in current Molecule'

            right_structures = list(self._structures)
            left_structures = list(in_db._structures)

            mapping = self.match_structures(in_db.structure_raw, structure)
            if any(k != v for k, v in mapping.items()):  # if mapping different
                for ms in left_structures:  # update structures mapping in db
                    ms.data = ms.structure.remap(mapping).pickle()
                    if ms.last:
                        ms.last = False

                for mr in in_db.reactions:  # update reactions mapping in db
                    old_mapping = mr.mapping
                    mr.mapping = {v: old_mapping.get(k, k) for k, v in mapping.items()}
            else:
                for ms in left_structures:  # unset canonical merging structure
                    if ms.last:
                        ms.last = False

            # create indexes for merged structures in reactions
            left_cgr_signatures = list(left_join(y.cgr_signature for x in db.MoleculeStructure if x in left_structures
                                                 for y in x.reaction_indexes))
            right_cgr_signatures = list(left_join(y.cgr_signature for x in db.MoleculeStructure if x in right_structures
                                                  for y in x.reaction_indexes))

            right_reactions, molecules_structures, reactions_reagents_len = self._preload_reactions_structures()
            combinations = self._reactions_combinations(right_reactions, molecules_structures, left_structures, self)
            self._create_reactions_indexes(combinations, reactions_reagents_len, left_cgr_signatures)

            left_reactions, molecules_structures, reactions_reagents_len = in_db._preload_reactions_structures()
            combinations = self._reactions_combinations(left_reactions, molecules_structures, right_structures, in_db)
            self._create_reactions_indexes(combinations, reactions_reagents_len, right_cgr_signatures)

            for ms in left_structures:  # move structures
                ms.molecule = self

            for mr in in_db.reactions:  # move reactions
                mr.molecule = self

            for mp in in_db.properties:  # move properties
                mp.molecule = self

            flush()
            in_db.delete()
            return {'structures': len(left_structures), }

    return MergeMolecules


__all__ = [mixin_factory.__name__]
