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
from collections import defaultdict
from itertools import product
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

            # preload left and right reactions and structures
            right_reactions, right_molecules_structures, right_reagents_len = self._preload_reactions_structures()
            left_reactions, left_molecules_structures, left_reagents_len = in_db._preload_reactions_structures()

            # reactions contains both merging molecule
            rs = set(right_reactions)
            mixed_reactions = rs.intersection(left_reactions)

            # get existing signatures
            exists_cgr_signatures = {}
            for ri in db.ReactionIndex.select(lambda x: x.reaction in rs.union(left_reactions)):
                exists_cgr_signatures[ri.cgr_signature] = ri.reaction

            right_combinations = self._reactions_combinations({r: mrs for r, mrs in right_reactions.items()
                                                               if r not in mixed_reactions},
                                                              right_molecules_structures, left_structures, self)
            right_doubles = self.__create_reactions_indexes(right_combinations, right_reagents_len,
                                                            exists_cgr_signatures)
            left_unique = {r: mrs for r, mrs in left_reactions.items()
                           if r not in right_doubles and r not in mixed_reactions}
            if left_unique:  # update indexes of unique left reactions
                left_combinations = self._reactions_combinations(left_unique, left_molecules_structures,
                                                                 right_structures, in_db)
                left_doubles = self.__create_reactions_indexes(left_combinations, left_reagents_len,
                                                               exists_cgr_signatures)
            else:
                left_doubles = {}

            mixed_unique = {r: mrs for r, mrs in right_reactions.items()
                            if r in mixed_reactions and r not in right_doubles and r not in left_doubles}
            if mixed_unique:
                mixed_combinations = self.__mixed_reactions_combinations(mixed_unique, right_molecules_structures,
                                                                         self, in_db)
                self.__create_mixed_reactions_indexes(mixed_combinations, right_reagents_len, exists_cgr_signatures)

            for l, r in right_doubles.items():
                for i in l.reaction_indexes:
                    i.reaction = r
                for c in l.conditions:
                    c.reaction = r
                # todo: classes
                l.delete()
            for l, r in left_doubles.items():
                for i in l.reaction_indexes:
                    i.reaction = r
                for c in l.conditions:
                    c.reaction = r
                # todo: classes
                l.delete()

            for ms in left_structures:  # move structures
                ms.molecule = self

            for mr in in_db.reactions:  # move reactions
                mr.molecule = self

            for mp in in_db.properties:  # move properties
                mp.molecule = self

            # todo: classes

            in_db.delete()
            return {'structures': len(left_structures), 'reactions': len(left_reactions), 'mixed': len(mixed_reactions)}

        @staticmethod
        def __mixed_reactions_combinations(reactions, molecules_structures, right_molecule, left_molecule):
            combos = defaultdict(list)
            right_molecule_id = right_molecule.id
            left_molecule_id = left_molecule.id
            left_right_ids = (right_molecule_id, left_molecule_id)
            combo_structures = molecules_structures[right_molecule_id] + molecules_structures[left_molecule_id]

            for r, mrs in reactions.items():
                tmp = []
                for mr in mrs:
                    mr_mid = mr.molecule.id
                    mapping = mr.mapping
                    mss = combo_structures if mr_mid in left_right_ids else molecules_structures[mr_mid]
                    tmp.append([(ms.structure.remap(mapping, copy=True), ms) for ms in mss])
                combos[r].extend(product(*tmp))
            return dict(combos)

        @staticmethod
        def __create_mixed_reactions_indexes(combinations, reactions_reagents_len, exists_cgr_signatures):
            for r, combos in combinations.items():
                signatures, cgr_signatures, fingerprints = \
                    r._prepare_reaction_sf(combos, reactions_reagents_len[r])
                clean_signatures, clean_cgr_signatures, clean_fingerprints, clean_combinations = [], [], [], []
                for cc, s, cs, f in zip(combinations, signatures, cgr_signatures, fingerprints):
                    if cs not in exists_cgr_signatures:
                        clean_signatures.append(s)
                        clean_cgr_signatures.append(cs)
                        clean_fingerprints.append(f)
                        clean_combinations.append(cc)
                r._create_reaction_indexes(clean_combinations, clean_fingerprints, clean_cgr_signatures,
                                           clean_signatures)

        @staticmethod
        def __create_reactions_indexes(combinations, reactions_reagents_len, exists_cgr_signatures):
            exists_cgr_signatures_set = set(exists_cgr_signatures)
            doubles = {}
            for r, combos in combinations.items():
                combos, signatures, cgr_signatures, fingerprints = r._prepare_reaction_sf(combos,
                                                                                          reactions_reagents_len[r])
                d = exists_cgr_signatures_set.intersection(cgr_signatures)
                if d:
                    for x in d:
                        left = exists_cgr_signatures[x]
                        if left not in doubles:
                            doubles[left] = r
                    clean_signatures, clean_cgr_signatures, clean_fingerprints, clean_combinations = [], [], [], []
                    for cc, s, cs, f in zip(combinations, signatures, cgr_signatures, fingerprints):
                        if cs not in d:
                            clean_signatures.append(s)
                            clean_cgr_signatures.append(cs)
                            clean_fingerprints.append(f)
                            clean_combinations.append(cc)
                    if not clean_combinations:
                        continue
                    combos, cgr_signatures, signatures = clean_combinations, clean_cgr_signatures, clean_signatures
                    fingerprints = clean_fingerprints
                r._create_reaction_indexes(combos, fingerprints, cgr_signatures, signatures)
            return doubles

    return MergeMolecules


__all__ = [mixin_factory.__name__]
