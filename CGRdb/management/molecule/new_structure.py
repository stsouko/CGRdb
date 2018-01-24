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
from pony.orm import select, flush


def mixin_factory(db):
    class NewStructure:
        def new_structure(self, structure, user=None):
            """
            Add new representation of molecule into database / set structure as canonical if it already exists.
            Generate new indexes for all reaction with this molecule(only for new structure) and add them into database.

            :param structure: CGRtools MoleculeContainer. Structure must be mapped as it's molecule in database.
            :param user: user entity
            """
            assert sorted(self.structure) == sorted(structure), 'structure has invalid mapping'

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
            flush()

            combinations = self._reactions_combinations(reactions, molecules_structures, [new_structure], self)
            for rid, combos in combinations.items():
                structure_combinations = db.Reaction._reactions_from_combinations(combos, reactions_reagents_len[rid])

                signatures, cgr_signatures, fingerprints, cgrs = \
                    db.Reaction._prepare_reaction_sf(structure_combinations, get_cgr=True)

                doubles = []
                for c, r, cc, fp, cs, s in zip(combos, structure_combinations, cgrs, fingerprints, cgr_signatures,
                                               signatures):
                    if cs in doubles:
                        continue
                    doubles.append(cs)

                    cl = {x for _, x in c}
                    db.ReactionIndex(rid, cl, fp, cs, s)
                flush()

        @staticmethod
        def _reactions_combinations(reactions, molecules_structures, new_structures, molecule):
            molecule_id = molecule.id
            combos = defaultdict(list)
            for rid, mrs in reactions.items():
                ind = [n for n, x in enumerate(mrs) if x.molecule.id == molecule_id]
                assert ind, 'reaction has not contain updating molecules'
                while ind:
                    i = ind.pop(0)
                    tmp = []
                    for n, mr in enumerate(mrs):
                        if n == i:
                            mss = new_structures
                        elif n in ind:
                            mss = molecules_structures[mr.molecule.id] + new_structures
                        else:
                            mss = molecules_structures[mr.molecule.id]

                        tmp.append([(ms.structure.remap(mr.mapping, copy=True), ms) for ms in mss])
                    combos[rid].extend(product(*tmp))

            return dict(combos)

        def _preload_reactions_structures(self):
            # NEED PR
            # select(y for x in db.MoleculeReaction if x.molecule == self for y in db.MoleculeReaction if
            #        x.reaction == y.reaction).order_by(lambda x: x.id)
            reactions = defaultdict(lambda: {True: [], False: []})
            molecules = set()
            for mr in select(x for x in db.MoleculeReaction if x.reaction in
                             select(y.reaction for y in db.MoleculeReaction
                                    if y.molecule == self)).order_by(lambda x: x.id):
                reactions[mr.reaction.id][mr.is_product].append(mr)
                molecules.add(mr.molecule.id)

            molecules_structures = defaultdict(list)
            for ms in select(x for x in db.MoleculeStructure if x.molecule.id in molecules):
                molecules_structures[ms.molecule.id].append(ms)

            return {k: v[False] + v[True] for k, v in reactions.items()}, dict(molecules_structures), \
                   {k: len(v[False]) for k, v in reactions.items()}

    return NewStructure


__all__ = [mixin_factory.__name__]
