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
from collections import defaultdict
from itertools import product
from pony.orm import select, left_join


def mixin_factory(db):
    class MoleculeManagement:
        @staticmethod
        def _reactions_combinations(reactions, molecules_structures, new_structures, molecule):
            molecule_id = molecule.id
            combos = defaultdict(list)
            for r, mrs in reactions.items():
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
                    combos[r].extend(product(*tmp))

            return dict(combos)

        def _preload_reactions_structures(self):
            # NEED PR
            # select(y for x in db.MoleculeReaction if x.molecule == self for y in db.MoleculeReaction if
            #        x.reaction == y.reaction).order_by(lambda x: x.id)
            reactions = {}
            for r in left_join(x.reaction for x in db.MoleculeReaction if x.molecule == self):
                reactions[r] = {True: [], False: []}

            molecules = set()
            for mr in select(x for x in db.MoleculeReaction if x.reaction in reactions.keys()).order_by(lambda x: x.id):
                reactions[mr.reaction][mr.is_product].append(mr)
                molecules.add(mr.molecule.id)

            molecules_structures = defaultdict(list)
            for ms in select(x for x in db.MoleculeStructure if x.molecule.id in molecules):
                molecules_structures[ms.molecule.id].append(ms)

            return {k: v[False] + v[True] for k, v in reactions.items()}, dict(molecules_structures), \
                   {k: len(v[False]) for k, v in reactions.items()}

        @staticmethod
        def _create_reactions_indexes(combinations, reactions_reagents_len):
            for r, combos in combinations.items():
                signatures, cgr_signatures, fingerprints = \
                    r._prepare_reaction_sf(combos, reactions_reagents_len[r])
                r._create_reaction_indexes(combos, fingerprints, cgr_signatures, signatures)

    return MoleculeManagement


__all__ = [mixin_factory.__name__]
