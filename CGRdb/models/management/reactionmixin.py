# -*- coding: utf-8 -*-
#
#  Copyright 2017 Ramil Nugmanov <stsouko@live.ru>
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

from CGRtools.containers import ReactionContainer
from itertools import product


def mixin_factory(db):
    class ReactionManager(object):
        def remap(self, structure):
            fear = self.get_fear(structure)
            if self.structure_exists(fear):
                raise Exception('This structure already exists')

            mf = self.get_mapless_fear(structure)
            ris = {x.mapless_fear: x for x in self.reaction_indexes}
            if mf not in ris:
                raise Exception('passed structure not equal to structure in DB')

            new_map, mss = [], {}
            for ms in ris[mf].structures:
                mss.setdefault(ms.molecule.id, []).append(ms)

            ir = iter(structure.substrats)
            ip = iter(structure.products)
            mrs = list(self.molecules.order_by(lambda x: x.id))
            for mr in mrs:
                user_structure = next(ip) if mr.product else next(ir)
                for ms in mss[mr.molecule.id]:
                    try:
                        mapping = self.match_structures(ms.structure, user_structure)
                        new_map.append(mapping)
                        break
                    except StopIteration:
                        pass
                else:
                    raise Exception('Structure not isomorphic to structure in DB')

            for mr, mp in zip(mrs, new_map):
                if any(k != v for k, v in mp.items()):
                    mr.mapping = mp

            mis = set(x.molecule.id for x in mrs)
            exists_ms = set(y.id for x in mss.values() for y in x)
            for ms in db.MoleculeStructure.select(lambda x: x.molecule.id in mis and x.id not in exists_ms):
                mss.setdefault(ms.molecule.id, []).append(ms)

            substs, prods = [], []
            for mr in mrs:
                s = [x.structure.remap(mr.mapping, copy=True) for x in mss[mr.molecule.id]]
                if mr.product:
                    prods.append(s)
                else:
                    substs.append(s)

            combos = product(*(substs + prods))
            substratslen = len(structure.substrats)
            check = []
            for x in combos:
                cs = ReactionContainer(substrats=[s for s in x[:substratslen]], products=[s for s in x[substratslen:]])
                mf, mgs = self.get_mapless_fear(cs, get_merged=True)
                fs, cgr = self.get_fear(mgs, get_cgr=True)
                fp = self.get_fingerprints([cgr], bit_array=False)[0]

                ris[mf].fear = fs
                ris[mf].update_fingerprint(fp)
                check.append(mf)

            if len(ris) != len(check):
                raise Exception('number of reaction indexes not equal to number of structure combinations')

    return ReactionManager
