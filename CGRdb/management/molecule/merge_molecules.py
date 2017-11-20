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


def mixin_factory(db):
    class MergeMolecules(object):
        def merge_molecules(self, structure):
            mol = self.find_structure(structure)
            if mol is None:
                raise Exception('Structure does not exist in db')
            if mol == self:
                raise Exception('It is the same molecule')
            try:
                mapping = self.match_structures(mol.structure, structure)
            except:
                raise Exception('Structure is invalid')
            if any(k != v for k, v in mapping.items()):
                raise Exception('Atom numbers of structures are not equal')
            mss = list(mol.structures.order_by(lambda x: x.id))
            for ms in mss:
                s = ms.structure.remap(mapping, copy=True)
                s.__class__ = MoleculeContainer
                ms.data = s.pickle()
                ms.molecule = self

            mrs = list(mol.reactions.order_by(lambda x: x.id))
            smrs = list(self.reactions.order_by(lambda x: x.id))
            for mr in mrs:
                mr.molecule = self
                map_ = {}
                if mr.mapping:
                    for k, v in mapping.items():
                        map_ = {v: mr.mapping.get(k, k)}
                    mr.mapping = map_
                else:
                    mr.mapping = {v: k for k, v in mapping.items()}

            mol.delete()
            ris = set(mr.reaction.id for mr in smrs)
            rs = {x.id: x for x in db.Reaction.select(lambda x: x.id in ris)}
            mrs = list(db.MoleculeReaction.select(lambda x: x.reaction.id in ris))
            mis = set(mr.molecule.id for mr in mrs)
            ems = [x.id for x in db.MoleculeStructure.select(lambda x: x.molecule == self and x not in mss)]
            mss, mcs = {}, {}
            for ms in list(db.MoleculeStructure.select(lambda x: x.molecule.id in mis and x.id not in ems)):
                mss.setdefault(ms.molecule.id, []).append(ms)
            for mr in mrs:
                ri = mr.reaction.id
                mi = mr.molecule.id
                subts, prods = mcs.get(ri) or mcs.setdefault(ri, ([], []))
                if mr.product:
                    prods.append((mi, mr.mapping))
                else:
                    subts.append((mi, mr.mapping))

            for ri, (subts, prods) in mcs.items():
                substratslen = len(subts)
                combos = [[(x, x.structure.remap(map_, copy=True)) for x in mss[mi]] for mi, map_ in (subts + prods)]
                for combo in product(*combos):
                    ss = [s for _, s in combo[:substratslen]]
                    ps = [s for _, s in combo[substratslen:]]
                    for s, p in ss, ps:
                        s.__class__, p.__class__ = MoleculeContainer, MoleculeContainer
                    cs = ReactionContainer(substrats=[s for s in ss],
                                           products=[s for s in ps])
                    mf, mgs = db.Reaction.get_mapless_fear(cs, get_merged=True)
                    fs, cgr = db.Reaction.get_fear(mgs, get_cgr=True)
                    fp = db.Reaction.get_fingerprints([cgr], bit_array=False)[0]
                    db.ReactionIndex(rs[ri], set(x for x, _ in combo), fp, fs, mf)
    return MergeMolecules
