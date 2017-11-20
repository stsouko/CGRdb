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
from CGRtools.containers import ReactionContainer, MoleculeContainer
from itertools import product
import networkx as nx


def mixin_factory(db):
    class ChangeMixin(object):
        def change(self, structure, new):
            mol = self.find_structure(structure)
            if mol is None:
                raise Exception('This structure not exist in db')
            if mol != self:
                raise Exception('Molecule has not this structure')

            mapping = {}
            for n in nx.nodes(new):
                mapping[n] = n
            try:
                structure.fear = self.get_fear(new)
                structure.bit_array = self.get_fingerprints([new], bit_array=False)[0]
            except:
                raise Exception('Invalid structure')
            mss = list(mol.structures.order_by(lambda x: x.id))  # self molecule mss with old structure
            structure.data = new.pickle()

            mrs = list(mol.reactions.order_by(lambda x: x.id))  # self molecule mrs with old structure
            for ms in mss:
                s = ms.structure.remap(mapping, copy=True)
                s.__class__ = MoleculeContainer
                ms.data = s.pickle()
            for mr in mrs:
                if mr.mapping:
                    for k in mapping.keys():
                        for v in mr.mapping.values():
                            mr.mapping = {k: v}
                else:
                    mr.mapping = mapping
            ris = set(mr.reaction.id for mr in mrs)  # ris for self molecule
            rs = {ri.id: ri for ri in db.ReactionIndex.select(lambda x: x.id in ris).order_by(lambda x: x.id)
                  for ms in mss if ms.structure == structure and ms in ri.structures}  # ris for new structure only
            rrs = {}
            for ri in rs.values():
                rrs.setdefault(ri.reaction.id, []).append(ri.id)
            mrs = list(db.MoleculeReaction.select(lambda x: x.reaction.id in ris).order_by(lambda x: x.id))  # all mrs
            mis = set(mr.molecule.id for mr in mrs)  # all mis for all reactions
            ems = set(x.id for x in db.MoleculeStructure.select(lambda x: x.id) if x.molecule == self and x not in mss)

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
                for x in rrs.get(ri):
                    substratslen = len(subts)
                    combos = [[(x.structure.remap(map_, copy=True)) for x in mss[mi]] for mi, map_ in (subts + prods)]
                    for combo in product(*combos):
                        ss = [s for s in combo[:substratslen]]
                        ps = [s for s in combo[substratslen:]]
                        for s, p in ss, ps:
                            s.__class__, p.__class__ = MoleculeContainer, MoleculeContainer
                        cs = ReactionContainer(substrats=[s for s in ss],
                                               products=[s for s in ps])
                        mf, mgs = db.Reaction.get_mapless_fear(cs, get_merged=True)
                        fs, cgr = db.Reaction.get_fear(mgs, get_cgr=True)
                        fp = db.Reaction.get_fingerprints([cgr], bit_array=False)[0]

                        rs[x].fear = fs
                        rs[x].mapless_fear = mf
                        rs[x].update_fingerprint(fp)
    return ChangeMixin
