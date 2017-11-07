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
    class MoleculeManager(object):
        def new_structure(self, structure, user=None):
            if self.last_edition == structure:
                raise Exception('structure already exists')
            if self.structure_exists(structure):
                self.last_edition = structure
                raise Exception('new canonical structure established')
            try:
                fear = self.get_fear(structure)
                fingerprint = self.get_fingerprints([structure], bit_array=False)[0]
            except:
                raise Exception('structure invalid')

            mrs = list(self.reactions.order_by(lambda x: x.id))
            mis = set(mr.molecule.id for mr in mrs)
            ris = set(mr.reaction.id for mr in mrs)
            rs = {x.id: x for x in db.Reaction.select(lambda x: x.id in ris)}
            mss = {}
            for ms in list(db.MoleculeStructure.select(lambda x: x.molecule.id in mis)):
                mss.setdefault(ms.molecule.id, []).append(ms)

            new_ms = db.MoleculeStructure(self, structure, self.user if user is None else user, fingerprint, fear)
            mss[self.id].append(new_ms)

            mcs, sis = {}, {}
            for mr in mrs:
                ri = mr.reaction.id
                mi = mr.molecule.id
                ss, ps, ns, np = mcs.get(ri) or mcs.setdefault(ri, ([], [], [], []))
                if mi == self.id:
                    if mr.product:
                        np.append(len(ps))
                    else:
                        ns.append(len(ss))

                if mr.product:
                    ps.append((mi, mr.mapping))
                else:
                    ss.append((mi, mr.mapping))

            for ri, (ps, ss, ns, np) in mcs.items():
                substratslen = len(ss)
                nsi = ns + [substratslen + x for x in np]
                combos = [[(x, x.structure.remap(map_, copy=True)) for x in mss[mi]] for mi, map_ in (ss + ps)]

                for i in nsi:
                    copy = combos.copy()
                    copy[i] = [copy[i][-1]]

                    for combo in product(*copy):
                        cs = ReactionContainer(substrats=[s for _, s in combo[:substratslen]],
                                               products=[s for _, s in combo[substratslen:]])
                        mf, mgs = db.Reactions.get_mapless_fear(cs, get_merged=True)
                        fs, cgr = db.Reactions.get_fear(mgs, get_cgr=True)
                        fp = db.Reactions.get_fingerprints([cgr], bit_array=False)[0]
                        db.ReactionIndex(rs[ri], set(x for x, _ in combo), fp, fs, mf)

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

    return MoleculeManager
