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
    class NewStructure(object):
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
    return NewStructure
