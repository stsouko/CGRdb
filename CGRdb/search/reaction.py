# -*- coding: utf-8 -*-
#
#  Copyright 2018 Ramil Nugmanov <stsouko@live.ru>
#  Copyright 2018 Adelia Fatykhova <adelik21979@gmail.com>
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
from collections import defaultdict
from itertools import product
from operator import itemgetter
from pony.orm import select, raw_sql, left_join


def mixin_factory(db):
    class Search:
        @classmethod
        def unmapped_structure_exists(cls, structure):
            return db.ReactionIndex.exists(signature=structure if isinstance(structure, bytes) else
                                           cls.get_signature(structure))

        @classmethod
        def structure_exists(cls, structure):
            return db.ReactionIndex.exists(cgr_signature=structure if isinstance(structure, bytes) else
                                           cls.get_cgr_signature(structure))

        @classmethod
        def find_unmapped_structures(cls, structure):
            fear = structure if isinstance(structure, bytes) else cls.get_signature(structure)
            return list(select(x.reaction for x in db.ReactionIndex if x.mapless_fear == fear))

        @classmethod
        def find_structure(cls, structure):
            ri = db.ReactionIndex.get(cgr_signature=structure if isinstance(structure, bytes) else
                                      cls.get_cgr_signature(structure))
            if ri:
                return ri.reaction

        @classmethod
        def find_substructures(cls, structure, number=10, *, pages=3):
            """
            cgr substructure search
            :param structure: CGRtools ReactionContainer
            :param number: top limit of returned results. not guarantee returning of all available data.
            set bigger value for this
            :param pages: max number of attempts to get required number of reactions from db.
            if required number is not reached, the next page of query result is taken.
            :return: list of Reaction entities, list of Tanimoto indexes
            """
            cgr = cls.get_cgr(structure)
            rxn, tan = [], []

            for x, y in zip(*cls.__get_reactions(cgr, '@>', number, 1, set_raw=True, overload=3)):
                if any(cls.is_substructure(rs, cgr) for rs in x.cgrs_raw):
                    rxn.append(x)
                    tan.append(y)
            if len(rxn) == number:
                return rxn, tan

            g = (x for p in range(2, pages + 1) for x in
                 zip(*cls.__get_reactions(cgr, '@>', number, p, set_raw=True, overload=3)))

            for x, y in g:
                if x not in rxn and any(cls.is_substructure(rs, cgr) for rs in x.cgrs_raw):
                    rxn.append(x)
                    tan.append(y)
                if len(rxn) == number:
                    break
            _map = sorted(zip(rxn, tan), reverse=True, key=itemgetter(1))

            return [i for i, _ in _map], [i for _, i in _map]

        @classmethod
        def find_similar(cls, structure, number=10):
            """
            cgr similar search
            :param structure: CGRtools ReactionContainer
            :param number: top limit of returned results. not guarantee returning of all available data.
            set bigger value for this
            :return: list of Reaction entities, list of Tanimoto indexes
            """
            return cls.__get_reactions(structure, '%%', number)

        @classmethod
        def __get_reactions(cls, structure, operator, number, page=1, set_raw=False, overload=2):
            """
            extract Reaction entities from ReactionIndex entities.
            cache reaction structure in Reaction entities
            :param structure: query structure
            :param operator: raw sql operator
            :return: Reaction entities
            """
            bit_set = cls.get_fingerprint(structure, bit_array=False)
            sql_select = "x.bit_array %s '%s'::int2[]" % (operator, bit_set)
            sql_smlar = "smlar(x.bit_array, '%s'::int2[], 'N.i / (N.a + N.b - N.i)') as T" % bit_set
            ris, its, iis = [], [], []
            q = select((x.reaction.id, raw_sql(sql_smlar), x.id) for x in db.ReactionIndex if raw_sql(sql_select))
            for ri, rt, ii in sorted(q.page(page, number * overload),
                                     key=itemgetter(2), reverse=True):
                if len(ris) == number:
                    break
                if ri not in ris:
                    ris.append(ri)
                    its.append(rt)
                    iis.append(ii)

            rs = {x.id: x for x in cls.select(lambda x: x.id in ris)}
            mrs = list(db.MoleculeReaction.select(lambda x: x.reaction.id in ris).order_by(lambda x: x.id))

            if set_raw:
                rsr, sis = defaultdict(list), set()
                for si, ri in left_join((x.structures.id, x.reaction.id) for x in db.ReactionIndex if x.id in iis):
                    sis.add(si)
                    rsr[ri].append(si)

                mss, mis = {}, defaultdict(list)
                for structure in db.MoleculeStructure.select(lambda x: x.id in sis):
                    mis[structure.molecule.id].append(structure)
                    if structure.last:
                        mss[structure.molecule.id] = structure

                not_last = set(mis).difference(mss)
                if not_last:
                    for structure in db.MoleculeStructure.select(lambda x: x.molecule.id in not_last and x.last):
                        mss[structure.molecule.id] = structure

                combos, mapping = defaultdict(list), defaultdict(list)
                for mr in mrs:
                    combos[mr.reaction.id].append([x for x in mis[mr.molecule.id] if x.id in rsr[mr.reaction.id]])
                    mapping[mr.reaction.id].append((mr.is_product, mr.mapping))

                rrcs = defaultdict(list)
                for ri in ris:
                    for combo in product(*combos[ri]):
                        rc = ReactionContainer()
                        for ms, (is_p, ms_map) in zip(combo, mapping[ri]):
                            rc['products' if is_p else 'reagents'].append(
                                ms.structure.remap(ms_map, copy=True) if ms_map else ms.structure)
                        rrcs[ri].append(rc)
            else:
                mss = {x.molecule.id: x for x in
                       select(ms for ms in db.MoleculeStructure for mr in db.MoleculeReaction
                              if ms.molecule == mr.molecule and mr.reaction.id in ris and ms.last)}

            rcs = {x: ReactionContainer() for x in ris}
            for mr in mrs:
                ms = mss[mr.molecule.id]
                rcs[mr.reaction.id]['products' if mr.is_product else 'reagents'].append(
                    ms.structure.remap(mr.mapping, copy=True) if mr.mapping else ms.structure)

            out = []
            for ri in ris:
                r = rs[ri]
                r.structure = rcs[ri]
                if set_raw:
                    r.structures_raw = rrcs[ri]
                out.append(r)

            return out, its

    return Search


__all__ = [mixin_factory.__name__]
