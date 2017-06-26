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
from CGRtools.containers import ReactionContainer, MergedReaction, MoleculeContainer
from CGRtools.preparer import CGRcombo
from CGRtools.strings import hash_cgr_string
from CIMtools.descriptors.fragmentor import Fragmentor
from collections import OrderedDict
from datetime import datetime
from itertools import count, product
from operator import itemgetter
from pony.orm import PrimaryKey, Required, Optional, Set, Json, select, raw_sql, left_join
from .mixins import ReactionMoleculeMixin, FingerprintMixin
from ..config import (FRAGMENTOR_VERSION, DEBUG, DATA_ISOTOPE, DATA_STEREO, FRAGMENT_TYPE_CGR, FRAGMENT_MIN_CGR,
                      FRAGMENT_MAX_CGR, FRAGMENT_DYNBOND_CGR, WORKPATH)


def load_tables(db, schema, user_entity):
    class Reaction(db.Entity, ReactionMoleculeMixin):
        _table_ = '%s_reaction' % schema if DEBUG else (schema, 'reaction')
        id = PrimaryKey(int, auto=True)
        date = Required(datetime, default=datetime.utcnow)
        user_id = Required(int, column='user')
        molecules = Set('MoleculeReaction')
        reaction_indexes = Set('ReactionIndex')
        conditions = Set('ReactionConditions')
        classes = Set('ReactionClass')
        special = Optional(Json)

        __cgr_core = CGRcombo()
        __fragmentor = Fragmentor(version=FRAGMENTOR_VERSION, header=False, fragment_type=FRAGMENT_TYPE_CGR,
                                  min_length=FRAGMENT_MIN_CGR, max_length=FRAGMENT_MAX_CGR, workpath=WORKPATH,
                                  cgr_dynbonds=FRAGMENT_DYNBOND_CGR, useformalcharge=True)

        def __init__(self, structure, user, conditions=None, special=None, fingerprints=None, fears=None,
                     mapless_fears=None, cgrs=None, substrats_fears=None, products_fears=None):
            """
            storing reaction in DB.
            :param structure: CGRtools ReactionContainer
            :param user: user entity
            :param conditions: list of Json serializable Data (expected list of dicts)
            :param special: Json serializable Data (expected dict)
            :param fingerprints: reaction fingerprints for all existing in db molecules structures combinations.
             for example: reaction A + B -> C. A has 2 structure in db, B - 3 and C - 1. number of combinations = 6.
             order of fingerprints have to be same as in product(A[], B[], C[]) where X[] is ordered by id list of
             structures of molecules.
            :param fears: fear strings of reaction. see fingerprints for details
            :param mapless_fears: mapless fear strings of reaction. see fingerprints for details
            :param cgrs: list of all possible CGRs of reaction. see fingerprints for details
            :param substrats_fears: fears of structure substrats in same order as substrats molecules
            :param products_fears: see substrats_fears
            """
            batch, all_combos = self.__load_molecules(structure, user, substrats_fears=substrats_fears,
                                                      products_fears=products_fears)

            combos = list(product(*[all_combos[x] for x in sorted(all_combos)]))
            combolen = len(combos)
            substratslen = len(structure.substrats)
            combo_structures = [ReactionContainer(substrats=[s for s, _ in x[:substratslen]],
                                                  products=[s for s, _ in x[substratslen:]]) for x in combos]

            if mapless_fears is None or len(mapless_fears) != combolen:
                mapless_fears, merged = [], []
                for cs in combo_structures:
                    mf, mgs = self.get_mapless_fear(cs, get_merged=True)
                    mapless_fears.append(mf)
                    merged.append(mgs)
            else:
                merged = None

            if fears is None or len(fears) != combolen:
                fears, cgrs = [], []
                for x in merged or combo_structures:
                    f, c = self.get_fear(x, get_cgr=True)
                    fears.append(f)
                    cgrs.append(c)

            elif cgrs is None or len(cgrs) != combolen:
                cgrs = [self.get_cgr(x) for x in merged or combo_structures]

            if fingerprints is None or len(fingerprints) != combolen:
                fingerprints = self.get_fingerprints(cgrs, bit_array=False)

            db.Entity.__init__(self, user_id=user.id)

            for m, is_p, mapping in (batch[x] for x in sorted(batch)):
                MoleculeReaction(self, m, is_product=is_p, mapping=mapping)

            for c, cs, cc, fp, f, mf in zip(combos, combo_structures, cgrs, fingerprints, fears, mapless_fears):
                cl = [x for _, x in c]
                ReactionIndex(self, cl, fp, f, mf)
                if self.__cached_structure is None and all(x.last for x in cl):
                    self.__cached_structure = cs
                    self.__cached_cgr = cc

            if conditions:
                for c in conditions:
                    db.ReactionConditions(c, self, user)

            if special:
                self.special = special

        def __load_molecules(self, structure, user, substrats_fears=None, products_fears=None):
            new_mols, batch, fearset, s_fears = OrderedDict(), {}, set(), {}
            for i, f in (('substrats', substrats_fears), ('products', products_fears)):
                tmp = f if f and len(f) == len(structure[i]) else [self.get_fear_string(x) for x in structure[i]]
                fearset.update(tmp)
                s_fears[i] = tmp

            # preload molecules entities. pony caching it.
            list(select(x.molecule for x in db.MoleculeStructure if x.fear in fearset))
            # preload all molecules structures entities
            molecule_structures, fear_structure = {}, {}
            for ms in select(y for x in db.MoleculeStructure if x.fear in fearset
                             for y in db.MoleculeStructure if y.molecule == x.molecule):
                if ms.fear in fearset:
                    fear_structure[ms.fear] = ms
                else:
                    molecule_structures.setdefault(ms.molecule, []).append(ms)

            m_count, all_combos = count(), {}
            for i, is_p in (('substrats', False), ('products', True)):
                for x, f in zip(structure[i], s_fears[i]):
                    ms = fear_structure.get(f)
                    n = next(m_count)
                    if ms:
                        mapping = self.match_structures(ms.structure, x)
                        batch[n] = (ms.molecule, is_p, mapping)

                        tmp = [(x, ms)]
                        if ms.molecule in molecule_structures:
                            tmp.extend((x.structure.remap(mapping, copy=True), x)
                                       for x in molecule_structures[ms.molecule])

                        all_combos[n] = sorted(tmp, key=lambda x: x[1].id)
                    else:
                        new_mols[n] = (x, is_p, f)

            if new_mols:
                f_list, x_list = [], []
                for x, _, f in new_mols.values():
                    if f not in f_list:
                        f_list.append(f)
                        x_list.append(x)

                fear_fingerprint = dict(zip(f_list, db.Molecule.get_fingerprints(x_list, bit_array=False)))
                dups = {}
                for n, (x, is_p, f) in new_mols.items():
                    if f not in dups:
                        m = db.Molecule(x, user, fear=f, fingerprint=fear_fingerprint[f])
                        dups[f] = m
                        mapping = None
                    else:
                        m = dups[f]
                        mapping = self.match_structures(m.structure, x)

                    batch[n] = (m, is_p, mapping)
                    all_combos[n] = [(x, m.last_edition)]

            return batch, all_combos

        @property
        def user(self):
            return user_entity[self.user_id]

        @classmethod
        def get_cgr(cls, structure):
            return cls.__cgr_core.getCGR(structure)

        @classmethod
        def merge_mols(cls, structure):
            return cls.__cgr_core.merge_mols(structure)

        @classmethod
        def get_fingerprints(cls, structures, bit_array=True):
            cgrs = [x if isinstance(x, MoleculeContainer) else cls.get_cgr(x) for x in structures]
            f = cls.__fragmentor.get(cgrs).X
            return cls.descriptors_to_fingerprints(f, bit_array=bit_array)

        @classmethod
        def get_fear(cls, structure, get_cgr=False):
            cgr = cls.get_cgr(structure)
            fear_string = cgr.get_fear_hash(isotope=DATA_ISOTOPE, stereo=DATA_STEREO)
            return (fear_string, cgr) if get_cgr else fear_string

        @classmethod
        def get_mapless_fear(cls, structure, get_merged=False):
            merged = structure if isinstance(structure, MergedReaction) else cls.merge_mols(structure)
            fear_string = hash_cgr_string('%s>>%s' % (merged.substrats.get_fear(isotope=DATA_ISOTOPE,
                                                                                stereo=DATA_STEREO),
                                                      merged.products.get_fear(isotope=DATA_ISOTOPE,
                                                                               stereo=DATA_STEREO)))
            return (fear_string, merged) if get_merged else fear_string

        @property
        def cgr(self):
            if self.__cached_cgr is None:
                self.__cached_cgr = self.get_cgr(self.structure)
            return self.__cached_cgr

        @property
        def cgrs_raw(self):
            if self.__cached_cgrs_raw is None:
                self.__cached_cgrs_raw = [self.get_cgr(x) for x in self.structures_raw]
            return self.__cached_cgrs_raw

        @property
        def structure(self):
            if self.__cached_structure is None:
                mrs = list(self.molecules.order_by(lambda x: x.id))
                mss = {x.molecule.id: x for x in
                       select(ms for ms in db.MoleculeStructure for mr in MoleculeReaction
                              if ms.molecule == mr.molecule and mr.reaction == self and ms.last)}

                r = ReactionContainer()
                for mr in mrs:
                    ms = mss[mr.molecule.id]
                    r['products' if mr.product else 'substrats'].append(
                        ms.structure.remap(mr.mapping, copy=True) if mr.mapping else ms.structure)
                self.__cached_structure = r
            return self.__cached_structure

        @property
        def structures_raw(self):
            if self.__cached_structures_raw is None:
                raise Exception('Available in entities from queries results only')
            return self.__cached_structures_raw

        @structure.setter
        def structure(self, structure):
            self.__cached_structure = structure

        @structures_raw.setter
        def structures_raw(self, structure):
            self.__cached_structures_raw = structure

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

        @classmethod
        def mapless_structure_exists(cls, structure):
            return ReactionIndex.exists(mapless_fear=structure if isinstance(structure, bytes) else
                                        cls.get_mapless_fear(structure))

        @classmethod
        def structure_exists(cls, structure):
            return ReactionIndex.exists(fear=structure if isinstance(structure, bytes) else cls.get_fear(structure))

        @classmethod
        def find_mapless_structures(cls, structure):
            fear = structure if isinstance(structure, bytes) else cls.get_mapless_fear(structure)
            return list(select(x.reaction for x in ReactionIndex if x.mapless_fear == fear))

        @classmethod
        def find_structure(cls, structure):
            ri = ReactionIndex.get(fear=structure if isinstance(structure, bytes) else cls.get_fear(structure))
            if ri:
                return ri.reaction

        @classmethod
        def find_substructures(cls, structure, number=10):
            """
            cgr substructure search
            :param structure: CGRtools ReactionContainer
            :param number: top limit of returned results. not guarantee returning of all available data.
            set bigger value for this
            :return: list of Reaction entities, list of Tanimoto indexes
            """
            cgr = cls.get_cgr(structure)
            tmp = [(x, y) for x, y in zip(*cls.__get_reactions(cls.get_fingerprints([structure], bit_array=False)[0],
                                                               '@>', number, set_raw=True))
                   if any(cls.get_cgr_matcher(rs, cgr).subgraph_is_isomorphic() for rs in x.cgrs_raw)]
            return [x for x, _ in tmp], [x for _, x in tmp]

        @classmethod
        def find_similar(cls, structure, number=10):
            """
            cgr similar search
            :param structure: CGRtools ReactionContainer
            :param number: top limit of returned results. not guarantee returning of all available data.
            set bigger value for this
            :return: list of Reaction entities, list of Tanimoto indexes
            """
            return cls.__get_reactions(cls.get_fingerprints([structure], bit_array=False)[0], '%%', number)

        @staticmethod
        def __get_reactions(bit_set, operator, number, set_raw=False):
            """
            extract Reaction entities from ReactionIndex entities.
            cache reaction structure in Reaction entities
            :param bit_set: fingerprint as a bits set
            :param operator: raw sql operator
            :return: Reaction entities
            """
            sql_select = "x.bit_array %s '%s'::int2[]" % (operator, bit_set)
            sql_smlar = "smlar(x.bit_array, '%s'::int2[], 'N.i / (N.a + N.b - N.i)') as T" % bit_set
            ris, its, iis = [], [], []
            for ri, rt, ii in sorted(select((x.reaction.id, raw_sql(sql_smlar), x.id) for x in ReactionIndex
                                     if raw_sql(sql_select)).limit(number * 2), key=itemgetter(2), reverse=True):
                if len(ris) == number:
                    break
                if ri not in ris:
                    ris.append(ri)
                    its.append(rt)
                    iis.append(ii)

            rs = {x.id: x for x in Reaction.select(lambda x: x.id in ris)}
            mrs = list(MoleculeReaction.select(lambda x: x.reaction.id in ris).order_by(lambda x: x.id))

            if set_raw:
                rsr, sis = {}, set()
                for si, ri in left_join((x.structures.id, x.reaction.id) for x in ReactionIndex if x.id in iis):
                    sis.add(si)
                    rsr.setdefault(ri, []).append(si)

                mss, mis = {}, {}
                for structure in db.MoleculeStructure.select(lambda x: x.id in sis):
                    mis.setdefault(structure.molecule.id, []).append(structure)
                    if structure.last:
                        mss[structure.molecule.id] = structure

                not_last = set(mis).difference(mss)
                if not_last:
                    for structure in db.MoleculeStructure.select(lambda x: x.molecule.id in not_last and x.last):
                        mss[structure.molecule.id] = structure

                combos, mapping = {}, {}
                for mr in mrs:
                    combos.setdefault(mr.reaction.id, []).append([x for x in mis[mr.molecule.id]
                                                                  if x.id in rsr[mr.reaction.id]])
                    mapping.setdefault(mr.reaction.id, []).append((mr.product, mr.mapping))

                rrcs = {}
                for ri in ris:
                    for combo in product(*combos[ri]):
                        rc = ReactionContainer()
                        for ms, (is_p, ms_map) in zip(combo, mapping[ri]):
                            rc['products' if is_p else 'substrats'].append(
                                ms.structure.remap(ms_map, copy=True) if ms_map else ms.structure)
                        rrcs.setdefault(ri, []).append(rc)
            else:
                mss = {x.molecule.id: x for x in
                       select(ms for ms in db.MoleculeStructure for mr in MoleculeReaction
                              if ms.molecule == mr.molecule and mr.reaction.id in ris and ms.last)}

            rcs = {x: ReactionContainer() for x in ris}
            for mr in mrs:
                ms = mss[mr.molecule.id]
                rcs[mr.reaction.id]['products' if mr.product else 'substrats'].append(
                    ms.structure.remap(mr.mapping, copy=True) if mr.mapping else ms.structure)

            out = []
            for ri in ris:
                r = rs[ri]
                r.structure = rcs[ri]
                if set_raw:
                    r.structures_raw = rrcs[ri]
                out.append(r)

            return out, its

        def add_conditions(self, data, user):
            db.ReactionConditions(data, self, user)

        __cached_structure = None
        __cached_cgr = None
        __cached_structures_raw = None
        __cached_cgrs_raw = None

    class MoleculeReaction(db.Entity):
        _table_ = '%s_molecule_reaction' % schema if DEBUG else (schema, 'molecule_reaction')
        id = PrimaryKey(int, auto=True)
        reaction = Required('Reaction')
        molecule = Required('Molecule')
        product = Required(bool, default=False)
        _mapping = Optional(Json, column='mapping')

        def __init__(self, reaction, molecule, is_product=False, mapping=None):
            mapping = mapping and self.mapping_transform(mapping)
            db.Entity.__init__(self, reaction=reaction, molecule=molecule, product=is_product, _mapping=mapping)

        @property
        def mapping(self):
            if self.__cached_mapping is None:
                self.__cached_mapping = dict(self._mapping) if self._mapping else {}
            return self.__cached_mapping

        @mapping.setter
        def mapping(self, mapping):
            self._mapping = self.mapping_transform(mapping)

        @staticmethod
        def mapping_transform(mapping):
            return [(k, v) for k, v in mapping.items() if k != v] or None

        __cached_mapping = None

    class ReactionIndex(db.Entity, FingerprintMixin):
        _table_ = '%s_reaction_index' % schema if DEBUG else (schema, 'reaction_index')
        id = PrimaryKey(int, auto=True)
        reaction = Required('Reaction')
        structures = Set('MoleculeStructure', table='%s_reaction_index_structure' % schema if DEBUG else
                         (schema, 'reaction_index_structure'))

        fear = Required(bytes, unique=True)
        mapless_fear = Required(bytes)
        bit_array = Required(Json, column='bit_list')

        def __init__(self, reaction, structures, fingerprint, fear, mapless_fear):
            fp, bs = self._init_fingerprint(fingerprint)

            db.Entity.__init__(self, reaction=reaction, fear=fear, mapless_fear=mapless_fear, bit_array=bs)
            for m in set(structures):
                self.structures.add(m)
            self.fingerprint = fp

        def update_fingerprint(self, fingerprint):
            fp, bs = self._init_fingerprint(fingerprint)
            self.fingerprint = fp
            self.bit_array = bs
