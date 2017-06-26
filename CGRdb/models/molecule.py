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
from CGRtools.containers import MoleculeContainer, ReactionContainer
from CIMtools.descriptors.fragmentor import Fragmentor
from datetime import datetime
from itertools import product
from operator import itemgetter
from pony.orm import PrimaryKey, Required, Optional, Set, Json, select, raw_sql
from .mixins import ReactionMoleculeMixin, FingerprintMixin
from ..config import (FRAGMENTOR_VERSION, DEBUG, DATA_ISOTOPE, DATA_STEREO, FRAGMENT_TYPE_MOL, FRAGMENT_MIN_MOL,
                      FRAGMENT_MAX_MOL, WORKPATH)


def load_tables(db, schema, user_entity):
    class UserMixin(object):
        @property
        def user(self):
            return user_entity[self.user_id]

    class Molecule(db.Entity, UserMixin, ReactionMoleculeMixin):
        _table_ = '%s_molecule' % schema if DEBUG else (schema, 'molecule')
        id = PrimaryKey(int, auto=True)
        date = Required(datetime, default=datetime.utcnow)
        user_id = Required(int, column='user')
        structures = Set('MoleculeStructure')
        reactions = Set('MoleculeReaction')
        properties = Set('MoleculeProperties')
        classes = Set('MoleculeClass')
        special = Optional(Json)

        __fragmentor = Fragmentor(version=FRAGMENTOR_VERSION, header=False, fragment_type=FRAGMENT_TYPE_MOL,
                                  workpath=WORKPATH, min_length=FRAGMENT_MIN_MOL, max_length=FRAGMENT_MAX_MOL,
                                  useformalcharge=True)

        def __init__(self, structure, user, fingerprint=None, fear=None):
            if fear is None:
                fear = self.get_fear(structure)
            if fingerprint is None:
                fingerprint = self.get_fingerprints([structure], bit_array=False)[0]

            db.Entity.__init__(self, user_id=user.id)
            self.__last = MoleculeStructure(self, structure, user, fingerprint, fear)

        @classmethod
        def get_fear(cls, structure):
            return structure.get_fear_hash(isotope=DATA_ISOTOPE, stereo=DATA_STEREO)

        @classmethod
        def get_fingerprints(cls, structures, bit_array=True):
            f = cls.__fragmentor.get(structures).X
            return cls.descriptors_to_fingerprints(f, bit_array=bit_array)

        @property
        def structure_raw(self):
            return self.raw_edition.structure

        @property
        def structure(self):
            return self.last_edition.structure

        @property
        def fingerprint_raw(self):
            return self.raw_edition.fingerprint

        @property
        def fingerprint(self):
            return self.last_edition.fingerprint

        @property
        def last_edition(self):
            if self.__last is None:
                self.__last = self.structures.filter(lambda x: x.last).first()
            return self.__last

        @property
        def raw_edition(self):
            if self.__raw is None:
                raise Exception('Available in entities from queries results only')
            return self.__raw

        @last_edition.setter
        def last_edition(self, structure):
            if self.__last is None:
                self.__last = structure

        @raw_edition.setter
        def raw_edition(self, structure):
            if self.__raw is None:
                self.__raw = structure

        @classmethod
        def structure_exists(cls, structure, is_fear=False):
            return MoleculeStructure.exists(fear=structure if is_fear else cls.get_fear(structure))

        @classmethod
        def find_structure(cls, structure, is_fear=False):
            ms = MoleculeStructure.get(fear=structure if is_fear else cls.get_fear(structure))
            if ms:
                molecule = ms.molecule
                if ms.last:  # save if structure is canonical
                    molecule.last_edition = ms

                return molecule

        @classmethod
        def find_substructures(cls, structure, number=10):
            """
            graph substructure search
            :param structure: CGRtools MoleculeContainer
            :param number: top limit of returned results. not guarantee returning of all available data.
            set bigger value for this
            :return: list of Molecule entities, list of Tanimoto indexes
            """
            tmp = [(x, y) for x, y in zip(*cls.__get_molecules(cls.get_fingerprints([structure], bit_array=False)[0],
                                                               '@>', number, set_raw=True))
                   if cls.get_cgr_matcher(x.structure_raw, structure).subgraph_is_isomorphic()]
            return [x for x, _ in tmp], [x for _, x in tmp]

        @classmethod
        def find_similar(cls, structure, number=10):
            """
            graph similar search
            :param structure: CGRtools MoleculeContainer
            :param number: top limit of returned results. not guarantee returning of all available data.
            set bigger value for this
            :return: list of Molecule entities, list of Tanimoto indexes
            """
            return cls.__get_molecules(cls.get_fingerprints([structure], bit_array=False)[0], '%%', number)

        @staticmethod
        def __get_molecules(bit_set, operator, number, set_raw=False):
            """
            find Molecule entities from MoleculeStructure entities.
            set to Molecule entities raw_structure property's found MoleculeStructure entities
            and preload canonical MoleculeStructure entities
            :param bit_set: fingerprint as a bits set
            :param operator: raw sql operator
            :return: Molecule entities
            """
            sql_select = "x.bit_array %s '%s'::int2[]" % (operator, bit_set)
            sql_smlar = "smlar(x.bit_array, '%s'::int2[], 'N.i / (N.a + N.b - N.i)') as T" % bit_set
            mis, sts, sis = [], [], []
            for mi, si, st in sorted(select((x.molecule.id, x.id, raw_sql(sql_smlar)) for x in MoleculeStructure
                                     if raw_sql(sql_select)).limit(number * 2), key=itemgetter(2), reverse=True):
                if len(mis) == number:
                    break  # limit of results len to given number
                if mi not in mis:
                    mis.append(mi)
                    sis.append(si)
                    sts.append(st)

            ms = {x.id: x for x in Molecule.select(lambda x: x.id in mis)}  # preload Molecule entities
            if set_raw:
                ss = {x.molecule.id: x for x in MoleculeStructure.select(lambda x: x.id in sis)}
                not_last = []
            else:
                ss = {x.molecule.id: x for x in MoleculeStructure.select(lambda x: x.molecule.id in mis and x.last)}
                not_last = False

            for mi in mis:
                molecule = ms[mi]
                structure = ss[mi]
                if set_raw:
                    molecule.raw_edition = structure
                    if structure.last:
                        molecule.last_edition = structure
                    else:
                        not_last.append(mi)
                else:
                    molecule.last_edition = structure

            if not_last:
                for structure in MoleculeStructure.select(lambda x: x.molecule.id in not_last and x.last):
                    ms[structure.molecule.id].last_edition = structure

            return [ms[x] for x in mis], sts

        def new_structure(self, structure, user=None):
            if self.structure_exists(structure):
                raise Exception('structure already exists')
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
            for ms in list(MoleculeStructure.select(lambda x: x.molecule.id in mis)):
                mss.setdefault(ms.molecule.id, []).append(ms)

            new_ms = MoleculeStructure(self, structure, self.user if user is None else user, fingerprint, fear)
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

        __last = None
        __raw = None

    class MoleculeStructure(db.Entity, UserMixin, FingerprintMixin):
        _table_ = '%s_molecule_structure' % schema if DEBUG else (schema, 'molecule_structure')
        id = PrimaryKey(int, auto=True)
        user_id = Required(int, column='user')
        molecule = Required('Molecule')
        reaction_indexes = Set('ReactionIndex')
        date = Required(datetime, default=datetime.utcnow)
        last = Required(bool, default=True)
        data = Required(Json)
        fear = Required(bytes, unique=True)
        bit_array = Required(Json, column='bit_list')

        def __init__(self, molecule, structure, user, fingerprint, fear):
            data = structure.pickle()
            fp, bs = self._init_fingerprint(fingerprint)

            db.Entity.__init__(self, data=data, user_id=user.id, fear=fear, bit_array=bs, molecule=molecule)

            self.__cached_structure = structure
            self.fingerprint = fp

        @property
        def structure(self):
            if self.__cached_structure is None:
                self.__cached_structure = MoleculeContainer.unpickle(self.data)
            return self.__cached_structure

        __cached_structure = None
