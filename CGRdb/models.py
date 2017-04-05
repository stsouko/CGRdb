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
from bitstring import BitArray
from CGRtools.CGRcore import CGRcore
from CGRtools.CGRreactor import CGRreactor
from CGRtools.FEAR import FEAR
from CGRtools.files import MoleculeContainer, ReactionContainer
from CIMtools.descriptors.fragmentor import Fragmentor
from collections import OrderedDict
from datetime import datetime
from itertools import count, product
from networkx import relabel_nodes
from networkx.readwrite.json_graph import node_link_graph, node_link_data
from pony.orm import PrimaryKey, Required, Optional, Set, Json, db_session, left_join, select
from shutil import rmtree
from tempfile import mkdtemp
from .config import (FP_SIZE, FP_ACTIVE_BITS, FRAGMENTOR_VERSION, DEBUG, DATA_ISOTOPE, DATA_STEREO,
                     FRAGMENT_TYPE_CGR, FRAGMENT_MIN_CGR, FRAGMENT_MAX_CGR, FRAGMENT_DYNBOND_CGR,
                     FRAGMENT_TYPE_MOL, FRAGMENT_MIN_MOL, FRAGMENT_MAX_MOL, WORKPATH)
from .search.fingerprints import Fingerprints
from .search.similarity import Similarity
from .search.structure import ReactionSearch as ReactionStructureSearch, MoleculeSearch as MoleculeStructureSearch
from .search.substructure import (ReactionSearch as ReactionSubStructureSearch,
                                  MoleculeSearch as MoleculeSubStructureSearch)


class UserADHOCMeta(type):
    def __getitem__(cls, item):
        return cls(item)


class UserADHOC(metaclass=UserADHOCMeta):
    def __init__(self, uid):
        self.id = uid


class ReactionMoleculeMixin(object):
    __fingerprints = Fingerprints(FP_SIZE, active_bits=FP_ACTIVE_BITS)
    __cgr_reactor = CGRreactor(isotope=DATA_ISOTOPE, stereo=DATA_STEREO)
    __fear = FEAR(isotope=DATA_ISOTOPE, stereo=DATA_STEREO)

    @classmethod
    def descriptors_to_fingerprints(cls, descriptors):
        return cls.__fingerprints.get_fingerprints(descriptors)

    @classmethod
    def get_cgr_matcher(cls, g, h):
        return cls.__cgr_reactor.get_cgr_matcher(g, h)

    @classmethod
    def match_structures(cls, g, h):
        return next(cls.get_cgr_matcher(g, h).isomorphisms_iter())

    @classmethod
    def get_fear_string(cls, structure):
        return cls.__fear.get_cgr_string(structure)


def load_tables(db, schema, user_entity=None, reindex=False):
    if not user_entity:  # User Entity ADHOC.
        user_entity = UserADHOC

    class UserMixin(object):
        @property
        @db_session
        def user(self):
            return user_entity[self.user_id]

    class Molecule(db.Entity, UserMixin, ReactionMoleculeMixin, Similarity,
                   MoleculeStructureSearch, MoleculeSubStructureSearch):
        _table_ = '%s_molecule' % schema if DEBUG else (schema, 'molecule')
        id = PrimaryKey(int, auto=True)
        structures = Set('MoleculeStructure')
        reactions = Set('MoleculeReaction')

        __fragmentor = Fragmentor(version=FRAGMENTOR_VERSION, header=False, fragment_type=FRAGMENT_TYPE_MOL,
                                  min_length=FRAGMENT_MIN_MOL, max_length=FRAGMENT_MAX_MOL, useformalcharge=True)

        def __init__(self, structure, user, fingerprint=None, fear=None):
            if fear is None:
                fear = self.get_fear(structure)
            if fingerprint is None:
                fingerprint = self.get_fingerprints([structure])[0]

            db.Entity.__init__(self)
            self.__raw = self.__last = MoleculeStructure(self, structure, user, fingerprint, fear)

        @classmethod
        def get_fear(cls, structure):
            return cls.get_fear_string(structure)

        @classmethod
        def get_fingerprints(cls, structures):
            workpath = mkdtemp(prefix='fps_', dir=WORKPATH)
            cls.__fragmentor.set_work_path(workpath)
            f = cls.__fragmentor.get(structures)['X']
            rmtree(workpath)
            return cls.descriptors_to_fingerprints(f)

        @staticmethod
        def exists_wrapper(**kwargs):
            return MoleculeStructure.exists(**kwargs)

        @staticmethod
        def get_wrapper(**kwargs):
            raw = MoleculeStructure.get(**kwargs)
            if raw:
                molecule = raw.molecule
                molecule.raw_edition = raw
                if raw.last:
                    molecule.last_edition = raw
                return molecule

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
                raise Exception('Entity loaded incorrectly')
            return self.__raw

        @last_edition.setter
        def last_edition(self, structure):
            self.__last = structure

        @raw_edition.setter
        def raw_edition(self, structure):
            self.__raw = structure

        __last = None
        __raw = None

    class MoleculeStructure(db.Entity, UserMixin):
        _table_ = '%s_molecule_structure' % schema if DEBUG else (schema, 'molecule_structure')
        id = PrimaryKey(int, auto=True)
        user_id = Required(int, column='user')
        molecule = Required('Molecule')
        reaction_indexes = Set('ReactionIndex')
        date = Required(datetime, default=datetime.utcnow)
        last = Required(bool, default=True)

        data = Required(Json)
        fear = Required(str, unique=True)
        bitstring = Required(str) if DEBUG else Required(str, sql_type='bit(%s)' % (2 ** FP_SIZE))

        def __init__(self, molecule, structure, user, fingerprint, fear):
            data = node_link_data(structure)
            self.__cached_structure = structure
            self.__cached_fingerprint = fingerprint
            db.Entity.__init__(self, data=data, user_id=user.id, fear=fear, fingerprint=fingerprint.bin,
                               molecule=molecule)

        @property
        def structure(self):
            if self.__cached_structure is None:
                g = node_link_graph(self.data)
                g.__class__ = MoleculeContainer
                self.__cached_structure = g
            return self.__cached_structure

        @property
        def fingerprint(self):
            if self.__cached_fingerprint is None:
                self.__cached_fingerprint = BitArray(bin=self.bitstring)
            return self.__cached_fingerprint

        __cached_structure = None
        __cached_fingerprint = None

    class Reaction(db.Entity, UserMixin, ReactionMoleculeMixin, Similarity,
                   ReactionStructureSearch, ReactionSubStructureSearch):
        _table_ = '%s_reaction' % schema if DEBUG else (schema, 'reaction')
        id = PrimaryKey(int, auto=True)
        date = Required(datetime, default=datetime.utcnow)
        user_id = Required(int, column='user')
        molecules = Set('MoleculeReaction')
        reaction_indexes = Set('ReactionIndex')

        conditions = Set('Conditions')
        classes = Set('ReactionClass')
        special = Optional(Json)

        __cgr_core = CGRcore()
        __fragmentor = Fragmentor(version=FRAGMENTOR_VERSION, header=False, fragment_type=FRAGMENT_TYPE_CGR,
                                  min_length=FRAGMENT_MIN_CGR, max_length=FRAGMENT_MAX_CGR,
                                  cgr_dynbonds=FRAGMENT_DYNBOND_CGR, useformalcharge=True)

        def __init__(self, structure, user, conditions=None, special=None, fingerprints=None, fears=None,
                     mapless_fears=None, cgrs=None, substrats_fears=None, products_fears=None):
            new_mols, batch, fearset, s_fears = OrderedDict(), {}, set(), {}
            for i, f in (('substrats', substrats_fears), ('products', products_fears)):
                tmp = f if f and len(f) == len(structure[i]) else [self.get_fear_string(x) for x in structure[i]]
                fearset.update(tmp)
                s_fears[i] = tmp

            #  preload molecules entities. m.b. need refactoring if pony cache work incorrectly.
            molecules = {x.id: x for x in select(x.molecule for x in MoleculeStructure if x.fear in fearset)}
            # preload all molecules structures entities
            molecule_structures, fear_structure, fear_molecule = {}, {}, {}
            for ms in select(y for x in MoleculeStructure if x.fear in fearset
                             for y in MoleculeStructure if y.molecule == x.molecule):
                if ms.fear in fearset:
                    fear_structure[ms.fear] = ms
                    fear_molecule[ms.fear] = ms.molecule
                else:
                    molecule_structures.setdefault(ms.molecule, []).append(ms)

            m_count, all_combos = count(), {}
            for i, is_p in (('substrats', False), ('products', True)):
                for x, f in zip(structure[i], s_fears[i]):
                    ms = fear_structure.get(f)
                    n = next(m_count)
                    if ms:
                        m = fear_molecule[f]
                        mapping = self.match_structures(ms.structure, x)
                        batch[n] = (m, is_p, mapping)
                        all_combos[n] = [(x, ms)]
                        all_combos[n].extend((relabel_nodes(s.structure, mapping), s)
                                             for s in molecule_structures[m])
                    else:
                        new_mols[n] = (x, is_p, f)

            if new_mols:
                f_list, x_list = [], []
                for x, _, f in new_mols.values():
                    if f not in f_list:
                        f_list.append(f)
                        x_list.append(x)

                fear_fingerprint = dict(zip(f_list, Molecule.get_fingerprints(x_list)))
                dups = {}
                for n, (x, is_p, f) in new_mols.items():
                    if f not in dups:
                        m = Molecule(x, user, fear=f, fingerprint=fear_fingerprint[f])
                        dups[f] = m
                        mapping = None
                    else:
                        m = dups[f]
                        mapping = self.match_structures(m.structure, x)

                    batch[n] = (m, is_p, mapping)
                    all_combos[n] = [(x, m.raw_edition)]

            combos = list(product(*[all_combos[x] for x in sorted(all_combos)]))
            combolen = len(combos)
            substratslen = len(structure['substrats'])
            combo_structures = []

            def get_combos():
                if not combo_structures:
                    combo_structures.extend(ReactionContainer(substrats=[s for s, _ in x[:substratslen]],
                                                              products=[s for s, _ in x[substratslen:]])
                                            for x in combos)
                return combo_structures

            if mapless_fears is None or len(mapless_fears) != combolen:
                mapless_fears, merged = [], []
                for cs in get_combos():
                    mf, mgs = self.get_mapless_fear(cs, get_merged=True)
                    mapless_fears.append(mf)
                    merged.append(mgs)
                    is_merged = True
            else:
                merged = None
                is_merged = False

            if fears is None or len(fears) != combolen:
                fears, cgrs = [], []
                for x in merged or get_combos():
                    f, c = self.get_fear(x, get_cgr=True, is_merged=is_merged)
                    fears.append(f)
                    cgrs.append(c)

            elif cgrs is None or len(cgrs) != combolen:
                cgrs = [self.get_cgr(x, is_merged=is_merged) for x in merged or get_combos()]

            if fingerprints is None or len(fingerprints) != combolen:
                fingerprints = self.get_fingerprints(cgrs, is_cgr=True)

            db.Entity.__init__(self, user_id=user.id)

            for m, is_p, mapping in (batch[x] for x in sorted(batch)):
                MoleculeReaction(self, m, is_product=is_p, mapping=mapping)

            for c, fp, f, mf in zip(combos, fingerprints, fears, mapless_fears):
                ReactionIndex(self, [x for _, x in c], fp, f, mf)

            if conditions:
                Conditions(conditions, self, user)

            if special:
                self.special = special

            #self.__cached_cgr = cgrs
            #self.__cached_structure = structure
            #self.__cached_bitstring = fingerprints

        @classmethod
        def get_cgr(cls, *args, **kwargs):
            return cls.__cgr_core.getCGR(*args, **kwargs)

        @classmethod
        def merge_mols(cls, *args, **kwargs):
            return cls.__cgr_core.merge_mols(*args, **kwargs)

        @classmethod
        def get_fingerprints(cls, structures, is_cgr=False):
            cgrs = structures if is_cgr else [cls.get_cgr(x) for x in structures]
            workpath = mkdtemp(prefix='fps_', dir=WORKPATH)
            cls.__fragmentor.set_work_path(workpath)
            f = cls.__fragmentor.get(cgrs)['X']
            rmtree(workpath)
            return cls.descriptors_to_fingerprints(f)

        @classmethod
        def get_fear(cls, structure, is_merged=False, get_cgr=False):
            cgr = cls.get_cgr(structure, is_merged=is_merged)
            fear_string = cls.get_fear_string(cgr)
            return (fear_string, cgr) if get_cgr else fear_string

        @classmethod
        def get_mapless_fear(cls, structure, is_merged=False, get_merged=False):
            merged = structure if is_merged else cls.merge_mols(structure)
            fear_string = '%s>>%s' % (cls.get_fear_string(merged['substrats']), cls.get_fear_string(merged['products']))
            return (fear_string, merged) if get_merged else fear_string

        @property
        def cgr(self):
            if self.__cached_cgr is None:
                self.__cached_cgr = self.get_cgr(self.structure)
            return self.__cached_cgr

        @property
        def structure(self):
            if self.__cached_structure is None:
                r = ReactionContainer()
                for m in self.molecules.order_by(lambda x: x.id):
                    r['products' if m.product else 'substrats'].append(
                        relabel_nodes(m.molecule.structure, m.mapping) if m.mapping else m.molecule.structure)
                self.__cached_structure = r
            return self.__cached_structure

        __cached_structure = None
        __cached_cgr = None

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
            return dict(self._mapping) if self.__mapping else None

        @staticmethod
        def mapping_transform(mapping):
            return [(k, v) for k, v in mapping.items() if k != v] or None

    class ReactionIndex(db.Entity):
        _table_ = '%s_reaction_index' % schema if DEBUG else (schema, 'reaction_index')
        id = PrimaryKey(int, auto=True)
        reaction = Required('Reaction')
        structures = Set('MoleculeStructure', table='%s_reaction_index_structure' % schema if DEBUG else
                                                    (schema, 'reaction_index_structure'))

        fear = Required(str, unique=True)
        mapless_fear = Required(str)
        bitstring = Required(str) if DEBUG else Required(str, sql_type='bit(%s)' % (2 ** FP_SIZE))

        def __init__(self, reaction, structures, fingerprint, fear, mapless_fear):
            db.Entity.__init__(self, reaction=reaction, fear=fear, bitstring=fingerprint.bin, mapless_fear=mapless_fear)
            for m in structures:
                self.structures.add(m)
            self.__cached_fingerprint = fingerprint

        @property
        def fingerprint(self):
            if self.__cached_fingerprint is None:
                self.__cached_fingerprint = BitArray(bin=self.bitstring)
            return self.__cached_fingerprint

        __cached_fingerprint = None

    class Conditions(db.Entity, UserMixin):
        _table_ = '%s_conditions' % schema if DEBUG else (schema, 'conditions')
        id = PrimaryKey(int, auto=True)
        date = Required(datetime)
        user_id = Required(int, column='user')
        data = Required(Json)
        reaction = Required('Reaction')

        def __init__(self, data, reaction, user, date=None):
            if date is None:
                date = datetime.utcnow()
            db.Entity.__init__(self, user_id=user.id, date=date, reaction=reaction, data=data)

    class ReactionClass(db.Entity):
        _table_ = '%s_reaction_class' % schema if DEBUG else (schema, 'reaction_class')
        id = PrimaryKey(int, auto=True)
        name = Required(str)
        reactions = Set('Reaction', table='%s_reaction_reaction_class' % schema if DEBUG else
                                          (schema, 'reaction_reaction_class'))

    Molecule.load_tree(reindex=reindex)
    Reaction.load_tree(reindex=reindex)
    return Molecule, Reaction, Conditions
