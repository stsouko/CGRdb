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
from collections import OrderedDict
from datetime import datetime
from pony.orm import PrimaryKey, Required, Optional, Set, Json, db_session
from networkx import relabel_nodes
from bitstring import BitArray
from itertools import count
from networkx.readwrite.json_graph import node_link_graph, node_link_data
from CGRtools.FEAR import FEAR
from CGRtools.CGRreactor import CGRreactor
from CGRtools.CGRcore import CGRcore
from CGRtools.files import MoleculeContainer, ReactionContainer
from CIMtools.descriptors.fragmentor import Fragmentor
from .search.fingerprints import Fingerprints
from .search.similarity import Similarity
from .search.structure import ReactionSearch as ReactionStructureSearch, MoleculeSearch as MoleculeStructureSearch
from .search.substructure import (ReactionSearch as ReactionSubStructureSearch,
                                  MoleculeSearch as MoleculeSubStructureSearch)
from ..config import (FP_SIZE, FP_ACTIVE_BITS, FRAGMENTOR_VERSION, DEBUG, DATA_ISOTOPE, DATA_STEREO,
                      FRAGMENT_TYPE_CGR, FRAGMENT_MIN_CGR, FRAGMENT_MAX_CGR, FRAGMENT_DYNBOND_CGR,
                      FRAGMENT_TYPE_MOL, FRAGMENT_MIN_MOL, FRAGMENT_MAX_MOL)

fear = FEAR(isotope=DATA_ISOTOPE, stereo=DATA_STEREO)
cgr_core = CGRcore()
cgr_reactor = CGRreactor(isotope=DATA_ISOTOPE, stereo=DATA_STEREO)
fingerprints = Fingerprints(FP_SIZE, active_bits=FP_ACTIVE_BITS)


class FingerprintMixin(object):
    @property
    def bitstring_fingerprint(self):
        if self.__cached_bitstring is None:
            self.__cached_bitstring = BitArray(bin=self.fingerprint)
        return self.__cached_bitstring

    def flush_cache(self):
        self.__cached_bitstring = None

    __cached_bitstring = None


class IsomorphismMixin(object):
    @staticmethod
    def match_structures(g, h):
        return next(cgr_reactor.get_cgr_matcher(g, h).isomorphisms_iter())


def load_tables(db, schema, user_entity):
    class UserMixin(object):
        @property
        @db_session
        def user(self):
            return user_entity[self.user_id]

    class Molecule(db.Entity, UserMixin, FingerprintMixin, IsomorphismMixin, Similarity,
                   MoleculeStructureSearch, MoleculeSubStructureSearch):
        _table_ = '%s_molecule' % schema if DEBUG else (schema, 'molecule')
        id = PrimaryKey(int, auto=True)
        date = Required(datetime)
        user_id = Required(int, column='user')
        data = Required(Json)
        fear = Required(str, unique=True)
        fingerprint = Required(str) if DEBUG else Required(str, sql_type='bit(%s)' % (2 ** FP_SIZE))

        children = Set('Molecule', reverse='parent', cascade_delete=True)
        parent = Optional('Molecule', reverse='children')
        last = Required(bool, default=True)

        merge_source = Set('MoleculeMerge', reverse='target')  # molecules where self is more correct
        merge_target = Set('MoleculeMerge', reverse='source')  # links to correct molecules

        reactions = Set('MoleculeReaction')

        def __init__(self, structure, user, fingerprint=None, fear_string=None):
            data = node_link_data(structure)

            if fear_string is None:
                fear_string = self.get_fear(structure)
            if fingerprint is None:
                fingerprint = self.get_fingerprints([structure])[0]

            self.__cached_structure_raw = structure
            self.__cached_bitstring = fingerprint
            db.Entity.__init__(self, data=data, user_id=user.id, fear=fear_string, fingerprint=fingerprint.bin,
                               date=datetime.utcnow())

        def update_structure(self, structure, user):
            """
            update structure representation. atom mapping should be equal to self.
            :param structure: Molecule container
            :param user: user entity
            :return: True if updated. False if conflict found.
            """
            new_hash = {k: v['element'] for k, v in structure.nodes(data=True)}
            old_hash = {k: v['element'] for k, v in self.structure_raw.nodes(data=True)}
            if new_hash != old_hash:
                raise Exception('Structure or mapping not match')

            fear_string = self.get_fear(structure)
            exists = Molecule.get(fear=fear_string)
            if not exists:
                m = Molecule(structure, user, fear_string=fear_string)
                for mr in self.last_edition.reactions:
                    ''' replace current last molecule edition in all reactions.
                    '''
                    mr.molecule = m
                    mr.reaction.refresh_fear_fingerprint()

                self.last_edition.last = False
                m.parent = self.parent or self
                self.__last_edition = m
                return True

            ''' this code not optimal. but this procedure is rare if db correctly standardized before population.
            '''
            ex_parent = exists.parent or exists
            if ex_parent != (self.parent or self) and not any((x.target.parent or x.target) == ex_parent
                                                              for x in self.merge_target):
                ''' if exists structure not already in merge list
                '''
                mapping = self.match_structures(structure, exists.structure_raw)
                MoleculeMerge(target=exists, source=self,
                              mapping=[(k, v) for k, v in mapping.items() if k != v] or None)

            return False

        def merge_molecule(self, molecule):
            m = Molecule[molecule]
            mm = MoleculeMerge.get(target=m, source=self)
            if not mm:
                return False
            ''' replace self in reactions to last edition of mergable molecule.
            '''
            mmap = dict(mm.mapping or [])
            mapping = [(n, mmap.get(n, n)) for n in self.structure_raw.nodes()]
            for mr in self.last_edition.reactions:
                rmap = dict(mr.mapping or [])
                mr.mapping = [(k, v) for k, v in ((v, rmap.get(k, k)) for k, v in mapping) if k != v] or None
                mr.molecule = m.last_edition
                mr.reaction.refresh_fear_fingerprint()

            ''' remap self'''
            if self.parent:
                tmp = [self.parent] + list(self.parent.children)
            else:
                tmp = [self] + list(self.children)

            for x in tmp:
                x.data = node_link_data(relabel_nodes(x.structure_raw, mmap))

            ''' set self.parent to molecule chain
            '''
            if m.parent:
                tmp = [m.parent] + list(m.parent.children)
            else:
                tmp = [m] + list(m.children)

            for x in tmp:
                x.parent = self.parent or self

            self.last_edition.last = False
            self.__last_edition = m.last_edition
            mm.delete()
            return True

        @staticmethod
        def get_fear(structure):
            return fear.get_cgr_string(structure)

        @staticmethod
        def get_fingerprints(structures):
            f = Fragmentor(workpath='.', version=FRAGMENTOR_VERSION, fragment_type=FRAGMENT_TYPE_MOL,
                           min_length=FRAGMENT_MIN_MOL, max_length=FRAGMENT_MAX_MOL,
                           useformalcharge=True).get(structures)['X']
            return fingerprints.get_fingerprints(f)

        @property
        def structure_raw(self):
            if self.__cached_structure_raw is None:
                g = node_link_graph(self.data)
                g.__class__ = MoleculeContainer
                self.__cached_structure_raw = g
            return self.__cached_structure_raw

        @property
        def structure_parent(self):
            if self.parent:
                return self.parent.structure_raw
            return None

        @property
        def structure(self):
            return self.last_edition.structure_raw

        @property
        def last_edition(self):
            if self.__last_edition is None:
                if self.last:
                    tmp = self
                elif self.parent and self.parent.last:
                    tmp = self.parent
                else:
                    tmp = (self.parent or self).children.filter(lambda x: x.last).first()
                self.__last_edition = tmp
            return self.__last_edition

        __cached_structure_raw = None
        __last_edition = None

        def flush_cache(self):
            self.__cached_structure_raw = None
            self.__last_edition = None
            FingerprintMixin.flush_cache(self)

    class Reaction(db.Entity, UserMixin, FingerprintMixin, IsomorphismMixin, Similarity,
                   ReactionStructureSearch, ReactionSubStructureSearch):
        _table_ = '%s_reaction' % schema if DEBUG else (schema, 'reaction')
        id = PrimaryKey(int, auto=True)
        date = Required(datetime)
        user_id = Required(int, column='user')
        fear = Required(str, unique=True)
        mapless_fear = Required(str)
        fingerprint = Required(str) if DEBUG else Required(str, sql_type='bit(%s)' % (2 ** FP_SIZE))

        children = Set('Reaction', cascade_delete=True)
        parent = Optional('Reaction')

        molecules = Set('MoleculeReaction')
        conditions = Set('Conditions')
        special = Optional(Json)

        def __init__(self, structure, user, conditions=None, special=None, fingerprint=None, fear_string=None,
                     mapless_fear_string=None, cgr=None, substrats_fears=None, products_fears=None):
            new_mols, batch = OrderedDict(), {}
            fears = dict(substrats=iter(substrats_fears if substrats_fears and
                                        len(substrats_fears) == len(structure.substrats) else []),
                         products=iter(products_fears if products_fears and
                                       len(products_fears) == len(structure.products) else []))

            refreshed = ReactionContainer()
            m_count = count()
            for i, is_p in (('substrats', False), ('products', True)):
                for x in structure[i]:
                    m_fear_string = next(fears[i], Molecule.get_fear(x))
                    m = Molecule.get(fear=m_fear_string)
                    if m:
                        mapping = self.match_structures(m.structure_raw, x)
                        batch[next(m_count)] = (m.last_edition, is_p,
                                                [(k, v) for k, v in mapping.items() if k != v] or None)
                        refreshed[i].append(relabel_nodes(m.structure, mapping))
                    else:
                        new_mols[next(m_count)] = (x, is_p, m_fear_string)
                        refreshed[i].append(x)

            if new_mols:
                for_fp, for_x = [], []
                for x, _, m_fp in new_mols.values():
                    if m_fp not in for_fp:
                        for_fp.append(m_fp)
                        for_x.append(x)

                fp_dict = dict(zip(for_fp, Molecule.get_fingerprints(for_x)))
                dups = {}
                for n, (x, is_p, m_fear_string) in new_mols.items():
                    if m_fear_string not in dups:
                        m = Molecule(x, user, fear_string=m_fear_string, fingerprint=fp_dict[m_fear_string])
                        dups[m_fear_string] = m
                        mapping = None
                    else:
                        m = dups[m_fear_string]
                        mapping = [(k, v) for k, v in
                                   self.match_structures(m.structure_raw, x).items() if k != v] or None
                    batch[n] = (m, is_p, mapping)

            if mapless_fear_string is None:
                mapless_fear_string, merged = self.get_mapless_fear(refreshed, get_merged=True)
            else:
                merged = None

            if fear_string is None:
                fear_string, cgr = (self.get_fear(structure, get_cgr=True) if merged is None else
                                    self.get_fear(merged, is_merged=True, get_cgr=True))
            elif cgr is None:
                cgr = cgr_core.getCGR(refreshed) if merged is None else cgr_core.getCGR(merged, is_merged=True)

            if fingerprint is None:
                fingerprint = self.get_fingerprints([cgr], is_cgr=True)[0]

            db.Entity.__init__(self, user_id=user.id, fear=fear_string, fingerprint=fingerprint.bin,
                               date=datetime.utcnow(), mapless_fear=mapless_fear_string)

            for m, is_p, mapping in (batch[x] for x in sorted(batch)):
                MoleculeReaction(reaction=self, molecule=m, product=is_p, mapping=mapping)

            if conditions:
                Conditions(conditions, self, user)

            if special:
                self.special = special

            self.__cached_cgr = cgr
            self.__cached_structure = structure
            self.__cached_bitstring = fingerprint

        @classmethod
        def refresh_reaction(cls, structure):
            fresh = dict(substrats=[], products=[])
            for i, is_p in (('substrats', False), ('products', True)):
                for x in structure[i]:
                    m = Molecule.get(fear=Molecule.get_fear(x))
                    if m:
                        fresh[i].append(m)
                    else:
                        return False

            res = ReactionContainer()
            for k in ('products', 'substrats'):
                for x, y in zip(fresh[k], structure[k]):
                    mapping = cls.match_structures(x.structure_raw, y)
                    res[k].append(relabel_nodes(x.structure, mapping))

            return res

        @staticmethod
        def get_fingerprints(structures, is_cgr=False):
            cgrs = structures if is_cgr else [cgr_core.getCGR(x) for x in structures]
            f = Fragmentor(workpath='.', version=FRAGMENTOR_VERSION, fragment_type=FRAGMENT_TYPE_CGR,
                           min_length=FRAGMENT_MIN_CGR, max_length=FRAGMENT_MAX_CGR,
                           cgr_dynbonds=FRAGMENT_DYNBOND_CGR, useformalcharge=True).get(cgrs)['X']
            return fingerprints.get_fingerprints(f)

        @staticmethod
        def get_fear(structure, is_merged=False, get_cgr=False):
            cgr = cgr_core.getCGR(structure, is_merged=is_merged)
            fear_string = Molecule.get_fear(cgr)
            return (fear_string, cgr) if get_cgr else fear_string

        @staticmethod
        def get_mapless_fear(structure, is_merged=False, get_merged=False):
            merged = structure if is_merged else cgr_core.merge_mols(structure)
            fear_string = '%s>>%s' % (Molecule.get_fear(merged['substrats']), Molecule.get_fear(merged['products']))
            return (fear_string, merged) if get_merged else fear_string

        @property
        def cgr(self):
            if self.__cached_cgr is None:
                self.__cached_cgr = cgr_core.getCGR(self.structure)
            return self.__cached_cgr

        @property
        def structure(self):
            if self.__cached_structure is None:
                r = ReactionContainer()
                for m in self.molecules.order_by(lambda x: x.id):
                    r['products' if m.product else 'substrats'].append(
                        relabel_nodes(m.molecule.structure_raw, dict(m.mapping)) if m.mapping else m.molecule.structure)
                self.__cached_structure = r
            return self.__cached_structure

        def refresh_fear_fingerprint(self):
            fear_string, cgr = self.get_fear(self.structure, get_cgr=True)
            fingerprint = self.get_fingerprints([cgr], is_cgr=True)[0]
            print(self.date)  # Pony BUG. AD-HOC!
            self.fear = fear_string
            self.fingerprint = fingerprint.bin
            self.__cached_bitstring = fingerprint

        __cached_structure = None
        __cached_cgr = None
        __cached_conditions = None

        def flush_cache(self):
            self.__cached_structure = None
            self.__cached_cgr = None
            self.__cached_conditions = None
            FingerprintMixin.flush_cache(self)

    class MoleculeReaction(db.Entity):
        _table_ = '%s_molecule_reaction' % schema if DEBUG else (schema, 'molecule_reaction')
        id = PrimaryKey(int, auto=True)
        reaction = Required('Reaction')
        molecule = Required('Molecule')
        product = Required(bool, default=False)
        mapping = Optional(Json)

    class MoleculeMerge(db.Entity):
        _table_ = '%s_molecule_merge' % schema if DEBUG else (schema, 'molecule_merge')
        id = PrimaryKey(int, auto=True)
        source = Required('Molecule', reverse='merge_target')
        target = Required('Molecule', reverse='merge_source')
        mapping = Optional(Json)

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

    Molecule.load_tree()
    Reaction.load_tree()
    return Molecule, Reaction, Conditions
